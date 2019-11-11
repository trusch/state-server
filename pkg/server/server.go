package server

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"golang.org/x/crypto/sha3"

	"github.com/Masterminds/squirrel"
	sq "github.com/Masterminds/squirrel"
	"github.com/golang/protobuf/ptypes"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/trusch/state-server/pkg/api"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

// New creates a new state server handler instance
func New(db *sql.DB) (api.StateServer, error) {
	s := &stateServer{db: db}
	if err := s.initTables(); err != nil {
		return nil, err
	}
	return s, nil
}

type stateServer struct {
	db *sql.DB
}

func (s *stateServer) initTables() error {
	_, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS values(
		scope TEXT,
		key TEXT,
		revision INT NOT NULL DEFAULT 0,
		value BYTEA,
		created_at TIMESTAMP,
		PRIMARY KEY(scope,key,revision))`)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(`CREATE TABLE IF NOT EXISTS childs(
		scope TEXT,
		parent TEXT,
		child TEXT,
		PRIMARY KEY(scope, parent, child))`)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(`CREATE TABLE IF NOT EXISTS hashes(
		scope TEXT,
		key TEXT,
		hash BYTEA,
		revision INT NOT NULL DEFAULT 0,
		created_at TIMESTAMP,
		PRIMARY KEY(scope,key,revision))`)
	if err != nil {
		return err
	}

	return nil
}

func (s *stateServer) Set(ctx context.Context, req *api.SetRequest) (_ *api.Value, err error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			logrus.Error(err)
			rerr := tx.Rollback()
			if rerr != nil {
				logrus.Error(rerr)
			}
		}
		err = tx.Commit()
	}()
	return s.set(ctx, tx, req)
}

func (s *stateServer) Get(ctx context.Context, req *api.GetRequest) (*api.Value, error) {
	return s.get(ctx, s.db, req)
}

func (s *stateServer) Delete(ctx context.Context, req *api.GetRequest) (*api.Value, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			logrus.Error(err)
			rerr := tx.Rollback()
			if rerr != nil {
				logrus.Error(rerr)
			}
		}
		err = tx.Commit()
	}()
	return s.set(ctx, tx, &api.SetRequest{
		Scope: req.Scope,
		Key:   req.Key,
	})
}

func (s *stateServer) Rollback(ctx context.Context, req *api.RollbackRequest) (*api.RollbackResult, error) {
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).RunWith(s.db)
	ts, err := ptypes.Timestamp(req.Time)
	if err != nil {
		return nil, err
	}
	filter := sq.And{
		sq.Eq{"scope": req.Scope},
		sq.Gt{"created_at": ts},
	}
	if req.Key != "" {
		filter = append(filter, sq.Eq{"key": req.Key})
	}
	res, err := psql.Delete("values").Where(filter).ExecContext(ctx)
	if err != nil {
		return nil, err
	}
	affectedValues, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	res, err = psql.Delete("hashes").Where(filter).ExecContext(ctx)
	if err != nil {
		return nil, err
	}
	affectedHashes, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	return &api.RollbackResult{
		AffectedRows: uint64(affectedValues + affectedHashes),
	}, nil
}

func (s *stateServer) List(req *api.ListRequest, srv api.State_ListServer) error {
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).RunWith(s.db)
	filter := sq.And{sq.Eq{"values.scope": req.Scope}}
	if req.Key != "" {
		filter = sq.And{filter, sq.Eq{"values.key": req.Key}}
	}
	rows, err := psql.Select("distinct on(values.scope, values.key) values.key", "values.value", "values.revision", "values.created_at", "hashes.hash").
		From("values").
		LeftJoin("hashes ON values.key=hashes.key AND values.revision=hashes.revision").
		Where(filter).
		OrderBy("values.scope, values.key, values.revision DESC").
		QueryContext(srv.Context())

	if err != nil {
		return err
	}
	for rows.Next() {
		var ts time.Time
		res := &api.Value{
			Scope: req.Scope,
		}
		err := rows.Scan(&res.Key, &res.Value, &res.Revision, &ts, &res.Hash)
		if err != nil {
			return err
		}
		res.CreatedAt, err = ptypes.TimestampProto(ts)
		if err != nil {
			return err
		}
		err = srv.Send(res)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *stateServer) Hash(ctx context.Context, req *api.HashRequest) (*api.HashResponse, error) {
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).RunWith(s.db)
	var row squirrel.RowScanner
	if req.Revision > 0 {
		row = psql.Select("hash").From("hashes").
			Where(squirrel.Eq{
				"scope":    req.Scope,
				"key":      req.Key,
				"revision": req.Revision,
			}).
			QueryRowContext(ctx)
	} else {
		row = psql.Select("hash").From("hashes").
			Where(squirrel.Eq{
				"scope": req.Scope,
				"key":   req.Key,
			}).
			OrderBy("revision DESC").
			Limit(1).
			QueryRowContext(ctx)
	}
	var hash []byte
	err := row.Scan(&hash)
	if err != nil {
		return nil, err
	}
	return &api.HashResponse{Hash: hash}, nil
}

func (s *stateServer) set(ctx context.Context, tx *sql.Tx, req *api.SetRequest) (_ *api.Value, err error) {
	old, err := s.get(ctx, tx, &api.GetRequest{Scope: req.Scope, Key: req.Key})
	if err != nil {
		old = &api.Value{}
	}
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).RunWith(tx)
	now := time.Now()
	_, err = psql.Insert("values").
		Columns("scope", "key", "value", "created_at", "revision").
		Values(req.Scope, req.Key, req.Value, now, old.Revision+1).
		ExecContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to insert value")
	}
	path := strings.Split(req.Key, "/")
	if len(path) > 0 {
		err = s.updateHashes(ctx, tx, req.Scope, path, req.Key, req.Value, now)
		if err != nil {
			return nil, errors.Wrap(err, "failed to update merkle tree")
		}
	}
	return s.get(ctx, tx, &api.GetRequest{
		Scope: req.Scope,
		Key:   req.Key,
	})
}

func (s *stateServer) get(ctx context.Context, runner squirrel.BaseRunner, req *api.GetRequest) (*api.Value, error) {
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).RunWith(runner)
	filter := sq.And{
		sq.Eq{"values.scope": req.Scope},
		sq.Eq{"values.key": req.Key},
	}
	if req.Revision > 0 {
		filter = append(filter, sq.Eq{"values.revision": req.Revision})
	}
	row := psql.Select("values.value", "values.revision", "values.created_at", "hashes.hash").
		From("values").
		LeftJoin("hashes ON values.key=hashes.key AND values.revision=hashes.revision").
		Where(filter).
		OrderBy("values.revision DESC").
		Limit(1).
		QueryRowContext(ctx)
	var ts time.Time
	res := &api.Value{
		Scope: req.Scope,
		Key:   req.Key,
	}
	err := row.Scan(&res.Value, &res.Revision, &ts, &res.Hash)
	if err != nil {
		return nil, err
	}
	res.CreatedAt, err = ptypes.TimestampProto(ts)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *stateServer) updateHash(ctx context.Context, tx *sql.Tx, scope string, path []string, value []byte, now time.Time) error {
	logrus.Debugf("update hash of %v  to %x", path, value[:8])
	lastRevision := 0
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).RunWith(tx)
	row := psql.Select("revision").From("hashes").
		Where(squirrel.Eq{
			"scope": scope,
			"key":   strings.Join(path, "/"),
		}).
		OrderBy("revision DESC").
		Limit(1).
		QueryRowContext(ctx)
	if err := row.Scan(&lastRevision); err != nil {
		logrus.Warn(err)
		lastRevision = 0
	}
	_, err := psql.Insert("hashes").Columns("scope", "key", "hash", "created_at", "revision").
		Values(scope, strings.Join(path, "/"), value, now, lastRevision+1).
		ExecContext(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to update single hash of %v", path)
	}

	return nil
}

func (s *stateServer) updateHashes(ctx context.Context, tx *sql.Tx, scope string, path []string, key string, value []byte, now time.Time) error {
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).RunWith(tx)
	var hash []byte
	hash = make([]byte, 32)
	leafHasher := sha3.NewShake256()
	_, _ = leafHasher.Write(value)
	_, _ = leafHasher.Read(hash[:])
	err := s.updateHash(ctx, tx, scope, path, hash, now)
	if err != nil {
		return errors.Wrap(err, "failed to hash leaf hash")
	}
	lastPath := path
	for i := 0; i < len(path)-1; i++ {
		currentPath := path[:len(path)-i-1]
		_, err = psql.Insert("childs").Columns("scope", "parent", "child").
			Values(scope, strings.Join(currentPath, "/"), strings.Join(lastPath, "/")).
			Suffix("ON CONFLICT(scope,parent,child) DO NOTHING").
			ExecContext(ctx)
		if err != nil {
			logrus.Warn(err)
		}
		rows, err := psql.Select("DISTINCT ON (hashes.scope,hashes.key) hash").
			From("hashes").
			Join("childs ON key = child").
			Where(sq.Eq{
				"parent": strings.Join(currentPath, "/"),
			}).
			OrderBy("hashes.scope, hashes.key, hashes.revision DESC").
			QueryContext(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to select child hashes")
		}
		hasher := sha3.NewShake256()
		for rows.Next() {
			var childHash []byte
			if err = rows.Scan(&childHash); err != nil {
				return errors.Wrap(err, "failed to scan child hash")
			}
			if len(childHash) > 0 {
				_, _ = hasher.Write(childHash)
			}
		}
		_, _ = hasher.Read(hash[:])
		err = s.updateHash(ctx, tx, scope, currentPath, hash, now)
		if err != nil {
			return err
		}
		lastPath = currentPath
	}
	return nil
}
