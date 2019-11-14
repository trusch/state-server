package main

import (
	"context"
	"database/sql"
	"os"
	"os/signal"

	"github.com/cenkalti/backoff"
	grpcserver "github.com/contiamo/goserver/grpc"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"

	"github.com/trusch/state-server/pkg/api"
	"github.com/trusch/state-server/pkg/server"
)

var (
	dbStr      = pflag.String("db", "postgres://postgres@localhost:5432?sslmode=disable", "postgres connect string")
	listenAddr = pflag.String("listen", ":3001", "listening address")
)

func main() {
	pflag.Parse()
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func(){
		<-c
		cancel()
	}()
	
	var (
		db  *sql.DB
		err error
	)
	err = backoff.Retry(func() error {
		select {
		case <-ctx.Done():
			return backoff.Permanent(ctx.Err())
		default:
			db, err = sql.Open("postgres", *dbStr)
			if err != nil {
				return err
			}
			err = db.Ping()
			if err != nil {
				return err
			}
			return nil
		}
	}, backoff.NewExponentialBackOff())
	if err != nil {
		logrus.Fatal(err)
	}

	// setup grpc server with options
	grpcServer, err := grpcserver.New(&grpcserver.Config{
		Options: []grpcserver.Option{
			grpcserver.WithCredentials("", "", ""),
			grpcserver.WithLogging("state-server"),
			grpcserver.WithMetrics(),
			grpcserver.WithRecovery(),
			grpcserver.WithReflection(),
		},
		Extras: []grpc.ServerOption{
			grpc.MaxSendMsgSize(1 << 12),
		},
		Register: func(srv *grpc.Server) {
			stateServer, err := server.New(db)
			if err != nil {
				panic(err)
			}
			api.RegisterStateServer(srv, stateServer)
		},
	})
	if err != nil {
		logrus.Fatal(err)
	}

	// start server
	if err := grpcserver.ListenAndServe(ctx, *listenAddr, grpcServer); err != nil {
		logrus.Fatal(err)
	}
}
