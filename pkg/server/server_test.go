package server

import (
	"testing"
	_ "github.com/lib/pq"
	"database/sql"
	"github.com/stretchr/testify/require"
)

const (
	dbStr = "postgres://postgres@localhost:5432?sslmode=disable"
)

func TestServer(t *testing.T) {
	db, err := sql.Open("postgres", dbStr)
	require.NoError(t, err)
	srv,err := New(db)
	require.NoError(t, err)
	require.NotNil(t, srv)
}
