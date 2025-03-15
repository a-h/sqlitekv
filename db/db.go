package db

import (
	"context"
	"errors"
	"time"
)

// Record is the record stored in the store prior to being unmarshaled.
type Record struct {
	Key     string    `json:"key"`
	Version int64     `json:"version"`
	Value   []byte    `json:"value"`
	Created time.Time `json:"created"`
}

type DB interface {
	// Query runs queries against the store. The query should return rows, and the rows are returned as-is.
	Query(ctx context.Context, queries ...Query) (output [][]Record, err error)
	// Mutate runs mutations against the store.
	Mutate(ctx context.Context, mutations ...Mutation) (rowsAffected []int64, err error)
	QueryScalarInt64(ctx context.Context, query string, args map[string]any) (n int64, err error)
}

type Query struct {
	SQL  string
	Args map[string]any
}

type Mutation struct {
	SQL  string
	Args map[string]any
	// If the value can't be marshalled, the ArgsError is set.
	ArgsError      error
	MustAffectRows bool
}

var ErrVersionMismatch = errors.New("version mismatch")
