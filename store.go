package sqlitekv

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/a-h/sqlitekv/db"
)

// ValuesOf returns the values of the records, unmarshaled into the given type.
func ValuesOf[T any](records []db.Record) (values []T, err error) {
	values = make([]T, len(records))
	for i, r := range records {
		err = json.Unmarshal(r.Value, &values[i])
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

type RecordOf[T any] struct {
	Key     string    `json:"key"`
	Version int64     `json:"version"`
	Value   T         `json:"value"`
	Created time.Time `json:"created"`
}

// RecordsOf returns the records, with the value unmarshaled into a type.
// Use map[string]any if you don't know the type.
func RecordsOf[T any](records []db.Record) (values []RecordOf[T], err error) {
	values = make([]RecordOf[T], len(records))
	for i, r := range records {
		err = json.Unmarshal(r.Value, &values[i].Value)
		if err != nil {
			return nil, err
		}
		values[i].Key = r.Key
		values[i].Version = r.Version
		values[i].Created = r.Created
	}
	return values, nil
}

func newBatchError(errs []error) error {
	var hasErrors bool
	for _, err := range errs {
		if err != nil {
			hasErrors = true
			break
		}
	}
	if !hasErrors {
		return nil
	}
	return &BatchError{
		Errors: errs,
	}
}

type BatchError struct {
	Errors []error
}

func (be *BatchError) Error() string {
	var sb strings.Builder
	for i, err := range be.Errors {
		if err != nil {
			sb.WriteString(fmt.Sprintf("%d: %v\n", i, err))
		}
	}
	return sb.String()
}

func NewStore(db db.DB) *Store {
	return &Store{
		db: db,
	}
}

type Store struct {
	db db.DB
}

// Init initializes the store. It should be called before any other method, and creates the necessary table.
func (s *Store) Init(ctx context.Context) error {
	_, err := s.db.Mutate(ctx, db.Init()...)
	return err
}

// Get gets a key from the store, and populates v with the value. If the key does not exist, it returns ok=false.
func (s *Store) Get(ctx context.Context, key string, v any) (r db.Record, ok bool, err error) {
	outputs, err := s.db.Query(ctx, db.Get(key))
	if err != nil {
		return db.Record{}, false, fmt.Errorf("get: %w", err)
	}
	rows := outputs[0]
	if len(rows) == 0 {
		return db.Record{}, false, nil
	}
	if len(rows) > 1 {
		return db.Record{}, false, fmt.Errorf("get: multiple rows found for key %q", key)
	}
	r = rows[0]
	err = json.Unmarshal(r.Value, v)
	return r, true, err
}

// GetPrefix gets all keys with a given prefix from the store.
func (s *Store) GetPrefix(ctx context.Context, prefix string, offset, limit int) (rows []db.Record, err error) {
	outputs, err := s.db.Query(ctx, db.GetPrefix(prefix, offset, limit))
	if err != nil {
		return nil, fmt.Errorf("getprefix: %w", err)
	}
	return outputs[0], nil
}

// GetRange gets all keys between the key from (inclusive) and to (exclusive).
// e.g. select key from kv where key >= 'a' and key < 'c';
func (s *Store) GetRange(ctx context.Context, from, to string, offset, limit int) (rows []db.Record, err error) {
	outputs, err := s.db.Query(ctx, db.GetRange(from, to, offset, limit))
	if err != nil {
		return nil, fmt.Errorf("getrange: %w", err)
	}
	return outputs[0], nil
}

// List gets all keys from the store, starting from the given offset and limiting the number of results to the given limit.
func (s *Store) List(ctx context.Context, start, limit int) (rows []db.Record, err error) {
	outputs, err := s.db.Query(ctx, db.List(start, limit))
	if err != nil {
		return nil, fmt.Errorf("list: %w", err)
	}
	return outputs[0], nil
}

// Put a key into the store. If the key already exists, it will update the value if the version matches, and increment the version.
//
// If the key does not exist, it will insert the key with version 1.
//
// If the key exists but the version does not match, it will return an error.
//
// If the version is -1, it will skip the version check.
//
// If the version is 0, it will only insert the key if it does not already exist.
func (s *Store) Put(ctx context.Context, key string, version int64, value any) (err error) {
	put := db.Put(key, version, value)
	if put.ArgsError != nil {
		return fmt.Errorf("put: %w", put.ArgsError)
	}
	if _, err = s.db.Mutate(ctx, put); err != nil {
		return fmt.Errorf("put: %w", err)
	}
	return nil
}

// Delete deletes a key from the store. If the key does not exist, no error is returned.
func (s *Store) Delete(ctx context.Context, key string) (rowsAffected int64, err error) {
	outputs, err := s.db.Mutate(ctx, db.Delete(key))
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}
	return outputs[0], nil
}

// DeletePrefix deletes all keys with a given prefix from the store.
func (s *Store) DeletePrefix(ctx context.Context, prefix string, offset, limit int) (rowsAffected int64, err error) {
	if prefix == "" {
		return 0, fmt.Errorf("deleteprefix: prefix cannot be empty, use '*' to delete all records")
	}
	if prefix == "*" {
		prefix = ""
	}
	outputs, err := s.db.Mutate(ctx, db.DeletePrefix(prefix, offset, limit))
	if err != nil {
		return 0, fmt.Errorf("deleteprefix: %w", err)
	}
	return outputs[0], nil
}

// DeleteRange deletes all keys between the key from (inclusive) and to (exclusive).
func (s *Store) DeleteRange(ctx context.Context, from, to string, offset, limit int) (rowsAffected int64, err error) {
	outputs, err := s.db.Mutate(ctx, db.DeleteRange(from, to, offset, limit))
	if err != nil {
		return 0, fmt.Errorf("deleterange: %w", err)
	}
	return outputs[0], nil
}

// Count returns the number of keys in the store.
func (s *Store) Count(ctx context.Context) (n int64, err error) {
	query := db.Count()
	n, err = s.db.QueryScalarInt64(ctx, query.SQL, query.Args)
	if err != nil {
		return 0, fmt.Errorf("count: %w", err)
	}
	return n, nil
}

// CountPrefix returns the number of keys in the store with a given prefix.
func (s *Store) CountPrefix(ctx context.Context, prefix string) (count int64, err error) {
	query := db.CountPrefix(prefix)
	count, err = s.db.QueryScalarInt64(ctx, query.SQL, query.Args)
	if err != nil {
		return 0, fmt.Errorf("countprefix: %w", err)
	}
	return count, nil
}

// CountRange returns the number of keys in the store between the key from (inclusive) and to (exclusive).
func (s *Store) CountRange(ctx context.Context, from, to string) (count int64, err error) {
	query := db.CountRange(from, to)
	count, err = s.db.QueryScalarInt64(ctx, query.SQL, query.Args)
	if err != nil {
		return 0, fmt.Errorf("countrange: %w", err)
	}
	return count, nil
}

// Patch patches a key in the store. The patch is a JSON merge patch (RFC 7396), so would look something like map[string]any{"key": "value"}.
func (s *Store) Patch(ctx context.Context, key string, version int64, patch any) (err error) {
	patchMutation := db.Patch(key, version, patch)
	if patchMutation.ArgsError != nil {
		return fmt.Errorf("patch: %w", patchMutation.ArgsError)
	}
	if _, err = s.db.Mutate(ctx, patchMutation); err != nil {
		return fmt.Errorf("patch: %w", err)
	}
	return nil
}

// Query runs a select query against the store, and returns the results.
func (s *Store) Query(ctx context.Context, query string, args map[string]any) (output []db.Record, err error) {
	outputs, err := s.db.Query(ctx, db.Query{SQL: query, Args: args})
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	return outputs[0], nil
}

// Mutate runs a mutation against the store, and returns the number of rows affected.
func (s *Store) Mutate(ctx context.Context, query string, args map[string]any) (rowsAffected int64, err error) {
	outputs, err := s.db.Mutate(ctx, db.Mutation{SQL: query, Args: args})
	if err != nil {
		return 0, fmt.Errorf("mutate: %w", err)
	}
	return outputs[0], nil
}

// MutateAll runs the mutations against the store, in the order they are provided.
//
// Use the Put, Patch, PutPatches, Delete, DeleteKeys, DeletePrefix and DeleteRange functions to populate the operations argument.
func (s *Store) MutateAll(ctx context.Context, mutations ...db.Mutation) (rowsAffected []int64, err error) {
	return s.db.Mutate(ctx, mutations...)
}
