package sqlitekv

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/a-h/sqlitekv/statements"
)

// Record is the record stored in the store prior to being unmarshaled.
type Record struct {
	Key     string    `json:"key"`
	Version int64     `json:"version"`
	Value   []byte    `json:"value"`
	Created time.Time `json:"created"`
}

// ValuesOf returns the values of the records, unmarshaled into the given type.
func ValuesOf[T any](records []Record) (values []T, err error) {
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
func RecordsOf[T any](records []Record) (values []RecordOf[T], err error) {
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

type DB interface {
	// Query runs queries against the store. The query should return rows, and the rows are returned as-is.
	Query(ctx context.Context, queries ...statements.Query) (output [][]Record, err error)
	// Mutate runs mutations against the store.
	Mutate(ctx context.Context, mutations ...statements.Mutation) (output []statements.MutationOutput, err error)
	QueryScalarInt64(ctx context.Context, query string, args map[string]any) (n int64, err error)
}

func newErrVersionMismatch(key string, expectedVersion int64) ErrVersionMismatch {
	return ErrVersionMismatch{
		KeyToVersion: map[string]int64{
			key: expectedVersion,
		},
	}
}

func newErrVersionMismatchAll(keyToVersion map[string]int64) ErrVersionMismatch {
	return ErrVersionMismatch{
		KeyToVersion: keyToVersion,
	}
}

// ErrVersionMismatch is returned when the version of a key does not match the expected version, typically the result of an optimistic lock failure.
type ErrVersionMismatch struct {
	KeyToVersion map[string]int64
}

func (e ErrVersionMismatch) Error() string {
	keys := make([]string, len(e.KeyToVersion))
	keyToValueStrings := make([]string, len(e.KeyToVersion))
	slices.Sort(keys)
	for _, key := range keys {
		keyToValueStrings = append(keyToValueStrings, fmt.Sprintf("%q: %d", key, e.KeyToVersion[key]))
	}

	return fmt.Sprintf("key version mismatch: [ %s ]", strings.Join(keyToValueStrings, ", "))
}

func NewStore(db DB) Store {
	return Store{
		db: db,
	}
}

type Store struct {
	db DB
}

// Init initializes the store. It should be called before any other method, and creates the necessary table.
func (s *Store) Init(ctx context.Context) error {
	_, err := s.db.Mutate(ctx, statements.Init()...)
	return err
}

// Get gets a key from the store, and populates v with the value. If the key does not exist, it returns ok=false.
func (s *Store) Get(ctx context.Context, key string, v any) (r Record, ok bool, err error) {
	outputs, err := s.db.Query(ctx, statements.Get(key))
	if err != nil {
		return Record{}, false, fmt.Errorf("get: %w", err)
	}
	rows := outputs[0]
	if len(rows) == 0 {
		return Record{}, false, nil
	}
	if len(rows) > 1 {
		return Record{}, false, fmt.Errorf("get: multiple rows found for key %q", key)
	}
	r = rows[0]
	err = json.Unmarshal(r.Value, v)
	return r, true, err
}

// GetPrefix gets all keys with a given prefix from the store.
func (s *Store) GetPrefix(ctx context.Context, prefix string, offset, limit int) (rows []Record, err error) {
	outputs, err := s.db.Query(ctx, statements.GetPrefix(prefix, offset, limit))
	if err != nil {
		return nil, fmt.Errorf("getprefix: %w", err)
	}
	return outputs[0], nil
}

// GetRange gets all keys between the key from (inclusive) and to (exclusive).
// e.g. select key from kv where key >= 'a' and key < 'c';
func (s *Store) GetRange(ctx context.Context, from, to string, offset, limit int) (rows []Record, err error) {
	outputs, err := s.db.Query(ctx, statements.GetRange(from, to, offset, limit))
	if err != nil {
		return nil, fmt.Errorf("getrange: %w", err)
	}
	return outputs[0], nil
}

// List gets all keys from the store, starting from the given offset and limiting the number of results to the given limit.
func (s *Store) List(ctx context.Context, start, limit int) (rows []Record, err error) {
	outputs, err := s.db.Query(ctx, statements.List(start, limit))
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
	put, err := statements.Put(key, version, value)
	if err != nil {
		return fmt.Errorf("put: %w", err)
	}
	outputs, err := s.db.Mutate(ctx, put)
	if err != nil {
		return fmt.Errorf("put: %w", err)
	}
	if outputs[0].RowsAffected == 0 {
		return newErrVersionMismatch(key, version)
	}
	return nil
}

// Delete deletes a key from the store. If the key does not exist, no error is returned.
func (s *Store) Delete(ctx context.Context, key string) (rowsAffected int64, err error) {
	outputs, err := s.db.Mutate(ctx, statements.Delete(key))
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}
	return outputs[0].RowsAffected, nil
}

// DeletePrefix deletes all keys with a given prefix from the store.
func (s *Store) DeletePrefix(ctx context.Context, prefix string, offset, limit int) (rowsAffected int64, err error) {
	if prefix == "" {
		return 0, fmt.Errorf("deleteprefix: prefix cannot be empty, use '*' to delete all records")
	}
	if prefix == "*" {
		prefix = ""
	}
	outputs, err := s.db.Mutate(ctx, statements.DeletePrefix(prefix, offset, limit))
	if err != nil {
		return 0, fmt.Errorf("deleteprefix: %w", err)
	}
	return outputs[0].RowsAffected, nil
}

// DeleteRange deletes all keys between the key from (inclusive) and to (exclusive).
func (s *Store) DeleteRange(ctx context.Context, from, to string, offset, limit int) (rowsAffected int64, err error) {
	outputs, err := s.db.Mutate(ctx, statements.DeleteRange(from, to, offset, limit))
	if err != nil {
		return 0, fmt.Errorf("deleterange: %w", err)
	}
	return outputs[0].RowsAffected, nil
}

// Count returns the number of keys in the store.
func (s *Store) Count(ctx context.Context) (n int64, err error) {
	query := statements.Count()
	n, err = s.db.QueryScalarInt64(ctx, query.SQL, query.Args)
	if err != nil {
		return 0, fmt.Errorf("count: %w", err)
	}
	return n, nil
}

// CountPrefix returns the number of keys in the store with a given prefix.
func (s *Store) CountPrefix(ctx context.Context, prefix string) (count int64, err error) {
	query := statements.CountPrefix(prefix)
	count, err = s.db.QueryScalarInt64(ctx, query.SQL, query.Args)
	if err != nil {
		return 0, fmt.Errorf("countprefix: %w", err)
	}
	return count, nil
}

// CountRange returns the number of keys in the store between the key from (inclusive) and to (exclusive).
func (s *Store) CountRange(ctx context.Context, from, to string) (count int64, err error) {
	query := statements.CountRange(from, to)
	count, err = s.db.QueryScalarInt64(ctx, query.SQL, query.Args)
	if err != nil {
		return 0, fmt.Errorf("countrange: %w", err)
	}
	return count, nil
}

// Patch patches a key in the store. The patch is a JSON merge patch (RFC 7396), so would look something like map[string]any{"key": "value"}.
func (s *Store) Patch(ctx context.Context, key string, version int64, patch any) (err error) {
	patchMutation, err := statements.Patch(key, version, patch)
	if err != nil {
		return fmt.Errorf("patch: %w", err)
	}
	outputs, err := s.db.Mutate(ctx, patchMutation)
	if err != nil {
		return fmt.Errorf("patch: %w", err)
	}
	if outputs[0].RowsAffected == 0 {
		return newErrVersionMismatch(key, version)
	}
	return nil
}

// Query runs a select query against the store, and returns the results.
func (s *Store) Query(ctx context.Context, query string, args map[string]any) (output []Record, err error) {
	outputs, err := s.db.Query(ctx, statements.Query{SQL: query, Args: args})
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	return outputs[0], nil
}

// Mutate runs a mutation against the store, and returns the number of rows affected.
func (s *Store) Mutate(ctx context.Context, query string, args map[string]any) (rowsAffected int64, err error) {
	outputs, err := s.db.Mutate(ctx, statements.Mutation{SQL: query, Args: args})
	if err != nil {
		return 0, fmt.Errorf("mutate: %w", err)
	}
	return outputs[0].RowsAffected, nil
}

type MutateAllInput struct {
	// Used in Delete, Put, Patch.
	Key string `json:"key"`

	// Used in DeleteKeys.
	Keys []string `json:"keys"`

	// Used in Put / Patch.
	Version int64 `json:"version"`
	Value   any   `json:"value"`

	// Used in DeletePrefix.
	Prefix string `json:"prefix"`

	// Used in DeleteRange.
	RangeFrom string `json:"from"`
	RangeTo   string `json:"to"`

	// Used in DeletePrefix, DeleteRange.
	Offset int `json:"offset"`
	Limit  int `json:"limit"`

	Operation string `json:"operation"`
}

// MutateAll runs the mutations against the store. Put/patch operations are executed in a transaction, deletions are executed separately.
//
// Use the Put, Patch, Delete, DeleteKeys, DeletePrefix and DeleteRange functions to populate the operations argument.
func (s *Store) MutateAll(ctx context.Context, operations ...MutateAllInput) (rowsAffected int64, err error) {
	putPatchedKeys := map[string]struct{}{}
	var putPatches []statements.PutPatchInput
	var mutations []statements.Mutation
	for _, op := range operations {
		if op.Operation == "put" || op.Operation == "patch" {
			putPatches = append(putPatches, statements.PutPatchInput{
				Key:       op.Key,
				Version:   op.Version,
				Value:     op.Value,
				Operation: op.Operation,
			})
			if _, keyExists := putPatchedKeys[op.Key]; keyExists {
				return 0, fmt.Errorf("mutateall: cannot put/patch key %q multiple times in one operation", op.Key)
			}
			putPatchedKeys[op.Key] = struct{}{}
			continue
		}
		if op.Operation == "delete" {
			mutations = append(mutations, statements.Delete(op.Key))
			continue
		}
		if op.Operation == "delete_keys" {
			m, err := statements.DeleteKeys(op.Keys...)
			if err != nil {
				return 0, fmt.Errorf("mutateall: failed to create delete keys operation: %w", err)
			}
			mutations = append(mutations, m)
			continue
		}
		if op.Operation == "delete_prefix" {
			mutations = append(mutations, statements.DeletePrefix(op.Prefix, op.Offset, op.Limit))
			continue
		}
		if op.Operation == "delete_range" {
			mutations = append(mutations, statements.DeleteRange(op.RangeFrom, op.RangeTo, op.Offset, op.Limit))
			continue
		}
		return 0, fmt.Errorf("mutateall: unknown operation: %q", op.Operation)
	}

	putPatchIndex := -1
	if len(putPatches) > 0 {
		m, err := statements.PutPatches(putPatches...)
		if err != nil {
			return 0, fmt.Errorf("mutateall: %w", err)
		}
		mutations = append(mutations, m)
		putPatchIndex = len(mutations) - 1
	}

	outputs, err := s.db.Mutate(ctx, mutations...)
	if err != nil {
		return 0, fmt.Errorf("mutateall: %w", err)
	}
	for _, output := range outputs {
		rowsAffected += output.RowsAffected
	}

	if putPatchIndex > -1 {
		putPatchRowsAffected := outputs[putPatchIndex].RowsAffected
		if putPatchRowsAffected != int64(len(putPatches)) {
			keyToVersion := make(map[string]int64)
			for _, input := range putPatches {
				keyToVersion[input.Key] = input.Version
			}
			return rowsAffected, newErrVersionMismatchAll(keyToVersion)
		}
	}

	return rowsAffected, nil
}

func Put(key string, version int64, value any) MutateAllInput {
	return MutateAllInput{
		Key:       key,
		Version:   version,
		Value:     value,
		Operation: "put",
	}
}

func Patch(key string, version int64, value any) MutateAllInput {
	return MutateAllInput{
		Key:       key,
		Version:   version,
		Value:     value,
		Operation: "patch",
	}
}

func Delete(key string) MutateAllInput {
	return MutateAllInput{
		Key:       key,
		Operation: "delete",
	}
}

func DeleteKeys(keys ...string) MutateAllInput {
	return MutateAllInput{
		Keys:      keys,
		Operation: "delete_keys",
	}
}

func DeletePrefix(prefix string, offset, limit int) MutateAllInput {
	return MutateAllInput{
		Prefix:    prefix,
		Offset:    offset,
		Limit:     limit,
		Operation: "delete_prefix",
	}
}

func DeleteRange(from, to string, offset, limit int) MutateAllInput {
	return MutateAllInput{
		RangeFrom: from,
		RangeTo:   to,
		Offset:    offset,
		Limit:     limit,
		Operation: "delete_range",
	}
}
