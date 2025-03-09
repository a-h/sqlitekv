package sqlitekv

import (
	"context"
	"fmt"
	"iter"
)

// Record can be used to store any type of value in the store.
type Record[T any] struct {
	Key     string `json:"key"`
	Version int64  `json:"version"`
	Value   T      `json:"value"`
}

type Records[T any] []Record[T]

func (r Records[T]) Values() iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, r := range r {
			if !yield(r.Value) {
				return
			}
		}
	}
}

type Store[T any] interface {
	// Init initializes the store. It should be called before any other method, and creates the necessary table.
	Init(ctx context.Context) error
	// Get gets a key from the store. If the key does not exist, it returns ok=false.
	Get(ctx context.Context, key string) (r Record[T], ok bool, err error)
	// GetPrefix gets all keys with a given prefix from the store.
	GetPrefix(ctx context.Context, prefix string, offset, limit int) (records Records[T], err error)
	// GetRange gets all keys between the key from (inclusive) and to (exclusive).
	// e.g. select key from kv where key >= 'a' and key < 'c';
	GetRange(ctx context.Context, from, to string, offset, limit int) (records Records[T], err error)
	// List gets all keys from the store, starting from the given offset and limiting the number of results to the given limit.
	List(ctx context.Context, offset, limit int) (records Records[T], err error)
	// Put a key into the store. If the key already exists, it will update the value if the version matches, and increment the version.
	//
	// If the key does not exist, it will insert the key with version 1.
	//
	// If the key exists but the version does not match, it will return an error.
	//
	// If the version is -1, it will skip the version check.
	//
	// If the version is 0, it will only insert the key if it does not already exist.
	Put(ctx context.Context, key string, version int64, value T) (err error)
	// Delete deletes a key from the store. If the key does not exist, no error is returned.
	Delete(ctx context.Context, key string) error
	// DeletePrefix deletes all keys with a given prefix from the store.
	DeletePrefix(ctx context.Context, prefix string, offset, limit int) (rowsAffected int64, err error)
	// DeleteRange deletes all keys between the key from (inclusive) and to (exclusive).
	DeleteRange(ctx context.Context, from, to string, offset, limit int) (rowsAffected int64, err error)
	// Count returns the number of keys in the store.
	Count(ctx context.Context) (count int64, err error)
	// CountPrefix returns the number of keys in the store with a given prefix.
	CountPrefix(ctx context.Context, prefix string) (count int64, err error)
	// CountRange returns the number of keys in the store between the key from (inclusive) and to (exclusive).
	CountRange(ctx context.Context, from, to string) (count int64, err error)
	// Patch patches a key in the store. The patch is a JSON merge patch (RFC 7396), so would look something like map[string]any{"key": "value"}.
	Patch(ctx context.Context, key string, version int64, patch any) (err error)
}

func newErrVersionMismatch(key string, expectedVersion int64) ErrVersionMismatch {
	return ErrVersionMismatch{
		Key:             key,
		ExpectedVersion: expectedVersion,
	}
}

// ErrVersionMismatch is returned when the version of a key does not match the expected version, typically the result of an optimistic lock failure.
type ErrVersionMismatch struct {
	Key             string
	ExpectedVersion int64
}

func (e ErrVersionMismatch) Error() string {
	return fmt.Sprintf("version mismatch for key %q: expected %d, but wasn't found", e.Key, e.ExpectedVersion)
}
