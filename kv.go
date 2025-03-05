package sqlitekv

import (
	"context"
	"fmt"
)

// Record can be used to store any type of value in the store.
type Record[T any] struct {
	ID      int64  `json:"id"`
	Key     string `json:"key"`
	Version int64  `json:"version"`
	Value   T      `json:"value"`
}

type Store[T any] interface {
	// Init initializes the store. It should be called before any other method, and creates the necessary table.
	Init(ctx context.Context) error
	// Get gets a key from the store. If the key does not exist, it returns ok=false.
	Get(ctx context.Context, key string) (r Record[T], ok bool, err error)
	// GetPrefix gets all keys with a given prefix from the store.
	GetPrefix(ctx context.Context, prefix string) (records []Record[T], err error)
	// List gets all keys from the store, starting from the given offset and limiting the number of results to the given limit.
	List(ctx context.Context, start, limit int) (records []Record[T], err error)
	// Put puts a key into the store. If the key already exists, it will update the value if the version matches, and increment the version.
	//
	// If the key does not exist, it will insert the key with version 1.
	//
	// If the key exists but the version does not match, it will return an error.
	//
	// If the version is -1, it will skip the version check.
	Put(ctx context.Context, key string, version int64, value T) (err error)
	// Delete deletes a key from the store. If the key does not exist, no error is returned.
	Delete(ctx context.Context, key string) error
	// DeletePrefix deletes all keys with a given prefix from the store.
	DeletePrefix(ctx context.Context, prefix string) error
	// Count returns the number of keys in the store.
	Count(ctx context.Context) (count int64, err error)
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
