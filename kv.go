package sqlitekv

import (
	"context"
	"encoding/json"
	"fmt"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func newRecordFromStmt[T any](stmt *sqlite.Stmt) (r Record[T], err error) {
	r.ID = stmt.GetInt64("id")
	r.Key = stmt.GetText("key")
	r.Version = stmt.GetInt64("version")
	err = json.NewDecoder(stmt.GetReader("value")).Decode(&r.Value)
	if err != nil {
		return r, err
	}
	return r, nil
}

type Record[T any] struct {
	ID      int64  `json:"id"`
	Key     string `json:"key"`
	Version int64  `json:"version"`
	Value   T      `json:"value"`
}

func NewStore[T any](pool *sqlitex.Pool) *Store[T] {
	return &Store[T]{
		pool: pool,
	}
}

type Store[T any] struct {
	pool *sqlitex.Pool
}

// Init creates the tables if they don't exist.
func (s *Store[T]) Init(ctx context.Context) error {
	conn, err := s.pool.Take(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	sql := `create table if not exists kv (id integer primary key, key text unique, version integer, value blob);

create index if not exists kv_key on kv(key);`
	return sqlitex.ExecScript(conn, sql)
}

func (s *Store[T]) Get(ctx context.Context, key string) (r Record[T], ok bool, err error) {
	conn, err := s.pool.Take(ctx)
	if err != nil {
		return Record[T]{}, false, err
	}
	defer s.pool.Put(conn)

	var count int
	sql := `select id, key, version, value from kv where key = :key;`
	opts := &sqlitex.ExecOptions{
		Named: map[string]any{
			":key": key,
		},
		ResultFunc: func(stmt *sqlite.Stmt) (err error) {
			count++
			r, err = newRecordFromStmt[T](stmt)
			return err
		},
	}
	if err := sqlitex.Execute(conn, sql, opts); err != nil {
		return Record[T]{}, false, err
	}
	if count > 1 {
		return Record[T]{}, false, fmt.Errorf("multiple records found for key %q", key)
	}

	return r, count == 1, nil
}

func (s *Store[T]) GetPrefix(ctx context.Context, prefix string) (records []Record[T], err error) {
	conn, err := s.pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer s.pool.Put(conn)

	sql := `select id, key, version, value from kv where key LIKE :prefix;`
	opts := &sqlitex.ExecOptions{
		Named: map[string]any{
			":prefix": prefix + "%",
		},
		ResultFunc: func(stmt *sqlite.Stmt) (err error) {
			r, err := newRecordFromStmt[T](stmt)
			if err != nil {
				return err
			}
			records = append(records, r)
			return nil
		},
	}
	if err := sqlitex.Execute(conn, sql, opts); err != nil {
		return nil, err
	}
	return records, nil
}

func newErrVersionMismatch(key string, expectedVersion int64) ErrVersionMismatch {
	return ErrVersionMismatch{
		Key:             key,
		ExpectedVersion: expectedVersion,
	}
}

type ErrVersionMismatch struct {
	Key             string
	ExpectedVersion int64
}

func (e ErrVersionMismatch) Error() string {
	return fmt.Sprintf("version mismatch for key %q: expected %d, but wasn't found", e.Key, e.ExpectedVersion)
}

// Put puts a key into the store. If the key already exists, it will update the value if the version matches, and increment the version.
//
// If the key does not exist, it will insert the key with version 1.
//
// If the key exists but the version does not match, it will return an error.
//
// If the version is -1, it will skip the version check.
func (s *Store[T]) Put(ctx context.Context, key string, version int64, value T) (err error) {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return err
	}

	conn, err := s.pool.Take(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	sql := `insert into kv (key, version, value) values (:key, 1, :value) 
		on conflict(key) do update set version = excluded.version + 1, value = excluded.value where (version = :version or excluded.version = -1);`
	opts := &sqlitex.ExecOptions{
		Named: map[string]any{
			":key":     key,
			":version": version,
			":value":   jsonValue,
		},
	}
	if err = sqlitex.Execute(conn, sql, opts); err != nil {
		return err
	}

	if conn.Changes() == 0 {
		return newErrVersionMismatch(key, version)
	}

	return nil
}

func (s *Store[T]) Delete(ctx context.Context, key string) error {
	conn, err := s.pool.Take(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	sql := `delete from kv where key = :key;`
	opts := &sqlitex.ExecOptions{
		Named: map[string]any{
			":key": key,
		},
	}
	return sqlitex.Execute(conn, sql, opts)
}
