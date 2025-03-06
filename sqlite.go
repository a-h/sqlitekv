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

func NewSqlite[T any](pool *sqlitex.Pool) *Sqlite[T] {
	return &Sqlite[T]{
		pool: pool,
	}
}

type Sqlite[T any] struct {
	pool *sqlitex.Pool
}

func (s *Sqlite[T]) isStore() Store[T] { return s }

func (s *Sqlite[T]) Init(ctx context.Context) error {
	conn, err := s.pool.Take(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	return sqlitex.ExecScript(conn, initCreateTableSQL+"\n"+initCreateIndexSQL)
}

func (s *Sqlite[T]) Get(ctx context.Context, key string) (r Record[T], ok bool, err error) {
	conn, err := s.pool.Take(ctx)
	if err != nil {
		return Record[T]{}, false, err
	}
	defer s.pool.Put(conn)

	var count int
	opts := &sqlitex.ExecOptions{
		Named: newGetSQLParamsSqlite(key),
		ResultFunc: func(stmt *sqlite.Stmt) (err error) {
			count++
			r, err = newRecordFromStmt[T](stmt)
			return err
		},
	}
	if err := sqlitex.Execute(conn, getSQL, opts); err != nil {
		return Record[T]{}, false, err
	}
	if count > 1 {
		return Record[T]{}, false, fmt.Errorf("multiple records found for key %q", key)
	}

	return r, count == 1, nil
}

func (s *Sqlite[T]) GetPrefix(ctx context.Context, prefix string) (records []Record[T], err error) {
	conn, err := s.pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer s.pool.Put(conn)

	opts := &sqlitex.ExecOptions{
		Named: newGetPrefixSQLParamsSqlite(prefix),
		ResultFunc: func(stmt *sqlite.Stmt) (err error) {
			r, err := newRecordFromStmt[T](stmt)
			if err != nil {
				return err
			}
			records = append(records, r)
			return nil
		},
	}
	if err := sqlitex.Execute(conn, getPrefixSQL, opts); err != nil {
		return nil, err
	}
	return records, nil
}

func (s *Sqlite[T]) List(ctx context.Context, start, limit int) (records []Record[T], err error) {
	conn, err := s.pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer s.pool.Put(conn)

	opts := &sqlitex.ExecOptions{
		Named: newListSQLParamsSqlite(start, limit),
		ResultFunc: func(stmt *sqlite.Stmt) (err error) {
			r, err := newRecordFromStmt[T](stmt)
			if err != nil {
				return err
			}
			records = append(records, r)
			return nil
		},
	}
	if err := sqlitex.Execute(conn, listSQL, opts); err != nil {
		return nil, err
	}
	return records, nil
}

func (s *Sqlite[T]) Put(ctx context.Context, key string, version int64, value T) (err error) {
	params, err := newPutSQLParamsSqlite(key, version, value)
	if err != nil {
		return err
	}

	conn, err := s.pool.Take(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	opts := &sqlitex.ExecOptions{
		Named: params,
	}
	if err = sqlitex.Execute(conn, putSQL, opts); err != nil {
		return err
	}

	if conn.Changes() == 0 {
		return newErrVersionMismatch(key, version)
	}

	return nil
}

func (s *Sqlite[T]) Delete(ctx context.Context, key string) error {
	conn, err := s.pool.Take(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	opts := &sqlitex.ExecOptions{
		Named: newDeleteSQLParamsSqlite(key),
	}
	return sqlitex.Execute(conn, deleteSQL, opts)
}

func (s *Sqlite[T]) DeletePrefix(ctx context.Context, prefix string) error {
	conn, err := s.pool.Take(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	opts := &sqlitex.ExecOptions{
		Named: newDeletePrefixSQLParamsSqlite(prefix),
	}
	return sqlitex.Execute(conn, deletePrefixSQL, opts)
}

func (s *Sqlite[T]) Count(ctx context.Context) (count int64, err error) {
	conn, err := s.pool.Take(ctx)
	if err != nil {
		return 0, err
	}
	defer s.pool.Put(conn)

	opts := &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) (err error) {
			count = stmt.GetInt64("count(*)")
			return nil
		},
	}
	if err := sqlitex.Execute(conn, countSQL, opts); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Sqlite[T]) Patch(ctx context.Context, key string, version int64, patch any) (err error) {
	params, err := newPatchSQLParamsSqlite(key, version, patch)
	if err != nil {
		return err
	}

	conn, err := s.pool.Take(ctx)
	if err != nil {
		return err
	}
	defer s.pool.Put(conn)

	opts := &sqlitex.ExecOptions{
		Named: params,
	}
	if err = sqlitex.Execute(conn, patchSQL, opts); err != nil {
		return err
	}

	if conn.Changes() == 0 {
		return newErrVersionMismatch(key, version)
	}

	return nil
}
