package sqlitekv

import (
	"context"
	"encoding/json"
	"fmt"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

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
	records, err := s.queryMany(ctx, getSQL, newGetSQLParamsSqlite(key))
	if err != nil {
		return Record[T]{}, false, fmt.Errorf("get: %w", err)
	}
	if len(records) == 0 {
		return Record[T]{}, false, nil
	}
	if len(records) > 1 {
		return Record[T]{}, false, fmt.Errorf("get: multiple records found for key %q", key)
	}
	return records[0], true, nil
}

func (s *Sqlite[T]) GetPrefix(ctx context.Context, prefix string, offset, limit int) (records Records[T], err error) {
	records, err = s.queryMany(ctx, getPrefixSQL, newGetPrefixSQLParamsSqlite(prefix, offset, limit))
	if err != nil {
		return nil, fmt.Errorf("getprefix: %w", err)
	}
	return records, nil
}

func (s *Sqlite[T]) GetRange(ctx context.Context, from, to string, offset, limit int) (records Records[T], err error) {
	records, err = s.queryMany(ctx, getRangeSQL, newGetRangeSQLParamsSqlite(from, to, offset, limit))
	if err != nil {
		return nil, fmt.Errorf("getrange: %w", err)
	}
	return records, nil
}

func (s *Sqlite[T]) List(ctx context.Context, start, limit int) (records Records[T], err error) {
	records, err = s.queryMany(ctx, listSQL, newListSQLParamsSqlite(start, limit))
	if err != nil {
		return nil, fmt.Errorf("list: %w", err)
	}
	return records, nil
}

func (s *Sqlite[T]) Put(ctx context.Context, key string, version int64, value T) (err error) {
	params, err := newPutSQLParamsSqlite(key, version, value)
	if err != nil {
		return fmt.Errorf("put: %w", err)
	}
	rowsAffected, err := s.executeSingle(ctx, putSQL, params)
	if err != nil {
		return fmt.Errorf("put: %w", err)
	}
	if rowsAffected == 0 {
		return newErrVersionMismatch(key, version)
	}
	return nil
}

func (s *Sqlite[T]) Delete(ctx context.Context, key string) error {
	if _, err := s.executeSingle(ctx, deleteSQL, newDeleteSQLParamsSqlite(key)); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	return nil
}

func (s *Sqlite[T]) DeletePrefix(ctx context.Context, prefix string, offset, limit int) (rowsAffected int64, err error) {
	if prefix == "" {
		return 0, fmt.Errorf("deleteprefix: prefix cannot be empty, use '*' to delete all records")
	}
	if prefix == "*" {
		prefix = ""
	}
	rowsAffected, err = s.executeSingle(ctx, deletePrefixSQL, newDeletePrefixSQLParamsSqlite(prefix, offset, limit))
	if err != nil {
		return 0, fmt.Errorf("deleteprefix: %w", err)
	}
	return rowsAffected, nil
}

func (s *Sqlite[T]) DeleteRange(ctx context.Context, from, to string, offset, limit int) (rowsAffected int64, err error) {
	rowsAffected, err = s.executeSingle(ctx, deleteRangeSQL, newDeleteRangeSQLParamsSqlite(from, to, offset, limit))
	if err != nil {
		return 0, fmt.Errorf("deleterange: %w", err)
	}
	return rowsAffected, nil
}

func (s *Sqlite[T]) Count(ctx context.Context) (count int64, err error) {
	count, err = s.queryScalarInt64(ctx, countSQL, nil)
	if err != nil {
		return 0, fmt.Errorf("count: %w", err)
	}
	return count, nil
}

func (s *Sqlite[T]) CountPrefix(ctx context.Context, prefix string) (count int64, err error) {
	count, err = s.queryScalarInt64(ctx, countPrefixSQL, newCountPrefixSQLParamsSqlite(prefix))
	if err != nil {
		return 0, fmt.Errorf("countprefix: %w", err)
	}
	return count, nil
}

func (s *Sqlite[T]) CountRange(ctx context.Context, from, to string) (count int64, err error) {
	count, err = s.queryScalarInt64(ctx, countRangeSQL, newCountRangeSQLParamsSqlite(from, to))
	if err != nil {
		return 0, fmt.Errorf("countrange: %w", err)
	}
	return count, nil
}

func (s *Sqlite[T]) Patch(ctx context.Context, key string, version int64, patch any) (err error) {
	params, err := newPatchSQLParamsSqlite(key, version, patch)
	if err != nil {
		return err
	}
	rowsAffected, err := s.executeSingle(ctx, patchSQL, params)
	if err != nil {
		return fmt.Errorf("patch: %w", err)
	}
	if rowsAffected == 0 {
		return newErrVersionMismatch(key, version)
	}
	return nil
}

func (s *Sqlite[T]) queryMany(ctx context.Context, sql string, params map[string]any) (records Records[T], err error) {
	conn, err := s.pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer s.pool.Put(conn)

	records = make(Records[T], 0)
	opts := &sqlitex.ExecOptions{
		Named: params,
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

func newRecordFromStmt[T any](stmt *sqlite.Stmt) (r Record[T], err error) {
	r.Key = stmt.GetText("key")
	r.Version = stmt.GetInt64("version")
	err = json.NewDecoder(stmt.GetReader("value")).Decode(&r.Value)
	if err != nil {
		return r, err
	}
	return r, nil
}

func (s *Sqlite[T]) queryScalarInt64(ctx context.Context, sql string, params map[string]any) (v int64, err error) {
	conn, err := s.pool.Take(ctx)
	if err != nil {
		return 0, err
	}
	defer s.pool.Put(conn)

	opts := &sqlitex.ExecOptions{
		Named: params,
		ResultFunc: func(stmt *sqlite.Stmt) (err error) {
			if stmt.ColumnType(0) != sqlite.TypeInteger {
				return fmt.Errorf("expected integer, got %s", stmt.ColumnType(0).String())
			}
			v = stmt.ColumnInt64(0)
			return nil
		},
	}
	if err := sqlitex.Execute(conn, sql, opts); err != nil {
		return 0, err
	}
	return v, nil
}

func (s *Sqlite[T]) executeSingle(ctx context.Context, sql string, params map[string]any) (rowsAffected int64, err error) {
	conn, err := s.pool.Take(ctx)
	if err != nil {
		return 0, err
	}
	defer s.pool.Put(conn)
	opts := &sqlitex.ExecOptions{
		Named: params,
	}
	if err = sqlitex.Execute(conn, sql, opts); err != nil {
		return 0, err
	}
	return int64(conn.Changes()), nil
}
