package sqlitekv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func NewSqlite(pool *sqlitex.Pool) *Sqlite {
	return &Sqlite{
		pool: pool,
	}
}

type Sqlite struct {
	pool *sqlitex.Pool
}

func (s *Sqlite) isDB() DB { return s }

func (s *Sqlite) Query(ctx context.Context, queries ...Query) (outputs [][]Record, err error) {
	conn, err := s.pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer s.pool.Put(conn)

	outputs = make([][]Record, len(queries))
	for i, q := range queries {
		opts := &sqlitex.ExecOptions{
			Named: q.Args,
			ResultFunc: func(stmt *sqlite.Stmt) (err error) {
				valueBytes, err := io.ReadAll(stmt.GetReader("value"))
				if err != nil {
					return fmt.Errorf("query: error reading value: %w", err)
				}
				created, err := time.Parse(sqliteTimeFormat, stmt.GetText("created"))
				if err != nil {
					return fmt.Errorf("query: error parsing created time: %w", err)
				}
				r := Record{
					Key:     stmt.GetText("key"),
					Version: stmt.GetInt64("version"),
					Value:   valueBytes,
					Created: created,
				}
				outputs[i] = append(outputs[i], r)
				return nil
			},
		}
		if err = sqlitex.Execute(conn, q.SQL, opts); err != nil {
			return outputs, fmt.Errorf("query: error in query index %d: %w", i, err)
		}
	}

	return outputs, nil
}

func (s *Sqlite) Mutate(ctx context.Context, mutations ...Mutation) (outputs []MutationOutput, err error) {
	conn, err := s.pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer s.pool.Put(conn)

	err = sqlitex.Execute(conn, "begin transaction;", nil)
	if err != nil {
		return nil, fmt.Errorf("mutate: error starting transaction: %w", err)
	}

	outputs = make([]MutationOutput, len(mutations))
	for i, m := range mutations {
		opts := &sqlitex.ExecOptions{
			Named: m.Args,
		}
		if err = sqlitex.Execute(conn, m.SQL, opts); err != nil {
			err = fmt.Errorf("mutate: error in mutation index %d: %w", i, err)
			rollbackErr := sqlitex.Execute(conn, "rollback;", nil)
			return outputs, errors.Join(err, rollbackErr)
		}
		outputs[i].RowsAffected = int64(conn.Changes())
	}

	err = sqlitex.Execute(conn, "commit;", nil)
	if err != nil {
		return nil, fmt.Errorf("mutate: error committing transaction: %w", err)
	}

	return outputs, nil
}

func (s *Sqlite) QueryScalarInt64(ctx context.Context, sql string, params map[string]any) (v int64, err error) {
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
