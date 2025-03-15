package sqlitekv

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/a-h/sqlitekv/db"
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

func (s *Sqlite) isDB() db.DB { return s }

func (s *Sqlite) Query(ctx context.Context, queries ...db.Query) (outputs [][]db.Record, err error) {
	conn, err := s.pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer s.pool.Put(conn)

	outputs = make([][]db.Record, len(queries))
	for i, q := range queries {
		opts := &sqlitex.ExecOptions{
			Named: q.Args,
			ResultFunc: func(stmt *sqlite.Stmt) (err error) {
				valueBytes, err := io.ReadAll(stmt.GetReader("value"))
				if err != nil {
					return fmt.Errorf("query: error reading value: %w", err)
				}
				created, err := time.Parse(time.RFC3339Nano, stmt.GetText("created"))
				if err != nil {
					return fmt.Errorf("query: error parsing created time: %w", err)
				}
				r := db.Record{
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

func (s *Sqlite) Mutate(ctx context.Context, mutations ...db.Mutation) (rowsAffected []int64, err error) {
	conn, err := s.pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer s.pool.Put(conn)

	rowsAffected = make([]int64, len(mutations))
	errs := make([]error, len(mutations))
	for i, m := range mutations {
		opts := &sqlitex.ExecOptions{
			Named: m.Args,
		}
		if err = sqlitex.Execute(conn, m.SQL, opts); err != nil {
			errs[i] = fmt.Errorf("mutate: error in mutation index %d: %w", i, err)
			continue
		}
		rowsAffected[i] = int64(conn.Changes())
		if mutations[i].MustAffectRows && rowsAffected[i] == 0 {
			errs[i] = db.ErrVersionMismatch
		}
	}

	return rowsAffected, newBatchError(errs)
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
