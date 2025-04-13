package sqlitekv

import (
	"context"
	"fmt"

	"github.com/a-h/sqlitekv/db"
	"github.com/a-h/sqlitekv/db/stmts"
	"github.com/jackc/pgx/v5"
)

func NewPostgres(conn *pgx.Conn) *KV {
	return &KV{
		conn: conn,
	}
}

type KV struct {
	conn *pgx.Conn
}

func (kv *KV) isDB() db.DB { return kv }

func (kv *KV) Query(ctx context.Context, queries ...db.Query) (outputs [][]db.Record, err error) {
	outputs = make([][]db.Record, len(queries))
	for i, q := range queries {
		rows, err := kv.conn.Query(ctx, q.SQL, pgx.NamedArgs(q.Args))
		if err != nil {
			return outputs, fmt.Errorf("query: error in query index %d: %w", i, err)
		}
		for rows.Next() {
			var r db.Record
			if err = rows.Scan(&r.Key, &r.Version, &r.Value, &r.Created); err != nil {
				return outputs, fmt.Errorf("query: error scanning row: %w", err)
			}
			outputs[i] = append(outputs[i], r)
		}
		rows.Close()
	}
	return outputs, nil
}

func (kv *KV) Mutate(ctx context.Context, mutations ...db.Mutation) (rowsAffected []int64, err error) {
	rowsAffected = make([]int64, len(mutations))
	errs := make([]error, len(mutations))
	for i, m := range mutations {
		if m.ArgsError != nil {
			return nil, fmt.Errorf("mutate: error in mutation: %w", m.ArgsError)
		}
		res, err := kv.conn.Exec(ctx, m.SQL, pgx.NamedArgs(m.Args))
		if err != nil {
			errs[i] = fmt.Errorf("mutate: error in mutation index %d: %w", i, err)
			continue
		}
		rowsAffected[i] = res.RowsAffected()
		if m.MustAffectRows && rowsAffected[i] == 0 {
			errs[i] = fmt.Errorf("mutate: error in mutation index %d: %w", i, db.ErrVersionMismatch)
		}
	}

	return rowsAffected, newBatchError(errs)
}

func (kv *KV) QueryScalarInt64(ctx context.Context, query string, args map[string]any) (n int64, err error) {
	row := kv.conn.QueryRow(ctx, query, pgx.NamedArgs(args))
	if err = row.Scan(&n); err != nil {
		return 0, fmt.Errorf("query: error scanning row: %w", err)
	}
	return n, nil
}

func (kv *KV) Statements() db.StatementSet {
	return stmts.Postgres{}
}
