package sqlitekv

import (
	"context"
	"fmt"
	"strings"
	"time"

	rqlitehttp "github.com/rqlite/rqlite-go-http"
)

func NewRqlite(client *rqlitehttp.Client) *Rqlite {
	return &Rqlite{
		Client:          client,
		Timeout:         time.Second * 10,
		ReadConsistency: rqlitehttp.ReadConsistencyLevelStrong,
	}
}

type Rqlite struct {
	Client          *rqlitehttp.Client
	Timeout         time.Duration
	ReadConsistency rqlitehttp.ReadConsistencyLevel
}

func (r *Rqlite) isDB() DB { return r }

func (rq *Rqlite) Query(ctx context.Context, queries ...QueryInput) (outputs [][]Record, err error) {
	statements := make(rqlitehttp.SQLStatements, len(queries))
	for i, query := range queries {
		args, err := query.Args()
		if err != nil {
			return nil, fmt.Errorf("query: %w", err)
		}
		statements[i] = rqlitehttp.SQLStatement{
			SQL:         query.SQL,
			NamedParams: convertToRqlite(args),
		}
	}
	opts := &rqlitehttp.QueryOptions{
		Timeout: rq.Timeout,
		Level:   rq.ReadConsistency,
	}
	qr, err := rq.Client.Query(ctx, statements, opts)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	outputs = make([][]Record, len(qr.Results))
	for i, result := range qr.Results {
		if result.Error != "" {
			return nil, fmt.Errorf("query: index %d: %s", i, result.Error)
		}
		if err := checkResultColumns(result); err != nil {
			return nil, fmt.Errorf("query: %w", err)
		}
		outputs[i] = make([]Record, len(result.Values))
		for j, values := range result.Values {
			r, err := newRowFromValues(values)
			if err != nil {
				return nil, fmt.Errorf("query: index %d: row %d: %w", i, j, err)
			}
			outputs[i][j] = r
		}
	}
	return outputs, nil
}

func checkResultColumns(result rqlitehttp.QueryResult) (err error) {
	if len(result.Columns) != 3 {
		return fmt.Errorf("record: expected 3 columns, got %d", len(result.Columns))
	}
	if result.Columns[0] != "key" || result.Columns[1] != "version" || result.Columns[2] != "value" {
		return fmt.Errorf("record: expected id, key, version and value columns not found, got: %#v", result.Columns)
	}
	return nil
}

func newRowFromValues(values []any) (r Record, err error) {
	if len(values) != 3 {
		return r, fmt.Errorf("row: expected 3 columns, got %d", len(values))
	}
	var ok bool
	r.Key, ok = values[0].(string)
	if !ok {
		return r, fmt.Errorf("row: key: expected string, got %T", values[1])
	}
	if r.Version, err = tryGetInt64(values[1]); err != nil {
		return r, fmt.Errorf("row: version: %w", err)
	}
	r.Value = []byte(values[2].(string))
	return r, nil
}

func tryGetInt64(v any) (int64, error) {
	floatValue, ok := v.(float64)
	if !ok {
		return 0, fmt.Errorf("expected float64, got %T", v)
	}
	return int64(floatValue), nil
}

func (rq *Rqlite) Mutate(ctx context.Context, mutations ...MutationInput) (output []MutationOutput, err error) {
	statements := make(rqlitehttp.SQLStatements, len(mutations))
	for i, mutation := range mutations {
		args, err := mutation.Args()
		if err != nil {
			return nil, fmt.Errorf("mutate: index %d: %w", i, err)
		}
		statements[i] = rqlitehttp.SQLStatement{
			SQL:         mutation.SQL,
			NamedParams: convertToRqlite(args),
		}
	}
	opts := &rqlitehttp.ExecuteOptions{
		Transaction: true,
		Wait:        true,
		Timeout:     rq.Timeout,
	}
	qr, err := rq.Client.Execute(ctx, statements, opts)
	if err != nil {
		return nil, fmt.Errorf("mutate: %w", err)
	}
	output = make([]MutationOutput, len(qr.Results))
	for i, result := range qr.Results {
		if result.Error != "" {
			return nil, fmt.Errorf("mutate: index %d: %s", i, result.Error)
		}
		output[i].RowsAffected = result.RowsAffected
	}
	return output, nil
}

func (rq *Rqlite) QueryScalarInt64(ctx context.Context, sql string, params map[string]any) (int64, error) {
	opts := &rqlitehttp.QueryOptions{
		Timeout: rq.Timeout,
		Level:   rq.ReadConsistency,
	}
	q := rqlitehttp.SQLStatement{
		SQL:         sql,
		NamedParams: convertToRqlite(params),
	}
	qr, err := rq.Client.Query(ctx, rqlitehttp.SQLStatements{q}, opts)
	if err != nil {
		return 0, err
	}
	if len(qr.Results) != 1 {
		return 0, fmt.Errorf("expected 1 result, got %d", len(qr.Results))
	}
	if qr.Results[0].Error != "" {
		return 0, fmt.Errorf("%s", qr.Results[0].Error)
	}
	if len(qr.Results[0].Values) != 1 {
		return 0, fmt.Errorf("expected 1 row, got %d", len(qr.Results[0].Values))
	}
	if len(qr.Results[0].Values[0]) != 1 {
		return 0, fmt.Errorf("expected 1 column, got %d", len(qr.Results[0].Values[0]))
	}
	vt, ok := qr.Results[0].Values[0][0].(float64)
	if !ok {
		return 0, fmt.Errorf("expected float64, got %T", qr.Results[0].Values[0][0])
	}
	return int64(vt), nil
}

func convertToRqlite(args map[string]any) (updated map[string]any) {
	updated = make(map[string]any, len(args))
	for k, v := range args {
		updated[strings.TrimPrefix(k, ":")] = v
	}
	return updated
}
