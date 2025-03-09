package sqlitekv

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	rqlitehttp "github.com/rqlite/rqlite-go-http"
)

func NewRqlite[T any](client *rqlitehttp.Client) *Rqlite[T] {
	return &Rqlite[T]{
		Client:          client,
		Timeout:         time.Second * 10,
		ReadConsistency: rqlitehttp.ReadConsistencyLevelStrong,
	}
}

type Rqlite[T any] struct {
	Client          *rqlitehttp.Client
	Timeout         time.Duration
	ReadConsistency rqlitehttp.ReadConsistencyLevel
}

func (r *Rqlite[T]) isStore() Store[T] { return r }

func (r *Rqlite[T]) Init(ctx context.Context) error {
	statements := rqlitehttp.NewSQLStatementsFromStrings(
		[]string{initCreateTableSQL, initCreateIndexSQL},
	)
	opts := &rqlitehttp.ExecuteOptions{
		Transaction: true,
		Wait:        true,
		Timeout:     r.Timeout,
	}
	qr, err := r.Client.Execute(ctx, statements, opts)
	if err != nil {
		return fmt.Errorf("init: %w", err)
	}
	if len(qr.Results) != 2 {
		return fmt.Errorf("init: expected 2 results, got %d", len(qr.Results))
	}
	for _, result := range qr.Results {
		if result.Error != "" {
			return fmt.Errorf("init: %s", result.Error)
		}
	}
	return err
}

func (rq *Rqlite[T]) Get(ctx context.Context, key string) (r Record[T], ok bool, err error) {
	q := rqlitehttp.SQLStatement{
		SQL:         getSQL,
		NamedParams: newGetSQLParamsRqlite(key),
	}
	records, err := rq.queryMany(ctx, q)
	if err != nil {
		return Record[T]{}, false, fmt.Errorf("get: %w", err)
	}
	if len(records) == 0 {
		return Record[T]{}, false, nil
	}
	if len(records) > 1 {
		return Record[T]{}, false, fmt.Errorf("get: expected 1 record, got %d", len(records))
	}
	return records[0], true, nil
}

func (rq *Rqlite[T]) GetPrefix(ctx context.Context, prefix string, offset, limit int) (records Records[T], err error) {
	q := rqlitehttp.SQLStatement{
		SQL:         getPrefixSQL,
		NamedParams: newGetPrefixSQLParamsRqlite(prefix, offset, limit),
	}
	records, err = rq.queryMany(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("getprefix: %w", err)
	}
	return records, nil
}

func (rq *Rqlite[T]) GetRange(ctx context.Context, from, to string, start, limit int) (records Records[T], err error) {
	q := rqlitehttp.SQLStatement{
		SQL:         getRangeSQL,
		NamedParams: newGetRangeSQLParamsRqlite(from, to, start, limit),
	}
	records, err = rq.queryMany(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("getrange: %w", err)
	}
	return records, nil
}

func (rq *Rqlite[T]) List(ctx context.Context, offset, limit int) (records Records[T], err error) {
	q := rqlitehttp.SQLStatement{
		SQL:         listSQL,
		NamedParams: newListSQLParamsRqlite(offset, limit),
	}
	records, err = rq.queryMany(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("list: %w", err)
	}
	return records, nil
}

func (rq *Rqlite[T]) Put(ctx context.Context, key string, version int64, value T) (err error) {
	params, err := newPutSQLParamsRqlite(key, version, value)
	if err != nil {
		return err
	}
	q := rqlitehttp.SQLStatement{
		SQL:         putSQL,
		NamedParams: params,
	}
	rowsAffected, err := rq.executeSingle(ctx, q)
	if err != nil {
		return fmt.Errorf("put: %w", err)
	}
	if rowsAffected == 0 {
		return newErrVersionMismatch(key, version)
	}
	return nil
}

func (rq *Rqlite[T]) Delete(ctx context.Context, key string) (err error) {
	q := rqlitehttp.SQLStatement{
		SQL:         deleteSQL,
		NamedParams: newDeleteSQLParamsRqlite(key),
	}
	_, err = rq.executeSingle(ctx, q)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	return nil
}

func (rq *Rqlite[T]) DeletePrefix(ctx context.Context, prefix string, offset, limit int) (rowsAffected int64, err error) {
	if prefix == "" {
		return 0, fmt.Errorf("deleteprefix: prefix cannot be empty, use '*' to delete all records")
	}
	if prefix == "*" {
		prefix = ""
	}
	q := rqlitehttp.SQLStatement{
		SQL:         deletePrefixSQL,
		NamedParams: newDeletePrefixSQLParamsRqlite(prefix, offset, limit),
	}
	rowsAffected, err = rq.executeSingle(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("deleteprefix: %w", err)
	}
	return rowsAffected, nil
}

func (rq *Rqlite[T]) DeleteRange(ctx context.Context, from, to string, offset, limit int) (rowsAffected int64, err error) {
	q := rqlitehttp.SQLStatement{
		SQL:         deleteRangeSQL,
		NamedParams: newDeleteRangeSQLParamsRqlite(from, to, offset, limit),
	}
	rowsAffected, err = rq.executeSingle(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("deleterange: %w", err)
	}
	return rowsAffected, nil
}

func (rq *Rqlite[T]) Count(ctx context.Context) (count int64, err error) {
	q := rqlitehttp.SQLStatement{
		SQL: countSQL,
	}
	count, err = rq.queryScalarInt64(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("count: %w", err)
	}
	return count, nil
}

func (rq *Rqlite[T]) CountPrefix(ctx context.Context, prefix string) (count int64, err error) {
	q := rqlitehttp.SQLStatement{
		SQL:         countPrefixSQL,
		NamedParams: newCountPrefixSQLParamsRqlite(prefix),
	}
	count, err = rq.queryScalarInt64(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("countprefix: %w", err)
	}
	return count, nil
}

func (rq *Rqlite[T]) CountRange(ctx context.Context, from, to string) (count int64, err error) {
	q := rqlitehttp.SQLStatement{
		SQL:         countRangeSQL,
		NamedParams: newCountRangeSQLParamsRqlite(from, to),
	}
	count, err = rq.queryScalarInt64(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("countrange: %w", err)
	}
	return count, nil
}

func (rq *Rqlite[T]) Patch(ctx context.Context, key string, version int64, patch any) (err error) {
	params, err := newPatchSQLParamsRqlite(key, version, patch)
	if err != nil {
		return fmt.Errorf("patch: %w", err)
	}
	q := rqlitehttp.SQLStatement{
		SQL:         patchSQL,
		NamedParams: params,
	}
	rowsAffected, err := rq.executeSingle(ctx, q)
	if err != nil {
		return fmt.Errorf("patch: %w", err)
	}
	if rowsAffected == 0 {
		return newErrVersionMismatch(key, version)
	}
	return nil
}

func (rq *Rqlite[T]) queryMany(ctx context.Context, q rqlitehttp.SQLStatement) (records Records[T], err error) {
	opts := &rqlitehttp.QueryOptions{
		Timeout: rq.Timeout,
		Level:   rq.ReadConsistency,
	}
	qr, err := rq.Client.Query(ctx, rqlitehttp.SQLStatements{q}, opts)
	if err != nil {
		return nil, err
	}
	if len(qr.Results) != 1 {
		return nil, fmt.Errorf("expected 1 result, got %d", len(qr.Results))
	}
	if qr.Results[0].Error != "" {
		return nil, fmt.Errorf("%s", qr.Results[0].Error)
	}
	if err := checkResultColumns(qr.Results[0]); err != nil {
		return nil, err
	}
	records = make(Records[T], len(qr.Results[0].Values))
	for i, values := range qr.Results[0].Values {
		r, err := newRecordFromValues[T](values)
		if err != nil {
			return nil, err
		}
		records[i] = r
	}
	return records, nil
}

func checkResultColumns(result rqlitehttp.QueryResult) (err error) {
	if len(result.Columns) != 3 {
		return fmt.Errorf("record: expected 4 columns, got %d", len(result.Columns))
	}
	if result.Columns[0] != "key" || result.Columns[1] != "version" || result.Columns[2] != "value" {
		return fmt.Errorf("record: expected id, key, version and value columns not found, got: %#v", result.Columns)
	}
	return nil
}

func newRecordFromValues[T any](values []any) (r Record[T], err error) {
	if len(values) != 3 {
		return r, fmt.Errorf("record: expected 4 columns, got %d", len(values))
	}
	var ok bool
	r.Key, ok = values[0].(string)
	if !ok {
		return r, fmt.Errorf("record: key: expected string, got %T", values[1])
	}
	if r.Version, err = tryGetInt64(values[1]); err != nil {
		return r, fmt.Errorf("record: version: %w", err)
	}
	valueString, ok := values[2].(string)
	if !ok {
		return r, fmt.Errorf("record: value: expected string, got %T", values[3])
	}
	err = json.Unmarshal([]byte(valueString), &r.Value)
	if err != nil {
		return r, fmt.Errorf("record: value: %w: %q", err, valueString)
	}
	return r, nil
}

func tryGetInt64(v any) (int64, error) {
	floatValue, ok := v.(float64)
	if !ok {
		return 0, fmt.Errorf("expected float64, got %T", v)
	}
	return int64(floatValue), nil
}

func (rq *Rqlite[T]) queryScalarInt64(ctx context.Context, q rqlitehttp.SQLStatement) (int64, error) {
	opts := &rqlitehttp.QueryOptions{
		Timeout: rq.Timeout,
		Level:   rq.ReadConsistency,
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

func (rq *Rqlite[T]) executeSingle(ctx context.Context, q rqlitehttp.SQLStatement) (rowsAffected int64, err error) {
	opts := &rqlitehttp.ExecuteOptions{
		Transaction: true,
		Wait:        true,
		Timeout:     rq.Timeout,
	}
	qr, err := rq.Client.Execute(ctx, rqlitehttp.SQLStatements{q}, opts)
	if err != nil {
		return 0, err
	}
	if len(qr.Results) != 1 {
		return 0, fmt.Errorf("expected 1 result, got %d", len(qr.Results))
	}
	if qr.Results[0].Error != "" {
		return 0, fmt.Errorf("%s", qr.Results[0].Error)
	}
	return qr.Results[0].RowsAffected, nil
}
