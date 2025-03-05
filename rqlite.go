package sqlitekv

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	rqlitehttp "github.com/rqlite/rqlite-go-http"
)

func checkResultColumns(result rqlitehttp.QueryResult) (err error) {
	if len(result.Columns) != 4 {
		return fmt.Errorf("record: expected 4 columns, got %d", len(result.Columns))
	}
	if result.Columns[0] != "id" || result.Columns[1] != "key" || result.Columns[2] != "version" || result.Columns[3] != "value" {
		return fmt.Errorf("record: expected id, key, version and value columns not found, got: %#v", result.Columns)
	}
	return nil
}

func newRecordFromValues[T any](values []any) (r Record[T], err error) {
	if len(values) != 4 {
		return r, fmt.Errorf("record: expected 4 columns, got %d", len(values))
	}
	if r.ID, err = tryGetInt64(values[0]); err != nil {
		return r, fmt.Errorf("record: id: %w", err)
	}
	var ok bool
	r.Key, ok = values[1].(string)
	if !ok {
		return r, fmt.Errorf("record: key: expected string, got %T", values[1])
	}
	if r.Version, err = tryGetInt64(values[2]); err != nil {
		return r, fmt.Errorf("record: version: %w", err)
	}
	resultString, ok := values[3].(string)
	if !ok {
		return r, fmt.Errorf("record: value: expected string, got %T", values[3])
	}
	err = json.Unmarshal([]byte(resultString), &r.Value)
	if err != nil {
		return r, fmt.Errorf("record: value: %w: %q", err, resultString)
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

func NewRqlite[T any](client *rqlitehttp.Client) *Rqlite[T] {
	return &Rqlite[T]{
		client:          client,
		timeout:         time.Second * 30,
		readConsistency: rqlitehttp.ReadConsistencyLevelWeak,
	}
}

type Rqlite[T any] struct {
	client          *rqlitehttp.Client
	timeout         time.Duration
	readConsistency rqlitehttp.ReadConsistencyLevel
}

func (r *Rqlite[T]) isStore() Store[T] { return r }

// Init creates the tables if they don't exist.
func (r *Rqlite[T]) Init(ctx context.Context) error {
	statements := rqlitehttp.NewSQLStatementsFromStrings(
		[]string{initCreateTableSQL, initCreateIndexSQL},
	)
	opts := &rqlitehttp.ExecuteOptions{
		Transaction: true,
		Wait:        true,
		Timeout:     r.timeout,
	}
	qr, err := r.client.Execute(ctx, statements, opts)
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
	opts := &rqlitehttp.QueryOptions{
		Timeout: rq.timeout,
		Level:   rq.readConsistency,
	}
	qr, err := rq.client.Query(ctx, rqlitehttp.SQLStatements{q}, opts)
	if err != nil {
		return Record[T]{}, false, err
	}
	if len(qr.Results) != 1 {
		return Record[T]{}, false, fmt.Errorf("get: expected 1 result, got %d", len(qr.Results))
	}
	if qr.Results[0].Error != "" {
		return Record[T]{}, false, fmt.Errorf("get: %s", qr.Results[0].Error)
	}
	if err := checkResultColumns(qr.Results[0]); err != nil {
		return Record[T]{}, false, err
	}
	if len(qr.Results[0].Values) == 0 {
		return Record[T]{}, false, nil
	}
	r, err = newRecordFromValues[T](qr.Results[0].Values[0])
	if err != nil {
		return Record[T]{}, false, err
	}
	return r, true, err
}

func (rq *Rqlite[T]) GetPrefix(ctx context.Context, prefix string) (records []Record[T], err error) {
	q := rqlitehttp.SQLStatement{
		SQL:         getPrefixSQL,
		NamedParams: newGetPrefixSQLParamsRqlite(prefix),
	}
	opts := &rqlitehttp.QueryOptions{
		Timeout: rq.timeout,
		Level:   rq.readConsistency,
	}
	qr, err := rq.client.Query(ctx, rqlitehttp.SQLStatements{q}, opts)
	if err != nil {
		return nil, err
	}
	if len(qr.Results) != 1 {
		return nil, fmt.Errorf("getprefix: expected 1 result, got %d", len(qr.Results))
	}
	if qr.Results[0].Error != "" {
		return nil, fmt.Errorf("getprefix: %s", qr.Results[0].Error)
	}
	if err := checkResultColumns(qr.Results[0]); err != nil {
		return nil, err
	}
	for _, values := range qr.Results[0].Values {
		r, err := newRecordFromValues[T](values)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, nil
}

func (rq *Rqlite[T]) List(ctx context.Context, start, limit int) (records []Record[T], err error) {
	q := rqlitehttp.SQLStatement{
		SQL:         listSQL,
		NamedParams: newListSQLParamsRqlite(start, limit),
	}
	opts := &rqlitehttp.QueryOptions{
		Timeout: rq.timeout,
		Level:   rq.readConsistency,
	}
	qr, err := rq.client.Query(ctx, rqlitehttp.SQLStatements{q}, opts)
	if err != nil {
		return nil, err
	}
	if len(qr.Results) != 1 {
		return nil, fmt.Errorf("list: expected 1 result, got %d", len(qr.Results))
	}
	if qr.Results[0].Error != "" {
		return nil, fmt.Errorf("list: %s", qr.Results[0].Error)
	}
	if err := checkResultColumns(qr.Results[0]); err != nil {
		return nil, err
	}
	for _, values := range qr.Results[0].Values {
		r, err := newRecordFromValues[T](values)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
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
	opts := &rqlitehttp.ExecuteOptions{
		Transaction: true,
		Wait:        true,
		Timeout:     rq.timeout,
	}
	qr, err := rq.client.Execute(ctx, rqlitehttp.SQLStatements{q}, opts)
	if err != nil {
		return err
	}
	if len(qr.Results) != 1 {
		return fmt.Errorf("put: expected 1 result, got %d", len(qr.Results))
	}
	if qr.Results[0].Error != "" {
		return fmt.Errorf("put: %s", qr.Results[0].Error)
	}
	if qr.Results[0].RowsAffected == 0 {
		return newErrVersionMismatch(key, version)
	}
	return nil
}

func (rq *Rqlite[T]) Delete(ctx context.Context, key string) (err error) {
	q := rqlitehttp.SQLStatement{
		SQL:         deleteSQL,
		NamedParams: newDeleteSQLParamsRqlite(key),
	}
	opts := &rqlitehttp.ExecuteOptions{
		Transaction: true,
		Wait:        true,
		Timeout:     rq.timeout,
	}
	qr, err := rq.client.Execute(ctx, rqlitehttp.SQLStatements{q}, opts)
	if err != nil {
		return err
	}
	if len(qr.Results) != 1 {
		return fmt.Errorf("delete: expected 1 result, got %d", len(qr.Results))
	}
	if qr.Results[0].Error != "" {
		return fmt.Errorf("delete: %s", qr.Results[0].Error)
	}
	return nil
}

func (rq *Rqlite[T]) DeletePrefix(ctx context.Context, prefix string) (err error) {
	q := rqlitehttp.SQLStatement{
		SQL:         deletePrefixSQL,
		NamedParams: newDeletePrefixSQLParamsRqlite(prefix),
	}
	opts := &rqlitehttp.ExecuteOptions{
		Transaction: true,
		Wait:        true,
		Timeout:     rq.timeout,
	}
	qr, err := rq.client.Execute(ctx, rqlitehttp.SQLStatements{q}, opts)
	if err != nil {
		return err
	}
	if len(qr.Results) != 1 {
		return fmt.Errorf("deleteprefix: expected 1 result, got %d", len(qr.Results))
	}
	if qr.Results[0].Error != "" {
		return fmt.Errorf("deleteprefix: %s", qr.Results[0].Error)
	}
	return nil
}

func (rq *Rqlite[T]) Count(ctx context.Context) (count int64, err error) {
	q := rqlitehttp.SQLStatement{
		SQL: countSQL,
	}
	opts := &rqlitehttp.QueryOptions{
		Timeout: rq.timeout,
		Level:   rq.readConsistency,
	}
	qr, err := rq.client.Query(ctx, rqlitehttp.SQLStatements{q}, opts)
	if err != nil {
		return 0, err
	}
	if len(qr.Results) != 1 {
		return 0, fmt.Errorf("count: expected 1 result, got %d", len(qr.Results))
	}
	if qr.Results[0].Error != "" {
		return 0, fmt.Errorf("count: %s", qr.Results[0].Error)
	}
	if len(qr.Results[0].Values) != 1 {
		return 0, fmt.Errorf("count: expected 1 row, got %d", len(qr.Results[0].Values))
	}
	if len(qr.Results[0].Values[0]) != 1 {
		return 0, fmt.Errorf("count: expected 1 column, got %d", len(qr.Results[0].Values[0]))
	}
	countFloat, ok := qr.Results[0].Values[0][0].(float64)
	if !ok {
		return 0, fmt.Errorf("count: expected float64, got %T", qr.Results[0].Values[0][0])
	}
	return int64(countFloat), nil
}

func (rq *Rqlite[T]) Patch(ctx context.Context, key string, version int64, patch any) (err error) {
	params, err := newPatchSQLParamsRqlite(key, version, patch)
	if err != nil {
		return err
	}
	q := rqlitehttp.SQLStatement{
		SQL:         patchSQL,
		NamedParams: params,
	}
	opts := &rqlitehttp.ExecuteOptions{
		Transaction: true,
		Wait:        true,
		Timeout:     rq.timeout,
	}
	qr, err := rq.client.Execute(ctx, rqlitehttp.SQLStatements{q}, opts)
	if err != nil {
		return err
	}
	if len(qr.Results) != 1 {
		return fmt.Errorf("patch: expected 1 result, got %d", len(qr.Results))
	}
	if qr.Results[0].Error != "" {
		return fmt.Errorf("patch: %s", qr.Results[0].Error)
	}
	if qr.Results[0].RowsAffected == 0 {
		return newErrVersionMismatch(key, version)
	}
	return nil
}
