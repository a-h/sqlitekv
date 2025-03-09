package sqlitekv

import "encoding/json"

// Init operation.

const initCreateTableSQL = `create table if not exists kv (key text primary key, version integer, value jsonb) without rowid;`

const initCreateIndexSQL = `create index if not exists kv_key on kv(key);`

// Get operation.

const getSQL = `select key, version, json(value) as value from kv where key = :key;`

func newGetSQLParamsSqlite(key string) map[string]any {
	return map[string]any{
		":key": key,
	}
}

func newGetSQLParamsRqlite(key string) map[string]any {
	return map[string]any{
		"key": key,
	}
}

// GetPrefix operation.

const getPrefixSQL = `select key, version, json(value) as value from kv where key like :prefix order by key limit :limit offset :offset;`

func newGetPrefixSQLParamsSqlite(prefix string, offset, limit int) map[string]any {
	return map[string]any{
		":prefix": prefix + "%",
		":limit":  limit,
		":offset": offset,
	}
}

func newGetPrefixSQLParamsRqlite(prefix string, offset, limit int) map[string]any {
	return map[string]any{
		"prefix": prefix + "%",
		"limit":  limit,
		"offset": offset,
	}
}

// GetRange operation.

const getRangeSQL = `select key, version, json(value) as value from kv where key >= :from and key < :to order by key limit :limit offset :offset;`

func newGetRangeSQLParamsSqlite(from, to string, offset, limit int) map[string]any {
	return map[string]any{
		":from":   from,
		":to":     to,
		":limit":  limit,
		":offset": offset,
	}
}

func newGetRangeSQLParamsRqlite(from, to string, offset, limit int) map[string]any {
	return map[string]any{
		"from":   from,
		"to":     to,
		"limit":  limit,
		"offset": offset,
	}
}

// List operation.

const listSQL = `select key, version, json(value) as value from kv order by key limit :limit offset :offset;`

func newListSQLParamsSqlite(offset, limit int) map[string]any {
	return map[string]any{
		":offset": offset,
		":limit":  limit,
	}
}

func newListSQLParamsRqlite(offset, limit int) map[string]any {
	return map[string]any{
		"offset": offset,
		"limit":  limit,
	}
}

// Put operation.

const putSQL = `insert into kv (key, version, value) values (:key, 1, jsonb(:value)) on conflict(key) do update set version = excluded.version + 1, value = jsonb(excluded.value) where (:version = -1 or version = :version) and (version <> 0);`

func newPutSQLParamsSqlite(key string, version int64, value any) (params map[string]any, err error) {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return map[string]any{}, err
	}
	params = map[string]any{
		":key":     key,
		":version": version,
		":value":   string(jsonValue),
	}
	return params, nil
}

func newPutSQLParamsRqlite(key string, version int64, value any) (params map[string]any, err error) {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return map[string]any{}, err
	}
	params = map[string]any{
		"key":     key,
		"version": version,
		"value":   string(jsonValue),
	}
	return params, nil
}

// Delete operation.

const deleteSQL = `delete from kv where key = :key`

func newDeleteSQLParamsSqlite(key string) map[string]any {
	return map[string]any{
		":key": key,
	}
}

func newDeleteSQLParamsRqlite(key string) map[string]any {
	return map[string]any{
		"key": key,
	}
}

// DeletePrefix operation.

// SQLite supports the `limit` and `offset` clauses in `delete` statements, but
// it's a compiler option (SQLITE_ENABLE_UPDATE_DELETE_LIMIT) that is disabled
// by default (although it is enabled in Ubuntu and MacOS builds of sqlite).
//
// CTEs are not supported with a join, so the simplest way to delete a prefix
// is to use a subquery.

const deletePrefixSQL = `delete from kv where key in (select key from kv where key like :prefix order by key limit :limit offset :offset);`

func newDeletePrefixSQLParamsSqlite(prefix string, offset, limit int) map[string]any {
	return map[string]any{
		":prefix": prefix + "%",
		":limit":  limit,
		":offset": offset,
	}
}

func newDeletePrefixSQLParamsRqlite(prefix string, offset, limit int) map[string]any {
	return map[string]any{
		"prefix": prefix + "%",
		"limit":  limit,
		"offset": offset,
	}
}

// DeleteRange operation.

// SQLite supports the `limit` and `offset` clauses in `delete` statements, but
// it's a compiler option (SQLITE_ENABLE_UPDATE_DELETE_LIMIT) that is disabled
// by default (although it is enabled in Ubuntu and MacOS builds of sqlite).
//
// CTEs are not supported with a join, so the simplest way to delete a range
// is to use a subquery.

const deleteRangeSQL = `delete from kv where key in (select key from kv where key >= :from and key < :to order by key limit :limit offset :offset);`

func newDeleteRangeSQLParamsSqlite(from, to string, offset, limit int) map[string]any {
	return map[string]any{
		":from":   from,
		":to":     to,
		":limit":  limit,
		":offset": offset,
	}
}

func newDeleteRangeSQLParamsRqlite(from, to string, offset, limit int) map[string]any {
	return map[string]any{
		"from":   from,
		"to":     to,
		"limit":  limit,
		"offset": offset,
	}
}

// Count operation.

const countSQL = `select count(*) from kv;`

// CountPrefix operation.

const countPrefixSQL = `select count(*) from kv where key like :prefix;`

func newCountPrefixSQLParamsSqlite(prefix string) map[string]any {
	return map[string]any{
		":prefix": prefix + "%",
	}
}

func newCountPrefixSQLParamsRqlite(prefix string) map[string]any {
	return map[string]any{
		"prefix": prefix + "%",
	}
}

// CountRange operation.

const countRangeSQL = `select count(*) from kv where key >= :from and key < :to;`

func newCountRangeSQLParamsSqlite(from, to string) map[string]any {
	return map[string]any{
		":from": from,
		":to":   to,
	}
}

func newCountRangeSQLParamsRqlite(from, to string) map[string]any {
	return map[string]any{
		"from": from,
		"to":   to,
	}
}

// Patch operation.

const patchSQL = `insert into kv (key, version, value) values (:key, 1, jsonb(:value)) on conflict(key) do update set version = excluded.version + 1, value = jsonb_patch(kv.value, excluded.value) where (:version = -1 or version = :version);`

func newPatchSQLParamsSqlite(key string, version int64, patch any) (params map[string]any, err error) {
	jsonPatch, err := json.Marshal(patch)
	if err != nil {
		return map[string]any{}, err
	}
	params = map[string]any{
		":key":     key,
		":version": version,
		":value":   string(jsonPatch),
	}
	return params, nil
}

func newPatchSQLParamsRqlite(key string, version int64, patch any) (params map[string]any, err error) {
	jsonPatch, err := json.Marshal(patch)
	if err != nil {
		return map[string]any{}, err
	}
	params = map[string]any{
		"key":     key,
		"version": version,
		"value":   string(jsonPatch),
	}
	return params, nil
}
