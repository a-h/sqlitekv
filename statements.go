package sqlitekv

import "encoding/json"

// Init operation.

const initCreateTableSQL = `create table if not exists kv (id integer primary key, key text unique, version integer, value jsonb);`

const initCreateIndexSQL = `create index if not exists kv_key on kv(key);`

// Get operation.

const getSQL = `select id, key, version, value from kv where key = :key;`

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

const getPrefixSQL = `select id, key, version, value from kv where key like :prefix;`

func newGetPrefixSQLParamsSqlite(prefix string) map[string]any {
	return map[string]any{
		":prefix": prefix + "%",
	}
}

func newGetPrefixSQLParamsRqlite(prefix string) map[string]any {
	return map[string]any{
		"prefix": prefix + "%",
	}
}

// List operation.

const listSQL = `select id, key, version, value from kv order by key limit :limit offset :start;`

func newListSQLParamsSqlite(start, limit int) map[string]any {
	return map[string]any{
		":start": start,
		":limit": limit,
	}
}

func newListSQLParamsRqlite(start, limit int) map[string]any {
	return map[string]any{
		"start": start,
		"limit": limit,
	}
}

// Put operation.

const putSQL = `insert into kv (key, version, value) values (:key, 1, :value) on conflict(key) do update set version = excluded.version + 1, value = excluded.value where (:version = -1 or version = :version);`

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

const deleteSQL = `delete from kv where key = :key;`

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

const deletePrefixSQL = `delete from kv where key like :prefix;`

func newDeletePrefixSQLParamsSqlite(prefix string) map[string]any {
	return map[string]any{
		":prefix": prefix + "%",
	}
}

func newDeletePrefixSQLParamsRqlite(prefix string) map[string]any {
	return map[string]any{
		"prefix": prefix + "%",
	}
}

// Count operation.

const countSQL = `select count(*) from kv;`

// Patch operation.

const patchSQL = `insert into kv (key, version, value) values (:key, 1, :value) on conflict(key) do update set version = excluded.version + 1, value = json_patch(kv.value, excluded.value) where (:version = -1 or version = :version);`

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
