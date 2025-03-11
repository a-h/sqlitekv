package sqlitekv

import (
	"encoding/json"
)

func Init() []Mutation {
	return []Mutation{
		{
			SQL: `create table if not exists kv (key text primary key, version integer, value jsonb, created text) without rowid;`,
		},
		{
			SQL: `create index if not exists kv_key on kv(key);`,
		},
		{
			SQL: `create index if not exists kv_created on kv(created);`,
		},
	}
}

func Get(key string) Query {
	return Query{
		SQL: `select key, version, json(value) as value, created from kv where key = :key;`,
		Args: map[string]any{
			":key": key,
		},
	}
}

func GetPrefix(prefix string, offset, limit int) Query {
	return Query{
		SQL: `select key, version, json(value) as value, created from kv where key like :prefix order by key limit :limit offset :offset;`,
		Args: map[string]any{
			":prefix": prefix + "%",
			":limit":  limit,
			":offset": offset,
		},
	}
}

func GetRange(from, to string, offset, limit int) Query {
	return Query{
		SQL: `select key, version, json(value) as value, created from kv where key >= :from and key < :to order by key limit :limit offset :offset;`,
		Args: map[string]any{
			":from":   from,
			":to":     to,
			":limit":  limit,
			":offset": offset,
		},
	}
}

func List(offset, limit int) Query {
	return Query{
		SQL: `select key, version, json(value) as value, created from kv order by key limit :limit offset :offset;`,
		Args: map[string]any{
			":offset": offset,
			":limit":  limit,
		},
	}
}

func Put(key string, version int64, value any) (m Mutation, err error) {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return Mutation{}, err
	}
	m = Mutation{
		SQL: `insert into kv (key, version, value, created)
values (:key, 1, jsonb(:value), :now)
on conflict(key) do update 
set version = excluded.version + 1, 
    value = jsonb(excluded.value)
where (:version = -1 or version = :version) and (:version <> 0);`,
		Args: map[string]any{
			":key":     key,
			":version": version,
			":value":   string(jsonValue),
			":now":     now(),
		},
	}
	return m, nil
}

type PutInput struct {
	Key     string `json:"key"`
	Version int64  `json:"version"`
	Value   any    `json:"value"`
}

func PutAll(puts ...PutInput) (m Mutation, err error) {
	jsonData, err := json.Marshal(puts)
	if err != nil {
		return Mutation{}, err
	}
	// This query is more complex...
	//
	// The input_data CTE extracts key, version, and value from the input JSON into a table.
	// The valid_updates CTE joins the input_data with the existing data, filtering out any invalid updates.
	//
	// The insert statement upserts the valid updates into the kv table, using a count of the input_data and valid_updates to ensure that if any invalid updates are present, the entire transaction is rolled back.
	m = Mutation{
		SQL: `with input_data as (
    select
        json_extract(value, '$.key') as key,
        json_extract(value, '$.version') as version,
        json_extract(value, '$.value') as value
    from json_each(:json_data)
),
valid_updates as (
    select
        input_data.key,
        input_data.version,
        input_data.value,
        kv.version as existing_version
    from input_data
    left join kv on kv.key = input_data.key
		where (input_data.version = -1 or kv.version = input_data.version) and (input_data.version <> 0)
)
insert into kv (key, version, value, created)
select
    valid_updates.key,
    1,
    jsonb(valid_updates.value),
		:now
from valid_updates
where (select count(*) from input_data) = (select count(*) from valid_updates)
on conflict(key) do update
set
    version = kv.version + 1,
    value = jsonb(excluded.value)
where (select count(*) from input_data) = (select count(*) from valid_updates);`,
		Args: map[string]any{
			":json_data": string(jsonData),
			":now":       now(),
		},
	}
	return m, nil
}

func Delete(key string) Mutation {
	return Mutation{
		SQL: `delete from kv where key = :key;`,
		Args: map[string]any{
			":key": key,
		},
	}
}

// SQLite supports the `limit` and `offset` clauses in `delete` statements, but
// it's a compiler option (SQLITE_ENABLE_UPDATE_DELETE_LIMIT) that is disabled
// by default (although it is enabled in Ubuntu and MacOS builds of sqlite).
//
// CTEs are not supported with a join, so the simplest way to delete a prefix
// is to use a subquery.

func DeletePrefix(prefix string, offset, limit int) Mutation {
	return Mutation{
		SQL: `delete from kv where key in (select key from kv where key like :prefix order by key limit :limit offset :offset);`,
		Args: map[string]any{
			":prefix": prefix + "%",
			":limit":  limit,
			":offset": offset,
		},
	}
}

func DeleteRange(from, to string, offset, limit int) Mutation {
	return Mutation{
		SQL: `delete from kv where key in (select key from kv where key >= :from and key < :to order by key limit :limit offset :offset);`,
		Args: map[string]any{
			":from":   from,
			":to":     to,
			":limit":  limit,
			":offset": offset,
		},
	}
}

func count() Query {
	return Query{
		SQL: `select count(*) from kv;`,
	}
}

func countPrefix(prefix string) Query {
	return Query{
		SQL: `select count(*) from kv where key like :prefix;`,
		Args: map[string]any{
			":prefix": prefix + "%",
		},
	}
}

func countRange(from, to string) Query {
	return Query{
		SQL: `select count(*) from kv where key >= :from and key < :to;`,
		Args: map[string]any{
			":from": from,
			":to":   to,
		},
	}
}

func Patch(key string, version int64, patch any) (m Mutation, err error) {
	jsonPatch, err := json.Marshal(patch)
	if err != nil {
		return Mutation{}, err
	}
	m = Mutation{
		SQL: `insert into kv (key, version, value, created)
values (:key, 1, jsonb(:value), :now)
on conflict(key) do update 
set version = excluded.version + 1, 
    value = jsonb_patch(kv.value, excluded.value)
where (:version = -1 or version = :version);`,
		Args: map[string]any{
			":key":     key,
			":version": version,
			":value":   string(jsonPatch),
			":now":     now(),
		},
	}
	return m, nil
}
