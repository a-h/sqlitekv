package db

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"time"
)

var TestTime time.Time

func now() string {
	if !TestTime.IsZero() {
		return TestTime.UTC().Format(time.RFC3339Nano)
	}
	return time.Now().UTC().Format(time.RFC3339Nano)
}

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

func Put(key string, version int64, value any) (m Mutation) {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return Mutation{
			ArgsError: err,
		}
	}
	return Mutation{
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
		MustAffectRows: true,
	}
}

type PutPatchInput struct {
	Key       string    `json:"key"`
	Version   int64     `json:"version"`
	Value     any       `json:"value"`
	Operation Operation `json:"operation"`
}

type Operation string

var OperationPut Operation = "put"
var OperationPatch Operation = "patch"

func PutInput(key string, version int64, value any) PutPatchInput {
	return PutPatchInput{
		Key:       key,
		Version:   version,
		Value:     value,
		Operation: OperationPut,
	}
}

func PatchInput(key string, version int64, patch any) PutPatchInput {
	return PutPatchInput{
		Key:       key,
		Version:   version,
		Value:     patch,
		Operation: OperationPatch,
	}
}

//go:embed putpatch.sql
var putPatchSQL string

func PutPatches(operations ...PutPatchInput) (m Mutation) {
	for _, op := range operations {
		if !(op.Operation == OperationPut || op.Operation == OperationPatch) {
			return Mutation{
				ArgsError: fmt.Errorf("putpatchinput: invalid operation type: %v", op.Operation),
			}
		}
	}
	putsAndPatchesJSON, err := json.Marshal(operations)
	if err != nil {
		return Mutation{
			ArgsError: err,
		}
	}
	return Mutation{
		SQL: putPatchSQL,
		Args: map[string]any{
			":input_data": string(putsAndPatchesJSON),
			":now":        now(),
		},
		MustAffectRows: true,
	}
}

func DeleteKeys(keys ...string) (m Mutation) {
	keysJSON, err := json.Marshal(keys)
	if err != nil {
		return Mutation{
			ArgsError: err,
		}
	}
	return Mutation{
		SQL: `delete from kv where key in (select value from json_each(:keys))`,
		Args: map[string]any{
			":keys": string(keysJSON),
		},
	}
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

func Count() Query {
	return Query{
		SQL: `select count(*) from kv;`,
	}
}

func CountPrefix(prefix string) Query {
	return Query{
		SQL: `select count(*) from kv where key like :prefix;`,
		Args: map[string]any{
			":prefix": prefix + "%",
		},
	}
}

func CountRange(from, to string) Query {
	return Query{
		SQL: `select count(*) from kv where key >= :from and key < :to;`,
		Args: map[string]any{
			":from": from,
			":to":   to,
		},
	}
}

func Patch(key string, version int64, patch any) (m Mutation) {
	jsonPatch, err := json.Marshal(patch)
	if err != nil {
		return Mutation{
			ArgsError: err,
		}
	}
	return Mutation{
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
}
