package stmts

import (
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/a-h/sqlitekv/db"
)

type SQLite struct {
}

func (ss SQLite) isStatementSet() db.StatementSet {
	return ss
}

func (SQLite) Init() []db.Mutation {
	return []db.Mutation{
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

func (SQLite) Get(key string) db.Query {
	return db.Query{
		SQL: `select key, version, json(value) as value, created from kv where key = :key;`,
		Args: map[string]any{
			":key": key,
		},
	}
}

func (SQLite) GetPrefix(prefix string, offset, limit int) db.Query {
	return db.Query{
		SQL: `select key, version, json(value) as value, created from kv where key like :prefix order by key limit :limit offset :offset;`,
		Args: map[string]any{
			":prefix": prefix + "%",
			":limit":  limit,
			":offset": offset,
		},
	}
}

func (SQLite) GetRange(from, to string, offset, limit int) db.Query {
	return db.Query{
		SQL: `select key, version, json(value) as value, created from kv where key >= :from and key < :to order by key limit :limit offset :offset;`,
		Args: map[string]any{
			":from":   from,
			":to":     to,
			":limit":  limit,
			":offset": offset,
		},
	}
}

func (SQLite) List(offset, limit int) db.Query {
	return db.Query{
		SQL: `select key, version, json(value) as value, created from kv order by key limit :limit offset :offset;`,
		Args: map[string]any{
			":offset": offset,
			":limit":  limit,
		},
	}
}

func (SQLite) Put(key string, version int64, value any) (m db.Mutation) {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return db.Mutation{
			ArgsError: err,
		}
	}
	return db.Mutation{
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

//go:embed putpatch_sqlite.sql
var putPatchSQLiteSQL string

func (SQLite) PutPatches(operations ...db.PutPatchInput) (m db.Mutation) {
	for _, op := range operations {
		if !(op.Operation == db.OperationPut || op.Operation == db.OperationPatch) {
			return db.Mutation{
				ArgsError: fmt.Errorf("putpatchinput: invalid operation type: %v", op.Operation),
			}
		}
	}
	putsAndPatchesJSON, err := json.Marshal(operations)
	if err != nil {
		return db.Mutation{
			ArgsError: err,
		}
	}
	return db.Mutation{
		SQL: putPatchSQLiteSQL,
		Args: map[string]any{
			":input_data": string(putsAndPatchesJSON),
			":now":        now(),
		},
		MustAffectRows: true,
	}
}

func (SQLite) DeleteKeys(keys ...string) (m db.Mutation) {
	keysJSON, err := json.Marshal(keys)
	if err != nil {
		return db.Mutation{
			ArgsError: err,
		}
	}
	return db.Mutation{
		SQL: `delete from kv where key in (select value from json_each(:keys))`,
		Args: map[string]any{
			":keys": string(keysJSON),
		},
	}
}

func (SQLite) Delete(key string) db.Mutation {
	return db.Mutation{
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

func (SQLite) DeletePrefix(prefix string, offset, limit int) db.Mutation {
	return db.Mutation{
		SQL: `delete from kv where key in (select key from kv where key like :prefix order by key limit :limit offset :offset);`,
		Args: map[string]any{
			":prefix": prefix + "%",
			":limit":  limit,
			":offset": offset,
		},
	}
}

func (SQLite) DeleteRange(from, to string, offset, limit int) db.Mutation {
	return db.Mutation{
		SQL: `delete from kv where key in (select key from kv where key >= :from and key < :to order by key limit :limit offset :offset);`,
		Args: map[string]any{
			":from":   from,
			":to":     to,
			":limit":  limit,
			":offset": offset,
		},
	}
}

func (SQLite) Count() db.Query {
	return db.Query{
		SQL: `select count(*) from kv;`,
	}
}

func (SQLite) CountPrefix(prefix string) db.Query {
	return db.Query{
		SQL: `select count(*) from kv where key like :prefix;`,
		Args: map[string]any{
			":prefix": prefix + "%",
		},
	}
}

func (SQLite) CountRange(from, to string) db.Query {
	return db.Query{
		SQL: `select count(*) from kv where key >= :from and key < :to;`,
		Args: map[string]any{
			":from": from,
			":to":   to,
		},
	}
}

func (SQLite) Patch(key string, version int64, patch any) (m db.Mutation) {
	jsonPatch, err := json.Marshal(patch)
	if err != nil {
		return db.Mutation{
			ArgsError: err,
		}
	}
	return db.Mutation{
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
