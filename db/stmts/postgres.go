package stmts

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"math"

	"github.com/a-h/sqlitekv/db"
)

type Postgres struct{}

func (pg Postgres) isStatementSet() db.StatementSet {
	return pg
}

func (Postgres) Init() []db.Mutation {
	return []db.Mutation{
		{
			SQL: `CREATE TABLE IF NOT EXISTS kv (
    key text PRIMARY KEY,
    version integer,
    value jsonb,
    created timestamptz
);`,
		},
		{
			SQL: `CREATE INDEX IF NOT EXISTS kv_key ON kv(key);`,
		},
		{
			SQL: `CREATE INDEX IF NOT EXISTS kv_created ON kv(created);`,
		},
	}
}

func (Postgres) Get(key string) db.Query {
	return db.Query{
		SQL: `SELECT key, version, value, created FROM kv WHERE key = @key;`,
		Args: map[string]any{
			"key": key,
		},
	}
}

func (Postgres) GetPrefix(prefix string, offset, limit int) db.Query {
	if limit < 0 {
		limit = math.MaxInt - 1
	}
	return db.Query{
		SQL: `SELECT key, version, value, created FROM kv WHERE key LIKE @prefix ORDER BY key LIMIT @limit OFFSET @offset;`,
		Args: map[string]any{
			"prefix": prefix + "%",
			"limit":  limit,
			"offset": offset,
		},
	}
}

func (Postgres) GetRange(from, to string, offset, limit int) db.Query {
	if limit < 0 {
		limit = math.MaxInt - 1
	}
	return db.Query{
		SQL: `SELECT key, version, value, created FROM kv WHERE key >= @from AND key < @to ORDER BY key LIMIT @limit OFFSET @offset;`,
		Args: map[string]any{
			"from":   from,
			"to":     to,
			"limit":  limit,
			"offset": offset,
		},
	}
}

func (Postgres) List(offset, limit int) db.Query {
	if limit < 0 {
		limit = math.MaxInt - 1
	}
	return db.Query{
		SQL: `SELECT key, version, value, created FROM kv ORDER BY key LIMIT @limit OFFSET @offset;`,
		Args: map[string]any{
			"limit":  limit,
			"offset": offset,
		},
	}
}

func (Postgres) Put(key string, version int64, value any) (m db.Mutation) {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return db.Mutation{
			ArgsError: err,
		}
	}
	return db.Mutation{
		SQL: `INSERT INTO kv (key, version, value, created)
VALUES (@key, 1, @value::jsonb, @now)
ON CONFLICT (key) DO UPDATE
SET version = kv.version + 1,
    value = @value::jsonb
WHERE (@version = -1 OR kv.version = @version) AND (@version <> 0);`,
		Args: map[string]any{
			"key":     key,
			"version": version,
			"value":   string(jsonValue),
			"now":     now(),
		},
		MustAffectRows: true,
	}
}

//go:embed putpatch_postgres.sql
var putPatchSQL string

func (Postgres) PutPatches(operations ...db.PutPatchInput) (m db.Mutation) {
	for _, op := range operations {
		if !(op.Operation == db.OperationPut || op.Operation == db.OperationPatch) {
			return db.Mutation{
				ArgsError: fmt.Errorf("putpatchinput: invalid operation type: %v", op.Operation),
			}
		}
	}
	opsJSON, err := json.Marshal(operations)
	if err != nil {
		return db.Mutation{
			ArgsError: err,
		}
	}
	return db.Mutation{
		SQL: putPatchSQL,
		Args: map[string]any{
			"input_data": string(opsJSON),
			"now":        now(),
		},
		MustAffectRows: true,
	}
}

func (Postgres) DeleteKeys(keys ...string) (m db.Mutation) {
	keysJSON, err := json.Marshal(keys)
	if err != nil {
		return db.Mutation{
			ArgsError: err,
		}
	}
	return db.Mutation{
		SQL: `DELETE FROM kv WHERE key IN (SELECT jsonb_array_elements_text(@keys::jsonb));`,
		Args: map[string]any{
			"keys": string(keysJSON),
		},
	}
}

func (Postgres) Delete(key string) db.Mutation {
	return db.Mutation{
		SQL: `DELETE FROM kv WHERE key = @key;`,
		Args: map[string]any{
			"key": key,
		},
	}
}

func (Postgres) DeletePrefix(prefix string, offset, limit int) db.Mutation {
	if limit < 0 {
		limit = math.MaxInt - 1
	}
	return db.Mutation{
		SQL: `DELETE FROM kv WHERE key IN (
			SELECT key FROM kv WHERE key LIKE @prefix ORDER BY key LIMIT @limit OFFSET @offset
		);`,
		Args: map[string]any{
			"prefix": prefix + "%",
			"limit":  limit,
			"offset": offset,
		},
	}
}

func (Postgres) DeleteRange(from, to string, offset, limit int) db.Mutation {
	if limit < 0 {
		limit = math.MaxInt - 1
	}
	return db.Mutation{
		SQL: `DELETE FROM kv WHERE key IN (
			SELECT key FROM kv WHERE key >= @from AND key < @to ORDER BY key LIMIT @limit OFFSET @offset
		);`,
		Args: map[string]any{
			"from":   from,
			"to":     to,
			"limit":  limit,
			"offset": offset,
		},
	}
}

func (Postgres) Count() db.Query {
	return db.Query{
		SQL: `SELECT count(*) FROM kv;`,
	}
}

func (Postgres) CountPrefix(prefix string) db.Query {
	return db.Query{
		SQL: `SELECT count(*) FROM kv WHERE key LIKE @prefix;`,
		Args: map[string]any{
			"prefix": prefix + "%",
		},
	}
}

func (Postgres) CountRange(from, to string) db.Query {
	return db.Query{
		SQL: `SELECT count(*) FROM kv WHERE key >= @from AND key < @to;`,
		Args: map[string]any{
			"from": from,
			"to":   to,
		},
	}
}

func (Postgres) Patch(key string, version int64, patch any) (m db.Mutation) {
	jsonPatch, err := json.Marshal(patch)
	if err != nil {
		return db.Mutation{
			ArgsError: err,
		}
	}
	return db.Mutation{
		SQL: `INSERT INTO kv (key, version, value, created)
VALUES (@key, 1, @value::jsonb, @now)
ON CONFLICT (key) DO UPDATE
SET version = kv.version + 1,
    value = kv.value || @value::jsonb
WHERE (@version = -1 OR kv.version = @version);`,
		Args: map[string]any{
			"key":     key,
			"version": version,
			"value":   string(jsonPatch),
			"now":     now(),
		},
	}
}
