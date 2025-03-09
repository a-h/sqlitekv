package sqlitekv

import "encoding/json"

func Init() []MutationInput {
	return []MutationInput{
		{
			SQL:  `create table if not exists kv (key text primary key, version integer, value jsonb) without rowid;`,
			Args: argsOf(nil),
		},
		{
			SQL:  `create index if not exists kv_key on kv(key);`,
			Args: argsOf(nil),
		},
	}
}

func argsOf(args map[string]any) func() (map[string]any, error) {
	return func() (map[string]any, error) {
		return args, nil
	}
}

func Get(key string) QueryInput {
	return QueryInput{
		SQL: `select key, version, json(value) as value from kv where key = :key;`,
		Args: argsOf(map[string]any{
			":key": key,
		}),
	}
}

func GetPrefix(prefix string, offset, limit int) QueryInput {
	return QueryInput{
		SQL: `select key, version, json(value) as value from kv where key like :prefix order by key limit :limit offset :offset;`,
		Args: argsOf(map[string]any{
			":prefix": prefix + "%",
			":limit":  limit,
			":offset": offset,
		}),
	}
}

func GetRange(from, to string, offset, limit int) QueryInput {
	return QueryInput{
		SQL: `select key, version, json(value) as value from kv where key >= :from and key < :to order by key limit :limit offset :offset;`,
		Args: argsOf(map[string]any{
			":from":   from,
			":to":     to,
			":limit":  limit,
			":offset": offset,
		}),
	}
}

func List(offset, limit int) QueryInput {
	return QueryInput{
		SQL: `select key, version, json(value) as value from kv order by key limit :limit offset :offset;`,
		Args: argsOf(map[string]any{
			":offset": offset,
			":limit":  limit,
		}),
	}
}

func Put(key string, version int64, value any) MutationInput {
	return MutationInput{
		SQL: `insert into kv (key, version, value) values (:key, 1, jsonb(:value)) on conflict(key) do update set version = excluded.version + 1, value = jsonb(excluded.value) where (:version = -1 or version = :version) and (version <> 0);`,
		Args: func() (args map[string]any, err error) {
			jsonValue, err := json.Marshal(value)
			if err != nil {
				return map[string]any{}, err
			}
			return map[string]any{
				":key":     key,
				":version": version,
				":value":   string(jsonValue),
			}, nil
		},
	}
}

func Delete(key string) MutationInput {
	return MutationInput{
		SQL: `delete from kv where key = :key;`,
		Args: argsOf(map[string]any{
			":key": key,
		}),
	}
}

// SQLite supports the `limit` and `offset` clauses in `delete` statements, but
// it's a compiler option (SQLITE_ENABLE_UPDATE_DELETE_LIMIT) that is disabled
// by default (although it is enabled in Ubuntu and MacOS builds of sqlite).
//
// CTEs are not supported with a join, so the simplest way to delete a prefix
// is to use a subquery.

func DeletePrefix(prefix string, offset, limit int) MutationInput {
	return MutationInput{
		SQL: `delete from kv where key in (select key from kv where key like :prefix order by key limit :limit offset :offset);`,
		Args: argsOf(map[string]any{
			":prefix": prefix + "%",
			":limit":  limit,
			":offset": offset,
		}),
	}
}

func DeleteRange(from, to string, offset, limit int) MutationInput {
	return MutationInput{
		SQL: `delete from kv where key in (select key from kv where key >= :from and key < :to order by key limit :limit offset :offset);`,
		Args: argsOf(map[string]any{
			":from":   from,
			":to":     to,
			":limit":  limit,
			":offset": offset,
		}),
	}
}

func count() QueryInput {
	return QueryInput{
		SQL: `select count(*) from kv;`,
		Args: func() (map[string]any, error) {
			return map[string]any{}, nil
		},
	}
}

func countPrefix(prefix string) QueryInput {
	return QueryInput{
		SQL: `select count(*) from kv where key like :prefix;`,
		Args: argsOf(map[string]any{
			":prefix": prefix + "%",
		}),
	}
}

func countRange(from, to string) QueryInput {
	return QueryInput{
		SQL: `select count(*) from kv where key >= :from and key < :to;`,
		Args: argsOf(map[string]any{
			":from": from,
			":to":   to,
		}),
	}
}

func Patch(key string, version int64, patch any) MutationInput {
	return MutationInput{
		SQL: `insert into kv (key, version, value) values (:key, 1, jsonb(:value)) on conflict(key) do update set version = excluded.version + 1, value = jsonb_patch(kv.value, excluded.value) where (:version = -1 or version = :version);`,
		Args: func() (map[string]any, error) {
			jsonPatch, err := json.Marshal(patch)
			if err != nil {
				return map[string]any{}, err
			}
			return map[string]any{
				":key":     key,
				":version": version,
				":value":   string(jsonPatch),
			}, nil
		},
	}
}
