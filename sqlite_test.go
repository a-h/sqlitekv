package sqlitekv

import (
	"testing"

	"zombiezen.com/go/sqlite/sqlitex"
)

func TestSqlite(t *testing.T) {
	pool, err := sqlitex.NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	db := NewSqlite(pool)
	store := NewStore(db)
	runStoreTests(t, store)
}
