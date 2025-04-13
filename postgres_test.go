package sqlitekv

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestPostgres(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://postgres:secret@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		t.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer conn.Close(ctx)

	db := NewPostgres(conn)
	store := NewStore(db)
	runStoreTests(t, store)
}
