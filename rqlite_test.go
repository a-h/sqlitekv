package sqlitekv

import (
	"testing"

	rqlitehttp "github.com/rqlite/rqlite-go-http"
)

func TestRqlite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	client, err := rqlitehttp.NewClient("http://localhost:4001", nil)
	if err != nil {
		t.Fatalf("failed to create rqlite client: %v", err)
	}
	// Username and password configured in auth.json.
	client.SetBasicAuth("admin", "secret")

	db := NewRqlite(client)
	store := NewStore(db)
	runStoreTests(t, store)
}
