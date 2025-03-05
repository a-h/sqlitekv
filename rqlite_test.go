package sqlitekv

import (
	"testing"

	rqlitehttp "github.com/rqlite/rqlite-go-http"
)

func TestRqlite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	client := rqlitehttp.NewClient("http://localhost:4001", nil)
	// Username and password configured in auth.json.
	client.SetBasicAuth("admin", "secret")

	store := NewRqlite[Person](client)
	runStoreTests(t, store)
}
