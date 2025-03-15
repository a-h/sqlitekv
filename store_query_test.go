package sqlitekv

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/a-h/sqlitekv/db"
)

func newQueryTest(ctx context.Context, store Store) func(t *testing.T) {
	return func(t *testing.T) {
		defer store.DeletePrefix(ctx, "*", 0, -1)

		inputs := []Person{
			{
				Name:         "Alice",
				PhoneNumbers: []string{"123-456-7890"},
			},
			{
				Name:         "Bob",
				PhoneNumbers: []string{"234-567-8901"},
			},
			{
				Name:         "Charlie",
				PhoneNumbers: []string{"345-678-9012"},
			},
		}
		insertTimes := []time.Time{
			time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
			time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC),
		}

		for i, person := range inputs {
			db.TestTime = insertTimes[i]
			if err := store.Put(ctx, "query/"+strings.ToLower(person.Name), -1, person); err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
		}
		db.TestTime = time.Time{}

		t.Run("Can query on values within JSON", func(t *testing.T) {
			actual, err := store.Query(ctx, "select key, version, json(value) as value, created from kv where value ->> '$.name' = :name", map[string]any{":name": "Alice"})
			if err != nil {
				t.Fatalf("unexpected error getting data: %v", err)
			}
			if len(actual) != 1 {
				t.Fatalf("expected 1 result, got %d", len(actual))
			}
			values, err := ValuesOf[map[string]any](actual)
			if err != nil {
				t.Fatalf("unexpected error getting values: %v", err)
			}
			if values[0]["name"] != "Alice" {
				t.Errorf("expected Alice, got %s", values[0]["name"])
			}
		})
		t.Run("Can query on created time", func(t *testing.T) {
			actual, err := store.Query(ctx, "select key, version, json(value) as value, created from kv where created >= :created", map[string]any{":created": insertTimes[2].Format(time.RFC3339Nano)})
			if err != nil {
				t.Fatalf("unexpected error getting data: %v", err)
			}
			if len(actual) != 1 {
				t.Fatalf("expected 1 results, got %d", len(actual))
			}
		})
	}
}
