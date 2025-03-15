package sqlitekv

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/a-h/sqlitekv/db"
)

func newMutateTest(ctx context.Context, store Store) func(t *testing.T) {
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
			{
				Name: "delete1",
			},
			{
				Name: "delete2",
			},
		}
		insertTimes := []time.Time{
			time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
			time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC),
			time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC),
			time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC),
		}

		for i, person := range inputs {
			db.TestTime = insertTimes[i]
			if err := store.Put(ctx, "mutate/"+strings.ToLower(person.Name), -1, person); err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
		}
		db.TestTime = time.Time{}

		t.Run("Can delete multiple values", func(t *testing.T) {
			keysToDelete := []string{"mutate/delete1", "mutate/delete2"}
			jsonKeysToDelete, err := json.Marshal(keysToDelete)
			if err != nil {
				t.Fatalf("unexpected error marshsalling JSON: %v", err)
			}

			args := map[string]any{
				":keys": string(jsonKeysToDelete),
			}
			rowsAffected, err := store.Mutate(ctx, "delete from kv where key in (select value from json_each(:keys))", args)
			if err != nil {
				t.Fatalf("failed to delete keys: %v", err)
			}
			if rowsAffected != 2 {
				t.Errorf("expected to delete 2 keys, got %d", rowsAffected)
			}
			count, err := store.CountPrefix(ctx, "mutate/delete")
			if err != nil {
				t.Errorf("failed to count keys: %v", err)
			}
			if count != 0 {
				t.Errorf("expected keys to be deleted, but got count of %d", count)
			}
		})

		t.Run("Can delete based on values within JSON", func(t *testing.T) {
			rowsAffected, err := store.Mutate(ctx, "delete from kv where value ->> '$.name' = :name", map[string]any{":name": "Alice"})
			if err != nil {
				t.Fatalf("unexpected error getting data: %v", err)
			}
			if rowsAffected != 1 {
				t.Fatalf("expected 1 row affected, got %d", rowsAffected)
			}
		})
		t.Run("Can do conditional update based on values within JSON", func(t *testing.T) {
			rowsAffected, err := store.Mutate(ctx, "update kv set version = version + 1, value = json_set(value, '$.phone_numbers', json('[\"123-456-7890\",\"999-999-9999\"]')) where value ->> '$.name' = :name", map[string]any{":name": "Bob"})
			if err != nil {
				t.Fatalf("unexpected error getting data: %v", err)
			}
			if rowsAffected != 1 {
				t.Fatalf("expected 1 row affected, got %d", rowsAffected)
			}

			var person Person
			r, ok, err := store.Get(ctx, "mutate/bob", &person)
			if err != nil {
				t.Fatalf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Fatalf("expected to find a record")
			}
			if r.Version != 2 {
				t.Fatalf("expected version 2, got %d", r.Version)
			}
			if len(person.PhoneNumbers) != 2 {
				t.Fatalf("expected 2 phone numbers, got %d", len(person.PhoneNumbers))
			}
			if person.PhoneNumbers[0] != "123-456-7890" {
				t.Fatalf("expected 123-456-7890, got %s", person.PhoneNumbers[0])
			}
			if person.PhoneNumbers[1] != "999-999-9999" {
				t.Fatalf("expected 999-999-9999, got %s", person.PhoneNumbers[1])
			}
		})
	}
}
