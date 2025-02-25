package sqlitekv

import (
	"context"
	"testing"

	"zombiezen.com/go/sqlite/sqlitex"
)

type Person struct {
	Name         string   `json:"name"`
	PhoneNumbers []string `json:"phone_numbers"`
}

func TestStore(t *testing.T) {
	pool, err := sqlitex.NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	expected := Person{
		Name:         "Alice",
		PhoneNumbers: []string{"123-456-7890"},
	}

	ctx := context.Background()
	store := NewStore[Person](pool)
	if err := store.Init(ctx); err != nil {
		t.Fatalf("unexpected error initializing store: %v", err)
	}

	t.Run("Put", func(t *testing.T) {
		err := store.Put(ctx, "person/alice", -1, expected)
		if err != nil {
			t.Errorf("unexpected error putting data: %v", err)
		}
	})

	t.Run("Put overwrite with correct version", func(t *testing.T) {
		// Create.
		data := Person{
			Name:         "Jess",
			PhoneNumbers: []string{"123-456-7890"},
		}

		// Put data.
		err := store.Put(ctx, "put_overwrite", -1, data)
		if err != nil {
			t.Fatalf("unexpected error putting data: %v", err)
		}

		// Get data.
		record, ok, err := store.Get(ctx, "put_overwrite")
		if err != nil {
			t.Fatalf("unexpected error getting data: %v", err)
		}
		if !ok {
			t.Fatal("expected ok to be true, got false")
		}

		// Update version 1 of the record.
		data.Name = "Jessie"
		err = store.Put(ctx, "put_overwrite", record.Version, data)
		if err != nil {
			t.Fatalf("unexpected error putting data over version 1: %v", err)
		}

		// Get the record again.
		record, ok, err = store.Get(ctx, "put_overwrite")
		if err != nil {
			t.Fatalf("unexpected error getting data: %v", err)
		}
		if !ok {
			t.Fatal("expected data to be found")
		}
		if record.Value.Name != data.Name {
			t.Errorf("expected name %q, got %q", data.Name, record.Value.Name)
		}

		err = store.Put(ctx, "put_overwrite", 1, expected)
		if err == nil {
			t.Fatal("expected error putting data over old version, got nil")
		}
	})

	t.Run("Get", func(t *testing.T) {
		actual, ok, err := store.Get(ctx, "person/alice")
		if err != nil {
			t.Errorf("unexpected error getting data: %v", err)
		}
		if !ok {
			t.Error("expected data to be found")
		}

		if actual.Value.Name != expected.Name {
			t.Errorf("expected name %q, got %q", expected.Name, actual.Value.Name)
		}
		if len(actual.Value.PhoneNumbers) != len(expected.PhoneNumbers) {
			t.Errorf("expected %d phone numbers, got %d", len(expected.PhoneNumbers), len(actual.Value.PhoneNumbers))
		}
		for i, expectedNumber := range expected.PhoneNumbers {
			if actual.Value.PhoneNumbers[i] != expectedNumber {
				t.Errorf("expected phone number %q, got %q", expectedNumber, actual.Value.PhoneNumbers[i])
			}
		}
	})

	t.Run("Delete", func(t *testing.T) {
		if err := store.Delete(ctx, "person/alice"); err != nil {
			t.Errorf("unexpected error deleting data: %v", err)
		}

		_, ok, err := store.Get(ctx, "person/alice")
		if err != nil {
			t.Errorf("unexpected error getting data: %v", err)
		}
		if ok {
			t.Error("expected data to be deleted")
		}
	})

	t.Run("GetPrefix", func(t *testing.T) {
		item1 := Person{
			Name:         "Bob",
			PhoneNumbers: []string{"234-567-8901"},
		}
		item2 := Person{
			Name:         "Charlie",
			PhoneNumbers: []string{"345-678-9012"},
		}
		item3 := Person{
			Name:         "David",
			PhoneNumbers: []string{"456-789-0123"},
		}

		if err := store.Put(ctx, "person/bob", -1, item1); err != nil {
			t.Errorf("unexpected error putting data: %v", err)
		}
		if err := store.Put(ctx, "person/charlie", -1, item2); err != nil {
			t.Errorf("unexpected error putting data: %v", err)
		}
		if err := store.Put(ctx, "person/david", -1, item3); err != nil {
			t.Errorf("unexpected error putting data: %v", err)
		}

		records, err := store.GetPrefix(ctx, "person/")
		if err != nil {
			t.Errorf("unexpected error getting data: %v", err)
		}
		if len(records) != 3 {
			t.Errorf("expected 3 records, got %d", len(records))
		}
		if records[0].Key != "person/bob" {
			t.Errorf("expected key %q, got %q", "person/bob", records[0].Key)
		}
		if records[1].Key != "person/charlie" {
			t.Errorf("expected key %q, got %q", "person/charlie", records[1].Key)
		}
		if records[2].Key != "person/david" {
			t.Errorf("expected key %q, got %q", "person/david", records[2].Key)
		}
	})

	t.Run("DeletePrefix", func(t *testing.T) {
		count, err := store.Count(ctx)
		if err != nil {
			t.Errorf("unexpected error counting data: %v", err)
		}
		if count == 0 {
			t.Fatal("expected data to exist")
		}

		err = store.DeletePrefix(ctx, "")
		if err != nil {
			t.Errorf("unexpected error deleting data: %v", err)
		}

		newCount, err := store.Count(ctx)
		if err != nil {
			t.Errorf("unexpected error counting data: %v", err)
		}
		if newCount != 0 {
			t.Errorf("expected 0 records, got %d", newCount)
		}
	})

	t.Run("List", func(t *testing.T) {
		listItems := []Person{
			{
				Name: "Eve",
			},
			{
				Name: "Steve",
			},
			{
				Name: "Bob",
			},
		}
		for _, item := range listItems {
			if err := store.Put(ctx, "list/"+item.Name, -1, item); err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
		}

		records, err := store.List(ctx, 0, 2)
		if err != nil {
			t.Errorf("unexpected error listing data: %v", err)
		}
		if len(records) != 2 {
			t.Errorf("expected 2 records, got %d", len(records))
		}

		records, err = store.List(ctx, 2, 2)
		if err != nil {
			t.Errorf("unexpected error listing data: %v", err)
		}
		if len(records) != 1 {
			t.Errorf("expected 1 record, got %d", len(records))
		}
	})

	t.Run("Patch", func(t *testing.T) {
		// Create.
		data := Person{
			Name:         "Jess",
			PhoneNumbers: []string{"123-456-7890"},
		}

		// Put data.
		err := store.Put(ctx, "patch", -1, data)
		if err != nil {
			t.Fatalf("unexpected error putting data: %v", err)
		}

		// Patch data.
		patch := map[string]any{
			"name": "Jessie",
		}
		err = store.Patch(ctx, "patch", -1, patch)
		if err != nil {
			t.Fatalf("unexpected error patching data: %v", err)
		}

		// Get the record again.
		record, ok, err := store.Get(ctx, "patch")
		if err != nil {
			t.Fatalf("unexpected error getting data: %v", err)
		}
		if !ok {
			t.Fatal("expected data to be found")
		}
		if record.Value.Name != patch["name"].(string) {
			t.Errorf("expected name %q, got %q", patch["name"].(string), record.Value.Name)
		}
	})
}
