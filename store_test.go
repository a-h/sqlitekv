package sqlitekv

import (
	"context"
	"testing"
)

type Person struct {
	Name         string   `json:"name"`
	PhoneNumbers []string `json:"phone_numbers"`
}

func (p Person) Equals(other Person) bool {
	if p.Name != other.Name {
		return false
	}
	if len(p.PhoneNumbers) != len(other.PhoneNumbers) {
		return false
	}
	for i, number := range p.PhoneNumbers {
		if number != other.PhoneNumbers[i] {
			return false
		}
	}
	return true
}

func personSliceIsEqual(a, b []Person) bool {
	if len(a) != len(b) {
		return false
	}
	for i, p := range a {
		if !p.Equals(b[i]) {
			return false
		}
	}
	return true
}

func runStoreTests(t *testing.T, store Store) {
	ctx := context.Background()
	if err := store.Init(ctx); err != nil {
		t.Fatalf("unexpected error initializing store: %v", err)
	}

	// Clear the data before running the tests.
	if _, err := store.DeletePrefix(ctx, "*", 0, -1); err != nil {
		t.Fatalf("unexpected error clearing data: %v", err)
	}

	t.Run("Get", newGetTest(ctx, store))
	t.Run("GetPrefix", newGetPrefixTest(ctx, store))
	t.Run("GetRange", newGetRangeTest(ctx, store))
	t.Run("List", newListTest(ctx, store))
	t.Run("Put", newPutTest(ctx, store))
	t.Run("Delete", newDeleteTest(ctx, store))
	t.Run("DeletePrefix", newDeletePrefixTest(ctx, store))
	t.Run("DeleteRange", newDeleteRangeTest(ctx, store))
	t.Run("Count", newCountTest(ctx, store))
	t.Run("CountPrefix", newCountPrefixTest(ctx, store))
	t.Run("CountRange", newCountRangeTest(ctx, store))
	t.Run("Patch", newPatchTest(ctx, store))

	deleted, err := store.DeletePrefix(ctx, "*", 0, -1)
	if err != nil {
		t.Fatalf("unexpected error clearing data after tests: %v", err)
	}
	if deleted > 0 {
		t.Fatalf("expected all data to be deleted after tests, got %d items", deleted)
	}
}
