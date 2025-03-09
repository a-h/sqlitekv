package sqlitekv

import (
	"context"
	"testing"
)

func newGetTest(ctx context.Context, store Store[Person]) func(t *testing.T) {
	return func(t *testing.T) {
		defer store.DeletePrefix(ctx, "*", 0, -1)

		expected := Person{
			Name:         "Alice",
			PhoneNumbers: []string{"123-456-7890", "234-567-8901"},
		}
		if err := store.Put(ctx, "get", -1, expected); err != nil {
			t.Errorf("unexpected error putting data: %v", err)
		}

		t.Run("Can get data", func(t *testing.T) {
			actual, ok, err := store.Get(ctx, "get")
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Error("expected data to be found")
			}
			if !actual.Value.Equals(expected) {
				t.Errorf("expected %#v, got %#v", expected, actual.Value)
			}
			if actual.Version != 1 {
				t.Errorf("expected version 1, got %d", actual.Version)
			}
		})
		t.Run("Returns ok=false if the key does not exist", func(t *testing.T) {
			_, ok, err := store.Get(ctx, "get-does-not-exist")
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if ok {
				t.Error("expected data not to be found")
			}
		})
	}
}
