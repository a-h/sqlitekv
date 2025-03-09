package sqlitekv

import (
	"context"
	"testing"
)

func newDeleteTest(ctx context.Context, store Store[Person]) func(t *testing.T) {
	return func(t *testing.T) {
		defer store.DeletePrefix(ctx, "*", 0, -1)

		t.Run("Can delete", func(t *testing.T) {
			data := Person{
				Name:         "Alice",
				PhoneNumbers: []string{"123-456-7890"},
			}
			if err := store.Put(ctx, "delete", -1, data); err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}

			if err := store.Delete(ctx, "delete"); err != nil {
				t.Errorf("unexpected error deleting data: %v", err)
			}

			_, ok, err := store.Get(ctx, "delete")
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if ok {
				t.Error("expected data to be deleted")
			}
		})
		t.Run("Deleting non-existent keys does not return an error", func(t *testing.T) {
			if err := store.Delete(ctx, "delete-does-not-exist"); err != nil {
				t.Errorf("unexpected error deleting data: %v", err)
			}
		})
	}
}
