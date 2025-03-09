package sqlitekv

import (
	"context"
	"testing"
)

func newDeleteRangeTest(ctx context.Context, store Store[Person]) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("Can delete within a range", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			store.Put(ctx, "deleterange/a", -1, Person{Name: "Alice"})
			store.Put(ctx, "deleterange/b", -1, Person{Name: "Bob"})
			store.Put(ctx, "deleterange/c", -1, Person{Name: "Charlie"})

			deleted, err := store.DeleteRange(ctx, "deleterange/a", "deleterange/c", 0, -1)
			if err != nil {
				t.Errorf("unexpected error deleting data: %v", err)
			}
			if deleted != 2 {
				t.Errorf("expected 2 records to be deleted, got %d", deleted)
			}
			count, err := store.Count(ctx)
			if err != nil {
				t.Errorf("unexpected error counting data: %v", err)
			}
			if count != 1 {
				t.Errorf("expected 1 record, got %d", count)
			}
			_, ok, err := store.Get(ctx, "deleterange/c")
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Error("expected the c record to still exist")
			}
		})
		t.Run("Can limit the number of records deleted", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			store.Put(ctx, "deleterange/a", -1, Person{Name: "Alice"})
			store.Put(ctx, "deleterange/b", -1, Person{Name: "Bob"})
			store.Put(ctx, "deleterange/c", -1, Person{Name: "Charlie"})
			store.Put(ctx, "deleterange/d", -1, Person{Name: "David"})

			deleted, err := store.DeleteRange(ctx, "deleterange/b", "deleterange/d", 0, 1)
			if err != nil {
				t.Errorf("unexpected error deleting data: %v", err)
			}
			if deleted != 1 {
				t.Errorf("expected 1 record to be deleted, got %d", deleted)
			}
			count, err := store.Count(ctx)
			if err != nil {
				t.Errorf("unexpected error counting data: %v", err)
			}
			if count != 3 {
				t.Errorf("expected 3 records, got %d", count)
			}
			_, ok, err := store.Get(ctx, "deleterange/b")
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if ok {
				t.Error("expected the b record to be deleted")
			}
		})
		t.Run("Can offset the records deleted", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			store.Put(ctx, "deleterange/a", -1, Person{Name: "Alice"})
			store.Put(ctx, "deleterange/b", -1, Person{Name: "Bob"})
			store.Put(ctx, "deleterange/c", -1, Person{Name: "Charlie"})
			store.Put(ctx, "deleterange/d", -1, Person{Name: "David"})

			deleted, err := store.DeleteRange(ctx, "deleterange/a", "deleterange/d", 1, -1)
			if err != nil {
				t.Errorf("unexpected error deleting data: %v", err)
			}
			if deleted != 2 {
				t.Errorf("expected 2 records (b and c) to be deleted, got %d", deleted)
			}
			count, err := store.Count(ctx)
			if err != nil {
				t.Errorf("unexpected error counting data: %v", err)
			}
			if count != 2 {
				t.Errorf("expected 2 records (a and d), got %d", count)
			}
			_, ok, err := store.Get(ctx, "deleterange/a")
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Error("expected the a record to still exist")
			}
		})
	}
}
