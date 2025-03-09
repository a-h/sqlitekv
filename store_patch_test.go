package sqlitekv

import (
	"context"
	"testing"
)

func newPatchTest(ctx context.Context, store Store) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("Can patch data", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

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

			// Get the updated again.
			var updated Person
			_, ok, err := store.Get(ctx, "patch", &updated)
			if err != nil {
				t.Fatalf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Fatal("expected data to be found")
			}
			if updated.Name != patch["name"].(string) {
				t.Errorf("expected name %q, got %q", patch["name"].(string), updated.Name)
			}
		})
		t.Run("Patching a non-existent record creates it", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			// Patch data.
			patch := map[string]any{
				"name": "Jessie",
			}
			err := store.Patch(ctx, "patch-does-not-exist", -1, patch)
			if err != nil {
				t.Fatalf("unexpected error patching data: %v", err)
			}

			// Get the updated again.
			var updated Person
			_, ok, err := store.Get(ctx, "patch-does-not-exist", &updated)
			if err != nil {
				t.Fatalf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Fatal("expected data to be found")
			}
			if updated.Name != patch["name"].(string) {
				t.Errorf("expected name %q, got %q", patch["name"].(string), updated.Name)
			}
		})
	}
}
