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
		t.Run("The created field is set and not updated", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			data := map[string]any{
				"key": "value",
			}
			err := store.Patch(ctx, "put", -1, data)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}

			var v struct{}
			r1, ok, err := store.Get(ctx, "put", &v)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Error("expected data not found")
			}
			if r1.Created.IsZero() {
				t.Error("expected created time to be set")
			}

			// Update.
			data["key"] = "value2"
			err = store.Patch(ctx, "put", -1, data)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
			r2, ok, err := store.Get(ctx, "put", &v)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Error("expected data not found")
			}
			if !r2.Created.Equal(r1.Created) {
				t.Errorf("expected created time to not change from %v, but got %v", r1.Created, r2.Created)
			}
		})
	}
}
