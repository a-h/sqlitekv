package sqlitekv

import (
	"context"
	"strings"
	"testing"
)

type putTestData struct {
	Value string `json:"value"`
}

func newPutAllTest(ctx context.Context, store Store) func(t *testing.T) {
	return func(t *testing.T) {
		defer store.DeletePrefix(ctx, "*", 0, -1)

		t.Run("PutAll is transactional", func(t *testing.T) {
			// Put two keys.
			// Then, attempt to update both of them.
			// One with an incorrect version number, the other with the correct version number.
			// The transaction should fail, and neither key should be updated.
			initial := []PutInput{
				{
					Key:     "putall-1",
					Version: -1,
					Value:   putTestData{Value: "value-1"},
				},
				{
					Key:     "putall-2",
					Version: -1,
					Value:   putTestData{Value: "value-2"},
				},
				{
					Key:     "putall-3",
					Version: -1,
					Value:   putTestData{Value: "value-3"},
				},
			}
			if err := store.PutAll(ctx, initial...); err != nil {
				t.Fatalf("unexpected error putting data: %v", err)
			}

			// Updates.
			updates := []PutInput{
				{
					Key:     "putall-1",
					Version: 1, // Correct version, update should succeed.
					Value:   putTestData{Value: "value-1-updated"},
				},
				{
					Key:     "putall-2",
					Version: -1, // Don't care about version, update should succeed.
					Value:   putTestData{Value: "value-2-updated"},
				},
				{
					Key:     "putall-3",
					Version: 2, // Incorrect version, update should fail.
					Value:   putTestData{Value: "value-3-updated"},
				},
				{
					Key:     "putall-4",
					Version: 0, // Key does not exist, insert should succeed.
					Value:   putTestData{Value: "value-4"},
				},
			}
			err := store.PutAll(ctx, updates...)
			if err == nil {
				t.Errorf("expected error, because one of the updates should fail, but got nil")
			}

			// Check that the count of the prefix is still 3.
			count, err := store.CountPrefix(ctx, "putall")
			if err != nil {
				t.Fatalf("unexpected error getting count: %v", err)
			}
			if count != 3 {
				t.Errorf("expected count 3, got %d", count)
			}

			// Check that the values were not updated.
			actual := make([]putTestData, len(initial))
			for i, input := range initial {
				r, ok, err := store.Get(ctx, input.Key, &actual[i])
				if err != nil {
					t.Errorf("unexpected error getting data: %v", err)
				}
				if !ok {
					t.Errorf("expected data to be found")
				}
				if r.Version != 1 {
					t.Errorf("expected version 1, got %d", r.Version)
				}
			}
			for i, a := range actual {
				if strings.HasSuffix(a.Value, "-updated") {
					t.Errorf("expected value for key %q not to be updated, got %s", initial[i], a.Value)
				}
			}
		})
	}
}
