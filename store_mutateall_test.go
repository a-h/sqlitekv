package sqlitekv

import (
	"context"
	"strings"
	"testing"
)

type mutateAllTestData struct {
	Value string `json:"value"`
}

func newMutateAllTest(ctx context.Context, store Store) func(t *testing.T) {
	return func(t *testing.T) {
		deletionTests := []struct {
			name                 string
			operations           []MutateAllInput
			expectedRowsAffected int
			expectedRemaining    int
		}{
			{
				name: "Can delete individual keys",
				operations: []MutateAllInput{
					Delete("mutateall-1"), Delete("mutateall-2"),
				},
				expectedRowsAffected: 2,
				expectedRemaining:    0,
			},
			{
				name: "Can delete multiple keys",
				operations: []MutateAllInput{
					DeleteKeys("mutateall-1", "mutateall-2"),
				},
				expectedRowsAffected: 2,
				expectedRemaining:    0,
			},
			{
				name: "Can delete prefixes",
				operations: []MutateAllInput{
					DeletePrefix("mutate", 0, 1),
				},
				expectedRowsAffected: 1,
				expectedRemaining:    1,
			},
			{
				name: "Can delete ranges",
				operations: []MutateAllInput{
					DeleteRange("mutateall-1", "mutateall-2", 0, 100),
				},
				expectedRowsAffected: 1,
				expectedRemaining:    1,
			},
			{
				name: "Can patch alongside",
				operations: []MutateAllInput{
					Delete("mutateall-1"), Delete("mutateall-2"),
					Patch("patch-1", -1, map[string]any{"key": "value"}),
				},
				expectedRowsAffected: 3,
				expectedRemaining:    1,
			},
		}
		for _, test := range deletionTests {
			t.Run(test.name, func(t *testing.T) {
				defer store.DeletePrefix(ctx, "*", 0, -1)

				initial := []MutateAllInput{
					Put("mutateall-1", -1, mutateAllTestData{Value: "value-1"}),
					Put("mutateall-2", -1, mutateAllTestData{Value: "value-2"}),
				}
				rowsAffected, err := store.MutateAll(ctx, initial...)
				if err != nil {
					t.Fatalf("unexpected error putting data: %v", err)
				}
				if rowsAffected != 2 {
					t.Errorf("expected 2 inserts, got %d", rowsAffected)
				}

				rowsAffected, err = store.MutateAll(ctx, test.operations...)
				if err != nil {
					t.Errorf("failed to delete records: %v", err)
				}
				if rowsAffected != int64(test.expectedRowsAffected) {
					t.Errorf("expected %d deletes, got %d", test.expectedRowsAffected, rowsAffected)
				}

				count, err := store.Count(ctx)
				if err != nil {
					t.Fatalf("unexpected error getting count: %v", err)
				}
				if count != int64(test.expectedRemaining) {
					t.Errorf("expected %d items to remain, but was %d", test.expectedRemaining, count)
				}
			})
		}
		t.Run("Cannot put/patch same key twice", func(t *testing.T) {
			_, err := store.MutateAll(ctx, Put("a", -1, ""), Put("a", -1, ""))
			if err == nil {
				t.Error("expected error, but got nil")
			}
		})
		t.Run("Patch and Put are transactional", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)
			// Then, attempt to update both of them.
			// One with an incorrect version number, the other with the correct version number.
			// The transaction should fail, and neither key should be updated.
			initial := []MutateAllInput{
				Put("mutateall-1", -1, mutateAllTestData{Value: "value-1"}),
				Put("mutateall-2", -1, mutateAllTestData{Value: "value-2"}),
			}
			rowsAffected, err := store.MutateAll(ctx, initial...)
			if err != nil {
				t.Fatalf("unexpected error putting data: %v", err)
			}
			if rowsAffected != 2 {
				t.Errorf("expected 2 inserts, got %d", rowsAffected)
			}

			// Updates.
			updates := []MutateAllInput{
				// Correct version, update should succeed.
				Put("mutateall-1", 1, mutateAllTestData{Value: "value-1-updated"}),
				// Don't care about version, update should succeed.
				Put("mutateall-2", -1, mutateAllTestData{Value: "value-2-updated"}),
				// Incorrect version, update should fail.
				Put("mutateall-3", 2, mutateAllTestData{Value: "value-3-updated"}),
				// Key does not exist, insert should succeed.
				Put("mutateall-4", 0, mutateAllTestData{Value: "value-4"}),
			}
			_, err = store.MutateAll(ctx, updates...)
			if err == nil {
				t.Errorf("expected error, because one of the updates should fail, but got nil")
			}

			// Check that the count of the prefix is still 3.
			count, err := store.CountPrefix(ctx, "mutateall")
			if err != nil {
				t.Fatalf("unexpected error getting count: %v", err)
			}
			if count != 2 {
				t.Errorf("expected count 2, got %d", count)
			}

			// Check that the values were not updated.
			actual := make([]mutateAllTestData, len(initial))
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
