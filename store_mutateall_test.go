package sqlitekv

import (
	"context"
	"strings"
	"testing"

	"github.com/a-h/sqlitekv/db"
)

type mutateAllTestData struct {
	Value string `json:"value"`
}

func newMutateAllTest(ctx context.Context, store Store) func(t *testing.T) {
	stmts := store.db.Statements()
	return func(t *testing.T) {
		tests := []struct {
			name                  string
			operations            []db.Mutation
			expectedRowsAffected  []int64
			expectedRemainingKeys []string
		}{
			{
				name: "Can delete individual keys",
				operations: []db.Mutation{
					stmts.Delete("mutateall-1"), stmts.Delete("mutateall-2"),
				},
				expectedRowsAffected:  []int64{1, 1},
				expectedRemainingKeys: nil,
			},
			{
				name: "Can delete multiple keys",
				operations: []db.Mutation{
					stmts.DeleteKeys("mutateall-1", "mutateall-2"),
				},
				expectedRowsAffected:  []int64{2},
				expectedRemainingKeys: nil,
			},
			{
				name: "Can delete prefixes",
				operations: []db.Mutation{
					stmts.DeletePrefix("mutate", 0, 1),
				},
				expectedRowsAffected:  []int64{1},
				expectedRemainingKeys: []string{"mutateall-2"},
			},
			{
				name: "Can delete ranges",
				operations: []db.Mutation{
					stmts.DeleteRange("mutateall-1", "mutateall-2", 0, 100),
				},
				expectedRowsAffected:  []int64{1},
				expectedRemainingKeys: []string{"mutateall-2"},
			},
			{
				name: "Can patch alongside",
				operations: []db.Mutation{
					stmts.Delete("mutateall-1"), stmts.Delete("mutateall-2"),
					stmts.Patch("patch-1", -1, map[string]any{"key": "value"}),
				},
				expectedRowsAffected:  []int64{1, 1, 1},
				expectedRemainingKeys: []string{"patch-1"},
			},
			{
				name: "Can put and patch in a single transaction",
				operations: []db.Mutation{
					stmts.PutPatches(
						db.PutInput("put1", -1, nil),
						db.PatchInput("patch1", -1, nil),
					),
				},
				expectedRowsAffected: []int64{2},
				expectedRemainingKeys: []string{
					"mutateall-1", "mutateall-2",
					"patch1", "put1",
				},
			},
			{
				name: "Can put and patch in a single transaction",
				operations: []db.Mutation{
					stmts.PutPatches(
						db.PutInput("put1", -1, nil),
						db.PatchInput("patch1", -1, nil),
					),
				},
				expectedRowsAffected: []int64{2},
				expectedRemainingKeys: []string{
					"mutateall-1", "mutateall-2",
					"patch1", "put1",
				},
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				defer store.DeletePrefix(ctx, "*", 0, -1)

				initial := []db.Mutation{
					stmts.Put("mutateall-1", -1, mutateAllTestData{Value: "value-1"}),
					stmts.Put("mutateall-2", -1, mutateAllTestData{Value: "value-2"}),
				}
				rowsAffected, err := store.MutateAll(ctx, initial...)
				if err != nil {
					t.Fatalf("unexpected error putting data: %v", err)
				}
				expectRowsAffectedEqual(t, []int64{1, 1}, rowsAffected)

				rowsAffected, err = store.MutateAll(ctx, test.operations...)
				if err != nil {
					t.Errorf("failed to mutate records: %v", err)
				}
				expectRowsAffectedEqual(t, test.expectedRowsAffected, rowsAffected)

				list, err := store.List(ctx, -1, -1)
				if err != nil {
					t.Fatalf("unexpected error getting count: %v", err)
				}
				actualKeys := make([]string, len(list))
				for i, r := range list {
					actualKeys[i] = r.Key
				}
				if len(test.expectedRemainingKeys) != len(actualKeys) {
					t.Fatalf("expected keys %#v, got keys %#v", test.expectedRemainingKeys, actualKeys)
				}
				for i, expectedKey := range test.expectedRemainingKeys {
					if expectedKey != actualKeys[i] {
						t.Errorf("index %d: expected key %q, got %q", i, expectedKey, actualKeys[i])
					}
				}
			})
		}
	}
}

func newPutPatchesTest(ctx context.Context, store Store) func(t *testing.T) {
	stmts := store.db.Statements()
	return func(t *testing.T) {
		t.Run("Can put and patch data", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			expected := []Person{
				{
					Name:         "Alice",
					PhoneNumbers: []string{"123-456-7890"},
				},
				{
					Name:         "Bob",
					PhoneNumbers: []string{"123-456-7890"},
				},
				{
					Name:         "Charlie",
					PhoneNumbers: []string{"123-456-7890"},
				},
			}
			rowsAffected, err := store.MutateAll(ctx, stmts.PutPatches(
				db.PutInput(expected[0].Name, -1, expected[0]),
				db.PutInput(expected[1].Name, -1, expected[1]),
				db.PatchInput(expected[2].Name, -1, expected[2]),
			))
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
			expectRowsAffectedEqual(t, []int64{3}, rowsAffected)

			records, err := store.List(ctx, 0, 100)
			if err != nil {
				t.Fatalf("failed to list rows: %v", err)
			}
			actual, err := ValuesOf[Person](records)
			if err != nil {
				t.Fatalf("failed to convert records to values: %v", err)
			}
			if !personSliceIsEqual(expected, actual) {
				t.Errorf("expected %#v, got %#v", expected, actual)
			}
		})
		t.Run("Can overwrite existing data if version is set to -1", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			expected := Person{
				Name:         "Alice",
				PhoneNumbers: []string{"123-456-7890"},
			}
			err := store.Put(ctx, "put", -1, expected)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
			expected.PhoneNumbers = []string{"234-567-8901"}

			rowsAffected, err := store.MutateAll(ctx, stmts.PutPatches(db.PutInput("put", -1, expected)))
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
			expectRowsAffectedEqual(t, []int64{1}, rowsAffected)

			var overwritten Person
			_, ok, err := store.Get(ctx, "put", &overwritten)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Error("expected data not found")
			}
			if !expected.Equals(overwritten) {
				t.Errorf("expected %#v, got %#v", expected, overwritten)
			}
		})
		t.Run("Can patch existing data if version is set to -1", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			expected := Person{
				Name:         "Alice",
				PhoneNumbers: []string{"123-456-7890"},
			}
			err := store.Put(ctx, "patch", -1, expected)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
			expected.PhoneNumbers = []string{"234-567-8901"}

			rowsAffected, err := store.MutateAll(ctx, stmts.PutPatches(db.PatchInput("patch", -1, map[string]any{"phone_numbers": expected.PhoneNumbers})))
			if err != nil {
				t.Errorf("unexpected error patching data: %v", err)
			}
			expectRowsAffectedEqual(t, []int64{1}, rowsAffected)

			var overwritten Person
			_, ok, err := store.Get(ctx, "patch", &overwritten)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Error("expected data not found")
			}
			if !expected.Equals(overwritten) {
				t.Errorf("expected %#v, got %#v", expected, overwritten)
			}
		})
		t.Run("Can not insert a record if one already exists and version is set to 0", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			expected := Person{Name: "Alice"}
			err := store.Put(ctx, "put", -1, expected)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}

			rowsAffected, err := store.MutateAll(ctx, stmts.PutPatches(db.PutInput("put", 0, expected)))
			if err == nil {
				t.Error("expected error putting data: got nil")
			}
			expectRowsAffectedEqual(t, []int64{0}, rowsAffected)
		})
		t.Run("Can overwrite existing data with specified version", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			expected := Person{
				Name:         "Alice",
				PhoneNumbers: []string{"123-456-7890"},
			}
			err := store.Put(ctx, "put", -1, expected)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
			expected.PhoneNumbers = []string{"234-567-8901"}
			rowsAffected, err := store.MutateAll(ctx, stmts.PutPatches(db.PutInput("put", 1, expected)))
			if err != nil {
				t.Errorf("unexpected error overwriting data: %v", err)
			}
			expectRowsAffectedEqual(t, []int64{1}, rowsAffected)

			var actual Person
			r, ok, err := store.Get(ctx, "put", &actual)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Error("expected data not found")
			}
			if !expected.Equals(actual) {
				t.Errorf("expected %#v, got %#v", expected, actual)
			}
			if r.Version != 2 {
				t.Errorf("expected version 2, got %d", r.Version)
			}
		})
		t.Run("The created field is set and not updated", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			expected := []Person{
				{
					Name:         "Alice",
					PhoneNumbers: []string{"123-456-7890"},
				},
				{
					Name:         "Bob",
					PhoneNumbers: []string{"123-456-7890"},
				},
				{
					Name:         "Charlie",
					PhoneNumbers: []string{"123-456-7890"},
				},
			}

			// Put the data once.
			rowsAffected, err := store.MutateAll(ctx, stmts.PutPatches(
				db.PutInput(expected[0].Name, -1, expected[0]),
				db.PutInput(expected[1].Name, -1, expected[1]),
				db.PatchInput(expected[2].Name, -1, expected[2]),
			))
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
			expectRowsAffectedEqual(t, []int64{3}, rowsAffected)

			records, err := store.List(ctx, 0, 100)
			if err != nil {
				t.Fatalf("failed to list rows: %v", err)
			}

			// Now update.
			expected[0].PhoneNumbers = nil
			expected[1].PhoneNumbers = nil
			expected[2].PhoneNumbers = nil
			rowsAffected, err = store.MutateAll(ctx, stmts.PutPatches(
				db.PutInput(expected[0].Name, -1, expected[0]),
				db.PatchInput(expected[1].Name, -1, expected[1]),
				db.PatchInput(expected[2].Name, -1, expected[2]),
			))

			// Ensure that the created dates haven't changed.
			updated, err := store.List(ctx, 0, 100)
			if err != nil {
				t.Fatalf("failed to list updated rows: %v", err)
			}
			if len(records) != len(updated) {
				t.Fatalf("expected %d updated records, got %d", len(records), len(updated))
			}
			for i, r := range records {
				u := updated[i]
				if r.Created.IsZero() {
					t.Errorf("expected a non-zero creation date, but got zero")
				}
				if !r.Created.Equal(u.Created) {
					t.Errorf("key %q expected created date to not be updated from %v, but got %v", r.Key, r.Created, u.Created)
				}
			}
		})
		t.Run("PutPatches is transactional", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			keys := []string{"mutateall-1", "mutateall-2"}
			values := []mutateAllTestData{
				{Value: "value-1"},
				{Value: "value-2"},
			}
			initial := []db.Mutation{
				stmts.PutPatches(
					db.PutInput(keys[0], -1, values[0]),
					db.PutInput(keys[1], -1, values[1]),
				),
			}
			rowsAffected, err := store.MutateAll(ctx, initial...)
			if err != nil {
				t.Fatalf("unexpected error putting data: %v", err)
			}
			expectRowsAffectedEqual(t, []int64{2}, rowsAffected)

			// Updates.
			updates := stmts.PutPatches(
				// Correct version, update should succeed.
				db.PutInput("mutateall-1", 1, mutateAllTestData{Value: "value-1-updated"}),
				// Don't care about version, update should succeed.
				db.PutInput("mutateall-2", -1, mutateAllTestData{Value: "value-2-updated"}),
				// Incorrect version, update should fail.
				db.PutInput("mutateall-3", 2, mutateAllTestData{Value: "value-3-updated"}),
				// Key does not exist, insert should succeed.
				db.PutInput("mutateall-4", 0, mutateAllTestData{Value: "value-4"}),
			)
			_, err = store.MutateAll(ctx, updates)
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
			actual := make([]mutateAllTestData, len(keys))
			for i, key := range keys {
				r, ok, err := store.Get(ctx, key, &actual[i])
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
					t.Errorf("expected value for key %q not to be updated, got %s", keys[i], a.Value)
				}
			}
		})
	}
}
