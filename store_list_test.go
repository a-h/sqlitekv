package sqlitekv

import (
	"context"
	"strings"
	"testing"
)

func newListTest(ctx context.Context, store *Store) func(t *testing.T) {
	return func(t *testing.T) {
		defer store.DeletePrefix(ctx, "*", 0, -1)

		expected := []Person{
			{
				Name:         "Alice",
				PhoneNumbers: []string{"123-456-7890"},
			},
			{
				Name:         "Bob",
				PhoneNumbers: []string{"234-567-8901"},
			},
			{
				Name:         "Charlie",
				PhoneNumbers: []string{"345-678-9012"},
			},
			{
				Name:         "David",
				PhoneNumbers: []string{"456-789-0123"},
			},
			{
				Name:         "Eve",
				PhoneNumbers: []string{"567-890-1234"},
			},
			{
				Name:         "Frank",
				PhoneNumbers: []string{"678-901-2345"},
			},
		}

		for _, person := range expected {
			if err := store.Put(ctx, "list/"+strings.ToLower(person.Name), -1, person); err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
		}

		t.Run("Can get all", func(t *testing.T) {
			actual, err := store.List(ctx, 0, -1)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			actualValues, err := ValuesOf[Person](actual)
			if err != nil {
				t.Errorf("unexpected error getting values: %v", err)
			}
			if !personSliceIsEqual(expected, actualValues) {
				t.Errorf("expected %#v, got %#v", expected, actualValues)
			}
		})
		t.Run("Can limit the number of results", func(t *testing.T) {
			actual, err := store.List(ctx, 0, 2)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			actualValues, err := ValuesOf[Person](actual)
			if err != nil {
				t.Errorf("unexpected error getting values: %v", err)
			}
			if !personSliceIsEqual(expected[:2], actualValues) {
				t.Errorf("expected %#v, got %#v", expected[:2], actualValues)
			}
		})
		t.Run("Can offset the results", func(t *testing.T) {
			actual, err := store.List(ctx, 1, -1)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			actualValues, err := ValuesOf[Person](actual)
			if err != nil {
				t.Errorf("unexpected error getting values: %v", err)
			}
			if !personSliceIsEqual(expected[1:], actualValues) {
				t.Errorf("expected %#v, got %#v", expected[1:], actualValues)
			}
		})
	}
}
