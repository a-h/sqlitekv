package sqlitekv

import (
	"context"
	"testing"
)

func newPutTest(ctx context.Context, store Store) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("Can put data", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			expected := Person{
				Name:         "Alice",
				PhoneNumbers: []string{"123-456-7890"},
			}
			err := store.Put(ctx, "put", -1, expected)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
			var p Person
			_, ok, err := store.Get(ctx, "put", &p)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Error("expected data not found")
			}
			if !expected.Equals(p) {
				t.Errorf("expected %#v, got %#v", expected, p)
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
			err = store.Put(ctx, "put", -1, expected)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
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
		t.Run("Can not insert a record if one already exists and version is set to 0", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			expected := Person{Name: "Alice"}
			err := store.Put(ctx, "put", -1, expected)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
			err = store.Put(ctx, "put", 0, expected)
			if err == nil {
				t.Error("expected error putting data: got nil")
			}
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
			err = store.Put(ctx, "put", 1, expected)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
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
		t.Run("Can use optimistic concurrency to ensure version being updated has not been changed", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			expected := Person{Name: "Alice"}
			err := store.Put(ctx, "put", -1, expected)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
			expected.PhoneNumbers = []string{"234-567-8901"}
			err = store.Put(ctx, "put", 3, expected)
			if err == nil {
				t.Error("expected error putting data: got nil")
			}
		})
	}
}
