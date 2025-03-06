package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

type ListCommand struct {
	Start int `help:"The index to start from." default:"0"`
	Limit int `help:"The maximum number of items to list." default:"100"`
}

func (c *ListCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	data, err := store.List(ctx, c.Start, c.Limit)
	if err != nil {
		return fmt.Errorf("failed to list data: %w", err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(data)
}
