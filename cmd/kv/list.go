package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

type ListCommand struct {
	Offset int `arg:"-o,--offset" help:"Range offset." default:"0"`
	Limit  int `arg:"-l,--limit" help:"The maximum number of records to return, or -1 for no limit." default:"1000"`
}

func (c *ListCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	data, err := store.List(ctx, c.Offset, c.Limit)
	if err != nil {
		return fmt.Errorf("failed to list data: %w", err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(data)
}
