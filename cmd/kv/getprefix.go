package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

type GetPrefixCommand struct {
	Prefix string `arg:"" help:"The prefix to search for in the KV store." required:""`
}

func (c *GetPrefixCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	data, err := store.GetPrefix(ctx, c.Prefix)
	if err != nil {
		return fmt.Errorf("failed to get data: %w", err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(data)
}
