package main

import (
	"context"
	"fmt"
)

type DeletePrefixCommand struct {
	Prefix string `arg:"" help:"The prefix to delete from the KV store." required:""`
}

func (c *DeletePrefixCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	return store.DeletePrefix(ctx, c.Prefix)
}
