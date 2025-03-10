package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/a-h/sqlitekv"
)

type PutAllCommand struct {
}

func (c *PutAllCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	var data []sqlitekv.PutInput
	err = json.NewDecoder(os.Stdin).Decode(&data)
	if err != nil {
		return fmt.Errorf("failed to decode data: %w", err)
	}

	return store.PutAll(ctx, data...)
}
