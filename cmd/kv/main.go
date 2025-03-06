package main

import (
	"context"
	"fmt"
	"net/url"
	"os"

	"github.com/a-h/sqlitekv"
	"github.com/alecthomas/kong"
	rqlitehttp "github.com/rqlite/rqlite-go-http"
	"zombiezen.com/go/sqlite/sqlitex"
)

type GlobalFlags struct {
	Type       string `help:"The type of KV store to use." enum:"sqlite,rqlite" default:"sqlite"`
	Connection string `help:"The connection string to use." default:"file:data.db?mode=rwc"`
}

func (g GlobalFlags) Store() (sqlitekv.Store[any], error) {
	switch g.Type {
	case "sqlite":
		pool, err := sqlitex.NewPool(g.Connection, sqlitex.PoolOptions{})
		if err != nil {
			return nil, err
		}
		return sqlitekv.NewSqlite[any](pool), nil
	case "rqlite":
		u, err := url.Parse(g.Connection)
		if err != nil {
			return nil, err
		}
		user := u.Query().Get("user")
		password := u.Query().Get("password")
		// Remove user and password from the connection string.
		u.RawQuery = ""
		client := rqlitehttp.NewClient(u.String(), nil)
		if user != "" && password != "" {
			client.SetBasicAuth(user, password)
		}
		return sqlitekv.NewRqlite[any](client), nil
	default:
		return nil, fmt.Errorf("unknown store type %q", g.Type)
	}
}

type CLI struct {
	GlobalFlags

	Init         InitCommand         `cmd:"init" help:"Initialize the KV store."`
	Get          GetCommand          `cmd:"get" help:"Get a key from the KV store."`
	GetPrefix    GetPrefixCommand    `cmd:"get-prefix" help:"Get all keys with a given prefix from the KV store."`
	List         ListCommand         `cmd:"list" help:"List all keys in the KV store."`
	Put          PutCommand          `cmd:"put" help:"Put a key into the KV store."`
	Delete       DeleteCommand       `cmd:"delete" help:"Delete a key from the KV store."`
	DeletePrefix DeletePrefixCommand `cmd:"delete-prefix" help:"Delete all keys with a given prefix from the KV store."`
	Count        CountCommand        `cmd:"count" help:"Count the number of keys in the KV store"`
	Patch        PatchCommand        `cmd:"patch" help:"Patch a key in the KV store."`
}

func main() {
	var cli CLI
	ctx := context.Background()
	kctx := kong.Parse(&cli,
		kong.UsageOnError(),
		kong.BindTo(ctx, (*context.Context)(nil)),
		kong.BindTo(cli.GlobalFlags, (*GlobalFlags)(nil)),
	)
	if err := kctx.Run(ctx, cli.GlobalFlags); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
