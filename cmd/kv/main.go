package main

import (
	"context"
	"fmt"
	"net/url"
	"os"

	"github.com/a-h/sqlitekv"
	"github.com/a-h/sqlitekv/db"
	"github.com/alecthomas/kong"
	rqlitehttp "github.com/rqlite/rqlite-go-http"
	"zombiezen.com/go/sqlite/sqlitex"
)

type GlobalFlags struct {
	Type       string `help:"The type of KV store to use." enum:"sqlite,rqlite" default:"sqlite"`
	Connection string `help:"The connection string to use." default:"file:data.db?mode=rwc"`
}

func (g GlobalFlags) Store() (sqlitekv.Store, error) {
	db, err := g.DB()
	if err != nil {
		return sqlitekv.Store{}, err
	}
	return sqlitekv.NewStore(db), nil
}

func (g GlobalFlags) DB() (db.DB, error) {
	switch g.Type {
	case "sqlite":
		pool, err := sqlitex.NewPool(g.Connection, sqlitex.PoolOptions{})
		if err != nil {
			return nil, err
		}
		return sqlitekv.NewSqlite(pool), nil
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
		return sqlitekv.NewRqlite(client), nil
	default:
		return nil, fmt.Errorf("unknown store type %q", g.Type)
	}
}

type CLI struct {
	GlobalFlags

	Init         InitCommand         `cmd:"init" help:"Initialize the store."`
	Get          GetCommand          `cmd:"get" help:"Get a key."`
	GetPrefix    GetPrefixCommand    `cmd:"get-prefix" help:"Get all keys with a given prefix."`
	GetRange     GetRangeCommand     `cmd:"get-range" help:"Get a range of keys."`
	List         ListCommand         `cmd:"list" help:"List all keys."`
	Put          PutCommand          `cmd:"put" help:"Put a key."`
	Delete       DeleteCommand       `cmd:"delete" help:"Delete a key."`
	DeletePrefix DeletePrefixCommand `cmd:"delete-prefix" help:"Delete all keys with a given prefix."`
	DeleteRange  DeleteRangeCommand  `cmd:"delete-range" help:"Delete a range of keys."`
	Count        CountCommand        `cmd:"count" help:"Count the number of keys."`
	CountPrefix  CountPrefixCommand  `cmd:"count-prefix" help:"Count the number of keys with a given prefix."`
	CountRange   CountRangeCommand   `cmd:"count-range" help:"Count the number of keys in a range."`
	Patch        PatchCommand        `cmd:"patch" help:"Patch a key."`
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
