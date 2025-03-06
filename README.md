# sqlitekv

A KV store on top of sqlite / rqlite.

## Usage

The `Store` interface has a sqlite and an rqlite implementation.

```go
func runSqlite(ctx context.Context) (error) {
  // Create a new in-memory SQLite database.
  pool, err := sqlitex.NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{})
  if err != nil {
    return fmt.Errorf("unexpected error creating SQLite pool: %w", err)
  }
  defer pool.Close()

  // Create a store.
  store := NewSqlite[Person](pool)

  // Run the example.
  return run(ctx, store)
}

func runRqlite(ctx context.Context) (error) {
  // Create a new Rqlite client.
  client := rqlitehttp.NewClient("http://localhost:4001", nil)

  // Create a store.
  store := NewRqlite[Person](client)

  // Run the example.
  return run(ctx, store)
}

func run(ctx context.Context, store sqlitekv.Store[Person]) (error) {
  // Initalize the store.
  if err := store.Init(ctx); err != nil {
    return fmt.Errorf("unexpected error initializing store: %w", err)
  }

  // Create a new person.
  alice := Person{
    Name:         "Alice",
    PhoneNumbers: []string{"123-456-7890"},
  }
  err := store.Put(ctx, "person/alice", -1, alice)
  if err != nil {
    return fmt.Errorf("unexpected error putting data: %w", err)
  }

  // Get the person we just added.
  p, ok, err := store.Get(ctx, "person/alice")
  if err != nil {
    return fmt.Errorf("unexpected error getting data: %w", err)
  }
  if !ok {
    return fmt.Errorf("expected data not found")
  }
  if p.Value.Name != alice.Name {
    return fmt.Errorf("expected name %q, got %q", alice.Name, p.Value.Name)
  }

  return nil
}
```

## Features

The `Store` interface has the following methods:

```go
type Store[T any] interface {
    // Init initializes the store. It should be called before any other method, and creates the necessary table.
    Init(ctx context.Context) error
    // Get gets a key from the store. If the key does not exist, it returns ok=false.
    Get(ctx context.Context, key string) (r Record[T], ok bool, err error)
    // GetPrefix gets all keys with a given prefix from the store.
    GetPrefix(ctx context.Context, prefix string) (records []Record[T], err error)
    // List gets all keys from the store, starting from the given offset and limiting the number of results to the given limit.
    List(ctx context.Context, start, limit int) (records []Record[T], err error)
    // Put a key into the store. If the key already exists, it will update the value if the version matches, and increment the version.
    //
    // If the key does not exist, it will insert the key with version 1.
    //
    // If the key exists but the version does not match, it will return an error.
    //
    // If the version is -1, it will skip the version check.
    Put(ctx context.Context, key string, version int64, value T) (err error)
    // Delete deletes a key from the store. If the key does not exist, no error is returned.
    Delete(ctx context.Context, key string) error
    // DeletePrefix deletes all keys with a given prefix from the store.
    DeletePrefix(ctx context.Context, prefix string) error
    // Count returns the number of keys in the store.
    Count(ctx context.Context) (count int64, err error)
    // Patch patches a key in the store. The patch is a JSON merge patch (RFC 7396), so would look something like map[string]any{"key": "value"}.
    Patch(ctx context.Context, key string, version int64, patch any) (err error)
}
```

## Tasks

### db-run

interactive: true

```bash
rqlited -auth=auth.json ~/sqlitekv.db
```

### db-shell

interactive: true

```bash
rqlite --user='admin:secret'
```

### build

```bash
go build ./...
```

### test

```bash
go test ./...
```

### develop

```bash
nix develop
```

### update-version

```bash
version set
```

### push-tag

Push a semantic version number.

```sh
version push
```

### docker-build-rqlite-aarch64

```bash
nix build .#packages.aarch64-linux.rqlite-docker-image
```

### docker-build-rqlite-x86_64

```bash
nix build .#packages.x86_64-linux.rqlite-docker-image
```

### crane-push-rqlite

env: CONTAINER_REGISTRY=ghcr.io/sqlitekv

```bash
nix build .#packages.x86_64-linux.rqlite-docker-image
cp ./result /tmp/rqlite.tar.gz
gunzip -f /tmp/rqlite.tar.gz
crane push /tmp/rqlite.tar ${CONTAINER_REGISTRY}/rqlite:v0.0.1
```

### docker-load-rqlite

Once you've built the image, you can load it into a local Docker daemon with `docker load`.

```bash
docker load < result
```

### docker-run-rqlite

```bash
docker run -v "$PWD/auth.json:/mnt/rqlite/auth.json" -v "$PWD/.rqlite:/mnt/data" -p 4001:4001 -p 4002:4002 -p 4003:4003 rqlite:latest
```
