# sqlitekv

A KV store on top of sqlite / rqlite.

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
