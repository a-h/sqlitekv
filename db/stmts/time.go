package stmts

import (
	"time"

	"github.com/a-h/sqlitekv/db"
)

func now() string {
	if !db.TestTime.IsZero() {
		return db.TestTime.UTC().Format(time.RFC3339Nano)
	}
	return time.Now().UTC().Format(time.RFC3339Nano)
}
