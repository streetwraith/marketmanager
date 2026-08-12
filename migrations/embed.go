// Package migrations holds the SQL schema migrations, embedded so the binary is
// self-contained (no files to ship alongside the image).
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
