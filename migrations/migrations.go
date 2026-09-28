// Package migrations owns Panta's ordered, append-only SQL migration history.
package migrations

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
)

//go:embed *.sql
var files embed.FS

var filenamePattern = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

type Migration struct {
	Version  int64
	Name     string
	Filename string
	Checksum string
	SQL      string
}

// All returns the embedded migrations in deterministic version order.
func All() ([]Migration, error) {
	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		return nil, fmt.Errorf("list embedded migrations: %w", err)
	}

	migrations := make([]Migration, 0, len(names))
	seenVersions := make(map[int64]string, len(names))
	for _, filename := range names {
		matches := filenamePattern.FindStringSubmatch(filename)
		if matches == nil {
			return nil, fmt.Errorf("invalid migration filename %q", filename)
		}
		version, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse migration version %q: %w", filename, err)
		}
		if previous, exists := seenVersions[version]; exists {
			return nil, fmt.Errorf("duplicate migration version %d in %q and %q", version, previous, filename)
		}
		seenVersions[version] = filename

		contents, err := files.ReadFile(filename)
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", filename, err)
		}
		digest := sha256.Sum256(contents)
		migrations = append(migrations, Migration{
			Version:  version,
			Name:     matches[2],
			Filename: filename,
			Checksum: hex.EncodeToString(digest[:]),
			SQL:      string(contents),
		})
	}

	sort.Slice(migrations, func(i, j int) bool { return migrations[i].Version < migrations[j].Version })
	return migrations, nil
}
