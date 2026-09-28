package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nathanxiangang-web/panta/migrations"
)

var ErrIncompatibleSchema = errors.New("incompatible Panta database schema")

const migrationLockID int64 = 0x50414e5441

type MigrationStatus struct {
	Version  int64  `json:"version"`
	Name     string `json:"name"`
	Checksum string `json:"checksum"`
}

type AppliedMigration struct {
	MigrationStatus
	AppliedAt time.Time `json:"applied_at"`
}

type SchemaStatus struct {
	Compatible     bool               `json:"compatible"`
	CurrentVersion int64              `json:"current_version"`
	LatestVersion  int64              `json:"latest_version"`
	Applied        []AppliedMigration `json:"applied"`
	Pending        []MigrationStatus  `json:"pending"`
	Unknown        []AppliedMigration `json:"unknown"`
	Modified       []AppliedMigration `json:"modified"`
}

type Migrator struct {
	pool       *pgxpool.Pool
	migrations []migrations.Migration
}

func NewMigrator(pool *pgxpool.Pool) (*Migrator, error) {
	if pool == nil {
		return nil, errors.New("database pool is required")
	}
	all, err := migrations.All()
	if err != nil {
		return nil, err
	}
	return &Migrator{pool: pool, migrations: all}, nil
}

func (m *Migrator) Status(ctx context.Context) (SchemaStatus, error) {
	return m.status(ctx, m.pool)
}

func (m *Migrator) Apply(ctx context.Context) (status SchemaStatus, err error) {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return SchemaStatus{}, fmt.Errorf("begin migration transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrationLockID); err != nil {
		return SchemaStatus{}, fmt.Errorf("acquire migration lock: %w", err)
	}
	if _, err = tx.Exec(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version bigint PRIMARY KEY,
    name text NOT NULL,
    checksum text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
)`); err != nil {
		return SchemaStatus{}, fmt.Errorf("ensure migration history: %w", err)
	}

	status, err = m.status(ctx, tx)
	if err != nil {
		return SchemaStatus{}, err
	}
	if len(status.Unknown) > 0 || len(status.Modified) > 0 {
		return status, ErrIncompatibleSchema
	}

	byVersion := make(map[int64]migrations.Migration, len(m.migrations))
	for _, migration := range m.migrations {
		byVersion[migration.Version] = migration
	}
	for _, pending := range status.Pending {
		migration := byVersion[pending.Version]
		if _, err = tx.Conn().PgConn().Exec(ctx, migration.SQL).ReadAll(); err != nil {
			return SchemaStatus{}, fmt.Errorf("apply migration %s: %w", migration.Filename, err)
		}
		if _, err = tx.Exec(ctx,
			"INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)",
			migration.Version, migration.Name, migration.Checksum,
		); err != nil {
			return SchemaStatus{}, fmt.Errorf("record migration %s: %w", migration.Filename, err)
		}
	}

	status, err = m.status(ctx, tx)
	if err != nil {
		return SchemaStatus{}, err
	}
	if !status.Compatible {
		return status, ErrIncompatibleSchema
	}
	if err = tx.Commit(ctx); err != nil {
		return SchemaStatus{}, fmt.Errorf("commit migrations: %w", err)
	}
	return status, nil
}

type migrationQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func (m *Migrator) status(ctx context.Context, queryer migrationQueryer) (SchemaStatus, error) {
	var historyExists bool
	if err := queryer.QueryRow(ctx, "SELECT to_regclass('public.schema_migrations') IS NOT NULL").Scan(&historyExists); err != nil {
		return SchemaStatus{}, fmt.Errorf("detect migration history: %w", err)
	}

	applied := make([]AppliedMigration, 0)
	if historyExists {
		rows, err := queryer.Query(ctx, "SELECT version, name, checksum, applied_at FROM schema_migrations ORDER BY version")
		if err != nil {
			return SchemaStatus{}, fmt.Errorf("read migration history: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var migration AppliedMigration
			if err := rows.Scan(&migration.Version, &migration.Name, &migration.Checksum, &migration.AppliedAt); err != nil {
				return SchemaStatus{}, fmt.Errorf("scan migration history: %w", err)
			}
			applied = append(applied, migration)
		}
		if err := rows.Err(); err != nil {
			return SchemaStatus{}, fmt.Errorf("iterate migration history: %w", err)
		}
	}

	return compareMigrations(m.migrations, applied), nil
}

func compareMigrations(known []migrations.Migration, applied []AppliedMigration) SchemaStatus {
	status := SchemaStatus{
		Applied:  make([]AppliedMigration, 0, len(applied)),
		Pending:  make([]MigrationStatus, 0),
		Unknown:  make([]AppliedMigration, 0),
		Modified: make([]AppliedMigration, 0),
	}
	knownByVersion := make(map[int64]migrations.Migration, len(known))
	for _, migration := range known {
		knownByVersion[migration.Version] = migration
		if migration.Version > status.LatestVersion {
			status.LatestVersion = migration.Version
		}
	}

	appliedVersions := make(map[int64]bool, len(applied))
	for _, migration := range applied {
		status.Applied = append(status.Applied, migration)
		appliedVersions[migration.Version] = true
		if migration.Version > status.CurrentVersion {
			status.CurrentVersion = migration.Version
		}
		expected, exists := knownByVersion[migration.Version]
		if !exists {
			status.Unknown = append(status.Unknown, migration)
			continue
		}
		if migration.Name != expected.Name || migration.Checksum != expected.Checksum {
			status.Modified = append(status.Modified, migration)
		}
	}

	for _, migration := range known {
		if !appliedVersions[migration.Version] {
			status.Pending = append(status.Pending, MigrationStatus{
				Version: migration.Version, Name: migration.Name, Checksum: migration.Checksum,
			})
		}
	}
	status.Compatible = len(status.Pending) == 0 && len(status.Unknown) == 0 && len(status.Modified) == 0
	return status
}
