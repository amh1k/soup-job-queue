package migrations

import (
	"context"
	"crypto/sha256"
	"embed"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.sql
var files embed.FS

type migration struct{ name, sql, checksum string }

func bundled() ([]migration, error) {
	names, err := fs.Glob(files, "*.sql") // Glob returns names in lexical order.
	if err != nil {
		return nil, err
	}
	var result []migration
	for _, name := range names {
		data, err := files.ReadFile(name)
		if err != nil {
			return nil, err
		}
		result = append(result, migration{name, string(data), fmt.Sprintf("%x", sha256.Sum256(data))})
	}
	return result, nil
}

// Apply serializes migration commands and commits schema plus history atomically.
// Applied files are immutable: edit by adding a new migration, never changing one.
func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	all, err := bundled()
	if err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(83174001)"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version text PRIMARY KEY, checksum text NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
	)`); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, "SELECT version, checksum FROM schema_migrations")
	if err != nil {
		return err
	}
	applied := make(map[string]string)
	for rows.Next() {
		var name, checksum string
		if err := rows.Scan(&name, &checksum); err != nil {
			rows.Close()
			return err
		}
		applied[name] = checksum
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, m := range all {
		if checksum, ok := applied[m.name]; ok {
			if checksum != m.checksum {
				return fmt.Errorf("migration %s checksum mismatch", m.name)
			}
			delete(applied, m.name)
			continue
		}
		if _, err := tx.Exec(ctx, m.sql); err != nil {
			return fmt.Errorf("apply %s: %w", m.name, err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version, checksum) VALUES ($1,$2)", m.name, m.checksum); err != nil {
			return err
		}
	}
	if len(applied) != 0 {
		return fmt.Errorf("database has migrations unknown to this binary")
	}
	return tx.Commit(ctx)
}

// Check verifies database connectivity and the complete expected migration set.
// A failed query, missing migration, or incompatible binary is not ready.
func Check(ctx context.Context, pool *pgxpool.Pool) error {
	all, err := bundled()
	if err != nil {
		return err
	}
	rows, err := pool.Query(ctx, "SELECT version, checksum FROM schema_migrations")
	if err != nil {
		return err
	}
	defer rows.Close()
	applied := make(map[string]string)
	for rows.Next() {
		var name, checksum string
		if err := rows.Scan(&name, &checksum); err != nil {
			return err
		}
		applied[name] = checksum
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(applied) != len(all) {
		return fmt.Errorf("schema migration count mismatch")
	}
	for _, m := range all {
		if applied[m.name] != m.checksum {
			return fmt.Errorf("migration %s is missing or changed", m.name)
		}
	}
	return nil
}
