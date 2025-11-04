// Package pgxmig supports running embedded pgx migrations.
package pgxmig

import (
	"context"
	"fmt"
	"io/fs"
	"slices"

	"github.com/jackc/pgx/v5"
)

// Source defines a FS and correlated Glob to provide a source of migrations.
type Source struct {
	FS   fs.FS
	Glob string
}

// DB must be satisfied for executing migrations.
type DB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Migrate runs the migrations on the target DB.
func (s Source) Migrate(ctx context.Context, db DB) error {
	files, err := fs.Glob(s.FS, s.Glob)
	if err != nil {
		return fmt.Errorf("pgxmig: error globbing: %q: %w", s.Glob, err)
	}
	slices.Sort(files)
	const migrationSchemaSQL = `
	create table if not exists db_migrations (
		name text primary key
	)`
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, migrationSchemaSQL)
		return err
	})
	if err != nil {
		return fmt.Errorf("pgxmig: error creating db_migrations table: %w", err)
	}
	for _, filename := range files {
		data, err := fs.ReadFile(s.FS, filename)
		if err != nil {
			return fmt.Errorf("pgxmig: error reading migration: %q: %w", filename, err)
		}
		err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
			const alreadyDoneSQL = `select count(*) from db_migrations where name = $1`
			var alreadyDone int
			if err := tx.QueryRow(ctx, alreadyDoneSQL, filename).Scan(&alreadyDone); err != nil {
				return fmt.Errorf("pgxmig: error checking migration status: %q: %w", filename, err)
			}
			if alreadyDone == 1 {
				return nil
			}
			if _, err := tx.Exec(ctx, `insert into db_migrations values ($1)`, filename); err != nil {
				return fmt.Errorf("pgxmig: error updating migration status: %q: %w", filename, err)
			}
			if _, err := tx.Exec(ctx, string(data)); err != nil {
				return fmt.Errorf("pgxmig: error executing migration: %q: %w", filename, err)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("pgxmig: error commiting migration: %q: %w", filename, err)
		}
	}
	return nil
}
