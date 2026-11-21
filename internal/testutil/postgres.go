// Package testutil starts throwaway infrastructure for integration tests.
package testutil

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // migrate driver
	_ "github.com/golang-migrate/migrate/v4/source/file"     // migrate source
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	pgOnce sync.Once
	pgDSN  string
	pgErr  error
)

// Postgres returns a pool connected to a migrated PostgreSQL container that
// is shared by every test in the process. Tables are truncated before the
// pool is returned, so tests using it must not run in parallel.
// Skipped with -short.
func Postgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: needs Docker")
	}
	pgOnce.Do(func() { pgDSN, pgErr = startPostgres() })
	if pgErr != nil {
		t.Fatalf("start postgres: %v", pgErr)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, "TRUNCATE delivery_attempts, notifications, batches, templates"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return pool
}

// PostgresDSN returns the DSN of the shared container (call Postgres first).
func PostgresDSN() string { return pgDSN }

func startPostgres() (string, error) {
	ctx := context.Background()
	// The container is removed by testcontainers' reaper when the test binary exits.
	c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("notify"),
		tcpostgres.WithUsername("notify"),
		tcpostgres.WithPassword("notify"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		return "", err
	}
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return "", err
	}

	m, err := migrate.New("file://"+migrationsDir(), "pgx5"+dsn[len("postgres"):])
	if err != nil {
		return "", fmt.Errorf("migrate init: %w", err)
	}
	defer func() { _, _ = m.Close() }()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return "", fmt.Errorf("migrate up: %w", err)
	}
	return dsn, nil
}

func migrationsDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "migrations")
}
