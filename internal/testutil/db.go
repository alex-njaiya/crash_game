package testutil

import (
	"context"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func SetupTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:18-alpine",
		postgres.WithDatabase("test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		postgres.BasicWaitStrategies(),
	)

	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %v", err)
		}
	})

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")

	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	// run the migrations against the container
	m, err := migrate.New("file://../../migrations", connStr)

	if err != nil {
		t.Fatalf("failed to create migrations: %v", err)
	}

	if err := m.Up(); err != nil {
		t.Fatalf("failed to run migration: %v", err)
	}

	pool, err := pgxpool.New(ctx, connStr)

	if err != nil {
		t.Fatalf("failed to create a connection pool: %v", err)
	}

	t.Cleanup(pool.Close)

	return pool
}
