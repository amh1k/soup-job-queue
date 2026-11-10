package database

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Open creates a lazy pool. Readiness, rather than liveness, checks connectivity.
func Open(ctx context.Context, url string, maxConns int32, timeout time.Duration) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, errors.New("invalid DATABASE_URL")
	} // Do not log connection credentials.
	cfg.MaxConns = maxConns
	cfg.ConnConfig.ConnectTimeout = timeout
	return pgxpool.NewWithConfig(ctx, cfg)
}
