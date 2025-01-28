package postgres

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Client ...
type Client struct {
	DB *pgxpool.Pool
}

var pgxOnce sync.Once

// New ...
func New(ctx context.Context, connString string) (*Client, error) {
	var db *pgxpool.Pool

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	config.MaxConns = int32(10) // Set the maximum number of connections in the pool
	config.MinConns = int32(2)  // Set the minimum number of connections in the pool
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = time.Minute * 30
	pgxOnce.Do(func() {
		db, err = pgxpool.NewWithConfig(ctx, config)
	})

	return &Client{DB: db}, nil
}

// NewPGXDatabase creates a new PGXDatabase instance.
func NewPGXDatabase(ctx context.Context, connString string) (*PGXDatabase, error) {
	pool, err := New(ctx, connString)
	if err != nil {
		return nil, err
	}
	return &PGXDatabase{db: pool.DB}, nil
}

func (r *PGXDatabase) Close() {
	r.Close()
}
