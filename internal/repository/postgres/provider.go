package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgxRepository defines database operations.
type PgxRepository interface {
	DB() *pgxpool.Pool
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) pgx.Row
	Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	Begin(ctx context.Context) (pgx.Tx, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	Close()
}

type DatabaseProvider struct {
	Client *Client
}

func NewDatabaseProvider(client *Client) PgxRepository {
	return &DatabaseProvider{Client: client}
}

func (s *DatabaseProvider) DB() *pgxpool.Pool {
	return s.Client.DB
}

func (s *DatabaseProvider) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	return s.Client.DB.Query(ctx, query, args...)
}

func (s *DatabaseProvider) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	return s.Client.DB.QueryRow(ctx, query, args...)
}

func (s *DatabaseProvider) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	return s.Client.DB.Exec(ctx, query, args...)
}

func (s *DatabaseProvider) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return s.Client.DB.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (s *DatabaseProvider) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return s.Client.DB.SendBatch(ctx, b)
}

func (s *DatabaseProvider) Begin(ctx context.Context) (pgx.Tx, error) {
	return s.Client.DB.Begin(ctx)
}

func (s *DatabaseProvider) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	return s.Client.DB.BeginTx(ctx, txOptions)
}

// Close ensures proper resource cleanup.
func (s *DatabaseProvider) Close() {
	s.Client.Close()
}
