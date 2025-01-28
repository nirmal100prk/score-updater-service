package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PGXDatabase struct {
	db *pgxpool.Pool
}

type PgxRepository interface {
	UpdateScore(ctx context.Context) error
}

func NewPgxRepository(PgServices PGXDatabase) PgxRepository {
	return &PGXDatabase{
		db: PgServices.db,
	}
}

func (r *PGXDatabase) UpdateScore(ctx context.Context) error {

	query := `INSERT INTO users ( name) VALUES ( $1)`
	res, err := r.db.Exec(ctx, query, "")
	if err != nil {
		return err
	}

	rowsAffected := res.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("query failed %v", "0 rows affected")
	}

	return nil
}
