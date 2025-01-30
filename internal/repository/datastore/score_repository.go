package datastore

import (
	"context"
	"fmt"
	"score-updater-svc/internal/repository/postgres"
)

type DataProvider struct {
	Client postgres.PgxRepository
}

func NewDataRepository(client postgres.PgxRepository) DataRepository {
	return &DataProvider{
		Client: client,
	}
}

type DataRepository interface {
	InsertScore(ctx context.Context, point int64) error
}

func (r *DataProvider) InsertScore(ctx context.Context, point int64) error {

	query := `INSERT INTO scores (score)  VALUES (  $1)`
	res, err := r.Client.Exec(ctx, query, point)
	if err != nil {
		return err
	}

	rowsAffected := res.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("query failed %v", "0 rows affected")
	}

	return nil
}
