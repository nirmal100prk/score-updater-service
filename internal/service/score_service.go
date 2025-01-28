package service

import (
	"context"
	"score-updater-svc/internal/repository/postgres"
)

type ScoreService struct {
	scoreRepo postgres.PgxRepository
}

func NewScoreService(repo postgres.PgxRepository) *ScoreService {
	return &ScoreService{scoreRepo: repo}
}

func (s *ScoreService) UpdateScore(ctx context.Context, val any) error {
	return s.scoreRepo.UpdateScore(ctx)
}
