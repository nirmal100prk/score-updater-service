package service

import (
	"context"
	"score-updater-svc/internal/repository/datastore"
)

type ScoreService struct {
	scoreRepo datastore.DataRepository
}

func NewScoreService(repo datastore.DataRepository) *ScoreService {
	return &ScoreService{scoreRepo: repo}
}

func (s *ScoreService) UpdateScore(ctx context.Context, val int64) error {
	return s.scoreRepo.InsertScore(ctx,val)
}
