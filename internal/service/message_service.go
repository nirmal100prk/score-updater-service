package service

import (
	"encoding/json"
	"fmt"
	"score-updater-svc/internal/repository/kafka"
)

type MessageService struct {
	messageRepo kafka.MessageRepository
}

func NewMessageRepository(messageRepo kafka.MessageRepository) *MessageService {
	return &MessageService{
		messageRepo: messageRepo,
	}
}

func (s *MessageService) StartConsuming() (string, error) {

	var msg string
	processMessage := func(message []byte) error {

		err := json.Unmarshal(message, &msg)
		if err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}

		return nil
	}

	s.messageRepo.StartConsuming(processMessage)

	return msg, nil
}
