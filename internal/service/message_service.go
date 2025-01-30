package service

import (
	"encoding/json"
	"log"
	"score-updater-svc/internal/models"
	"score-updater-svc/internal/repository/kafka"
)

type MessageService struct {
	messageRepo kafka.MessageRepository
}

func NewMessageService(messageRepo kafka.MessageRepository) *MessageService {
	return &MessageService{
		messageRepo: messageRepo,
	}
}

func (s *MessageService) StartConsuming() (<-chan models.Message, error) {

	messageChannel := make(chan models.Message)
	go func() {
		processMessage := func(message []byte) error {
			var msg models.Message
			err := json.Unmarshal(message, &msg)
			if err != nil {
				log.Printf("Failed to unmarshal message: %v", err)
				return err
			}

			messageChannel <- msg
			return nil
		}

		s.messageRepo.StartConsuming(processMessage)
	}()

	return messageChannel, nil
}
