package kafka

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

type KafkaConsumer struct {
	Consumer sarama.ConsumerGroup
	Topic    string
	GroupID  string
}

func NewKafkaConsumer(brokers []string, topic string, groupID string) (*KafkaConsumer, error) {
	// Create a new Kafka consumer config
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// Create a new Kafka consumer
	consumer, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	return &KafkaConsumer{
		Consumer: consumer,
		Topic:    topic,
		GroupID:  groupID,
	}, nil
}

type MessageRepository interface {
	StartConsuming(processMessage func(message []byte) error)
}

func NewMessageRepository(kp *KafkaConsumer) MessageRepository {
	return kp
}

func (kc *KafkaConsumer) StartConsuming(processMessage func(message []byte) error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	handler := &ConsumerGroupHandler{processMessage: processMessage}

	go func() {
		for {
			if err := kc.Consumer.Consume(ctx, []string{kc.Topic}, handler); err != nil {
				log.Printf("Error while consuming: %v\n", err)
			}
			// Check for cancellation
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	// Wait for shutdown signal
	<-sigchan
	log.Println("Shutting down Kafka consumer...")
}

func (kc *KafkaConsumer) CloseConnection() {
	if err := kc.Consumer.Close(); err != nil {
		log.Printf("Error closing Kafka consumer: %v\n", err)
	}
}

type ConsumerGroupHandler struct {
	processMessage func(message []byte) error
}

func (h *ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("Received raw message: %s\n", string(msg.Value))

		// Process the message
		err := h.processMessage(msg.Value)
		if err != nil {
			log.Printf("Failed to process message: %v\n", err)
			continue
		}

		// Mark message as processed
		session.MarkMessage(msg, "")

	}
	return nil
}
