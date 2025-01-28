package kafka

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaConsumer struct {
	Consumer *kafka.Consumer
	Topic    string
}

func NewKafkaConsumer(brokers []string, topic string, groupID string) (*KafkaConsumer, error) {
	// Create a new Kafka consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers[0], // Use the first broker in the list
		"group.id":          groupID,    // Consumer group ID
		"auto.offset.reset": "earliest", // Start consuming from the earliest offset if no offset is committed
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	// Subscribe to the topic
	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to topic: %w", err)
	}

	return &KafkaConsumer{
		Consumer: consumer,
		Topic:    topic,
	}, nil
}

type MessageRepository interface {
	StartConsuming(processMessage func(message []byte) error)
}

func NewMessageRepository(kp *KafkaConsumer) MessageRepository {
	return kp
}

func (kc *KafkaConsumer) StartConsuming(processMessage func(message []byte) error) {
	// Handle graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Start consuming messages
	for {
		select {
		case <-sigchan:
			log.Println("Shutting down consumer...")
			kc.Consumer.Close()
			return
		default:
			// Poll for new messages
			msg, err := kc.Consumer.ReadMessage(-1) // Blocking call
			if err != nil {
				log.Printf("Error while consuming message: %v\n", err)
				continue
			}

			// Process the message
			err = processMessage(msg.Value)
			if err != nil {
				log.Printf("Failed to process message: %v\n", err)
				continue
			}

			// Commit the offset
			_, err = kc.Consumer.CommitMessage(msg)
			if err != nil {
				log.Printf("Failed to commit offset: %v\n", err)
			} else {
				log.Printf("Processed message: %s\n", string(msg.Value))
			}
		}
	}
}

func (kc *KafkaConsumer) CloseConnection() {
	kc.Consumer.Close()
}
