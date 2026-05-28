package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func NewReader(brokers []string, topic []string, groupID string) *kafka.Reader {
	log.Printf("initializing kafka reader brokers=%v topics=%v group_id=%s", brokers, topic, groupID)

	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupTopics: topic,
		GroupID:     groupID,

		MinBytes: 1,
		MaxBytes: 10e6,

		StartOffset:    kafka.FirstOffset,
		CommitInterval: 0,
		MaxWait:        1 * time.Second,
	})
}

func Ping(ctx context.Context, brokers []string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("kafka brokers are required")
	}

	dialer := &kafka.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka broker %s: %w", brokers[0], err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("failed to close kafka ping connection broker=%s error=%v", brokers[0], err)
		}
	}()

	log.Printf("kafka broker connection verified broker=%s", brokers[0])
	return nil
}
