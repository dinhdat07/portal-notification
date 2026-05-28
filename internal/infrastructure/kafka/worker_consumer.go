package kafka

import (
	"context"

	"portal-notification/internal/worker"

	"github.com/segmentio/kafka-go"
)

var _ worker.Consumer = (*Consumer)(nil)

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(reader *kafka.Reader) *Consumer {
	return &Consumer{reader: reader}
}

func (c *Consumer) FetchMessage(ctx context.Context) (worker.Message, error) {
	m, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return worker.Message{}, err
	}
	return worker.Message{
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
		Original:  m,
	}, nil
}

func (c *Consumer) CommitMessages(ctx context.Context, msgs ...worker.Message) error {
	kafkaMsgs := make([]kafka.Message, len(msgs))
	for i, m := range msgs {
		kafkaMsgs[i] = m.Original.(kafka.Message)
	}
	return c.reader.CommitMessages(ctx, kafkaMsgs...)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
