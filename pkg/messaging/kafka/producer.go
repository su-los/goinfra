// Package kafka 提供了 Kafka 消息生产者、消费者、拦截器等工具。
package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

// Producer Kafka 消息生产者
type Producer struct {
	producer sarama.SyncProducer
	config   *sarama.Config
}

// NewProducer 创建新的生产者
func NewProducer(brokers []string, opts ...ConfigOption) (*Producer, error) {
	cfg := sarama.NewConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create producer")
	}

	return &Producer{
		producer: producer,
		config:   cfg,
	}, nil
}

// Send 发送消息
func (p *Producer) Send(topic, key string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	_, _, err := p.producer.SendMessage(msg)
	return err
}

// Close 关闭生产者
func (p *Producer) Close() error {
	return p.producer.Close()
}
