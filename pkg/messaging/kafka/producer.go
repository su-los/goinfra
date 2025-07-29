// Package kafka 提供了 Kafka 消息生产者、消费者、拦截器等工具。
package kafka

import (
	"github.com/IBM/sarama"
	"github.com/pkg/errors"
)

// SyncProducer 同步生产者
type SyncProducer struct {
	producer sarama.SyncProducer
	config   *sarama.Config
}

// NewSyncProducer 创建新的同步生产者
func NewSyncProducer(brokers []string, opts ...ConfigOption) (*SyncProducer, error) {
	cfg := sarama.NewConfig()
	// 设置 SyncProducer 必需的配置
	cfg.Producer.Return.Successes = true

	for _, opt := range opts {
		opt(cfg)
	}

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create sync producer")
	}

	return &SyncProducer{
		producer: producer,
		config:   cfg,
	}, nil
}

// Send 发送消息
func (sp *SyncProducer) Send(topic, key string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	_, _, err := sp.producer.SendMessage(msg)
	return err
}

// Close 关闭同步生产者
func (sp *SyncProducer) Close() error {
	return sp.producer.Close()
}

// AsyncProducer 异步生产者
type AsyncProducer struct {
	producer sarama.AsyncProducer
	config   *sarama.Config
}

// NewAsyncProducer 创建新的异步生产者（待完善）
func NewAsyncProducer(brokers []string, opts ...ConfigOption) (*AsyncProducer, error) {
	cfg := sarama.NewConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	producer, err := sarama.NewAsyncProducer(brokers, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create async producer")
	}

	return &AsyncProducer{
		producer: producer,
		config:   cfg,
	}, nil
}
