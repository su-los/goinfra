package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/stretchr/testify/require"
)

// TestProducerWithMock 使用 Mock 测试生产者
func TestProducerWithMock(t *testing.T) {
	// 创建 Mock 生产者
	mockProducer := mocks.NewSyncProducer(t, nil)
	// 期望发送消息并成功
	mockProducer.ExpectSendMessageAndSucceed()

	// 创建生产者
	producer := &Producer{
		producer: mockProducer,
		config:   sarama.NewConfig(),
	}

	// 测试发送消息
	err := producer.Send("test-topic", "test-key", []byte("test-message"))
	require.NoError(t, err)
}

// TestSimpleConsumerWithMock 使用 Mock 测试简单消费者
func TestSimpleConsumerWithMock(t *testing.T) {
	// 创建 Mock 消费者
	mockConsumer := mocks.NewConsumer(t, nil)

	// 期望消费分区
	mockPartitionConsumer := mockConsumer.ExpectConsumePartition("test-topic", 0, sarama.OffsetOldest)

	// 产生测试消息
	mockPartitionConsumer.YieldMessage(&sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    1,
		Key:       []byte("test-key"),
		Value:     []byte("test-message"),
	})

	ctx, cancel := context.WithCancel(context.Background())
	// 创建简单消费者
	simpleConsumer := &SimpleConsumer{
		consumer: mockConsumer,
		config:   sarama.NewConfig(),
		ctx:      ctx,
		cancel:   cancel,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	// 测试消费消息
	messageReceived := false
	go func() {
		defer wg.Done()
		err := simpleConsumer.ConsumeTopic("test-topic", 0, func(_ context.Context, message *sarama.ConsumerMessage) error {
			messageReceived = true
			require.Equal(t, "test-topic", message.Topic)
			require.Equal(t, []byte("test-key"), message.Key)
			require.Equal(t, []byte("test-message"), message.Value)
			return nil
		})
		require.NoError(t, err)
	}()

	time.Sleep(1000 * time.Millisecond)
	simpleConsumer.Close()
	wg.Wait()
	require.True(t, messageReceived)
}

// TestConsumerStats 测试消费者统计信息
func TestConsumerStats(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		config:   sarama.NewConfig(),
		handlers: make(map[string]MessageHandler),
		ctx:      ctx,
		cancel:   cancel,
	}

	// 注册处理器
	consumer.RegisterHandler("test-topic", func(ctx context.Context, message *sarama.ConsumerMessage) error {
		return nil
	})

	// 获取统计信息
	stats := consumer.GetStats()
	require.Equal(t, 1, stats.HandlerCount)
	require.True(t, stats.IsRunning)
}

// TestConsumerHandlerManagement 测试处理器管理
func TestConsumerHandlerManagement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		config:   sarama.NewConfig(),
		handlers: make(map[string]MessageHandler),
		ctx:      ctx,
		cancel:   cancel,
	}

	// 注册处理器
	consumer.RegisterHandler("topic1", func(ctx context.Context, message *sarama.ConsumerMessage) error {
		return nil
	})

	consumer.RegisterHandler("topic2", func(ctx context.Context, message *sarama.ConsumerMessage) error {
		return nil
	})

	// 验证处理器数量
	handlers := consumer.GetHandlers()
	require.Len(t, handlers, 2)
	require.Contains(t, handlers, "topic1")
	require.Contains(t, handlers, "topic2")

	// 注销处理器
	consumer.UnregisterHandler("topic1")
	handlers = consumer.GetHandlers()
	require.Len(t, handlers, 1)
	require.Contains(t, handlers, "topic2")
}

// TestSimpleConsumerPartitions 测试简单消费者分区
func TestSimpleConsumerPartitions(t *testing.T) {
	// 创建 Mock 消费者
	mockConsumer := mocks.NewConsumer(t, nil)

	// 模拟分区
	mockConsumer.SetTopicMetadata(map[string][]int32{
		"test-topic": {0, 1, 2},
	})

	simpleConsumer := &SimpleConsumer{
		consumer: mockConsumer,
		config:   sarama.NewConfig(),
		ctx:      context.Background(),
	}

	// 获取分区
	partitions, err := simpleConsumer.GetPartitions("test-topic")
	require.NoError(t, err)
	require.Len(t, partitions, 3)
	require.Contains(t, partitions, int32(0))
	require.Contains(t, partitions, int32(1))
	require.Contains(t, partitions, int32(2))
}
