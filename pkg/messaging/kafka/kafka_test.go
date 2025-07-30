package kafka

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProducerWithMock 使用 Mock 测试生产者
func TestProducerWithMock(t *testing.T) {
	// 创建 Mock 生产者
	mockProducer := mocks.NewSyncProducer(t, nil)
	// 期望发送消息并成功
	mockProducer.ExpectSendMessageAndSucceed()

	// 创建生产者
	producer := &SyncProducer{
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

// 需要本地启动 kafka 服务
func TestConsumerGroupHandlerImpl(t *testing.T) {
	var (
		// 使用 localhost 访问 Kafka
		brokers = []string{"localhost:9092"}
		topic   = "test-topic"
		group   = "test-group"
	)

	log.Println("开始测试消费者组处理器")
	consumer, err := NewConsumer(brokers, group,
		WithAPIVersion(sarama.V4_0_0_0),                 // 使用更低的 API 版本以兼容旧版 Kafka
		WithConsumerOffsetsInitial(sarama.OffsetOldest), // 从最早的消息开始消费
		WithConsumerOffsetsAutoCommit(true),             // 启用自动提交
		WithConsumerMaxWaitTime(1*time.Second),          // 设置最大等待时间
		WithDialTimeout(5*time.Second),                  // 设置连接超时
	)
	require.NoError(t, err)
	log.Println("消费者创建成功")

	var cnt atomic.Int32
	consumer.RegisterHandler(topic, func(ctx context.Context, message *sarama.ConsumerMessage) error {
		cnt.Add(1)
		log.Printf("收到消息 #%d: %s", cnt.Load(), string(message.Value))
		return nil
	})
	log.Println("处理器注册成功")

	// 启动消费者
	err = consumer.Start([]string{topic}, WithRetryInterval(1*time.Second))
	require.NoError(t, err)
	log.Println("消费者启动成功")

	// 等待消费者完全启动
	log.Println("等待消费者启动...")
	time.Sleep(3 * time.Second)

	// 启动生产者
	producer, err := NewSyncProducer(brokers,
		WithAPIVersion(sarama.V4_0_0_0), // 使用更低的 API 版本以兼容旧版 Kafka
		WithDialTimeout(5*time.Second),  // 设置连接超时
	)
	require.NoError(t, err)
	defer producer.Close()
	log.Println("生产者创建成功")

	// 发送消息
	log.Println("开始发送消息...")
	for i := range 10 {
		message := fmt.Sprintf("test-message-%d", i)
		err := producer.Send(topic, "test-key", []byte(message))
		require.NoError(t, err)
		log.Printf("发送消息 #%d: %s", i+1, message)
	}
	log.Println("所有消息发送完成")

	// 等待消息被消费
	log.Println("等待消息被消费...")
	time.Sleep(6 * time.Second)

	actualCount := cnt.Load()
	log.Printf("实际收到消息数量: %d", actualCount)
	require.Equal(t, int32(10), actualCount, "期望收到10条消息，实际收到%d条", actualCount)

	require.NoError(t, consumer.Stop())
	log.Println("测试完成")
}

// 测试消费失败重试（需要本地启动 kafka 服务）
func TestConsumerGroupHandlerImpl_Err(t *testing.T) {
	var (
		// 使用 localhost 访问 Kafka
		brokers = []string{"localhost:9092"}
		topic   = "test-topic"
		group   = "test-group"
	)

	log.Println("开始测试消费者组处理器")
	consumer, err := NewConsumer(brokers, group,
		WithAPIVersion(sarama.V4_0_0_0),                 // 使用更低的 API 版本以兼容旧版 Kafka
		WithConsumerOffsetsInitial(sarama.OffsetOldest), // 从最早的消息开始消费
		WithConsumerOffsetsAutoCommit(true),             // 启用自动提交
		WithConsumerMaxWaitTime(1*time.Second),          // 设置最大等待时间
		WithDialTimeout(5*time.Second),                  // 设置连接超时
	)
	require.NoError(t, err)
	log.Println("消费者创建成功")

	var cnt atomic.Int32
	consumer.RegisterHandler(topic, func(ctx context.Context, message *sarama.ConsumerMessage) error {
		cnt.Add(1)
		log.Printf("收到消息 #%d: %s", cnt.Load(), string(message.Value))
		return assert.AnError
	})

	err = consumer.Start([]string{topic}, WithRetryInterval(1*time.Second))
	require.NoError(t, err)
	log.Println("消费者启动成功")

	producer, err := NewSyncProducer(brokers,
		WithAPIVersion(sarama.V4_0_0_0), // 使用更低的 API 版本以兼容旧版 Kafka
		WithDialTimeout(5*time.Second),  // 设置连接超时
	)
	require.NoError(t, err)
	defer producer.Close()
	log.Println("生产者创建成功")

	for i := range 10 {
		message := fmt.Sprintf("test-message-%d", i)
		err := producer.Send(topic, "test-key", []byte(message))
		require.NoError(t, err)
		log.Printf("发送消息 #%d: %s", i+1, message)
	}

	time.Sleep(6 * time.Second)
	require.NoError(t, consumer.Stop())
	log.Println("测试完成")
	require.Equal(t, int32(10), cnt.Load(), "期望收到10条消息，实际收到%d条", cnt.Load())
}
