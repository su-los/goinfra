//go:build examples
// +build examples

package examples

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/su-los/goinfra/pkg/messaging/kafka"
)

// sarama 的使用样例

// AdvancedProducer 高级生产者示例
func AdvancedProducer() {
	// 创建高性能生产者配置
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Idempotent = true
	config.Producer.Retry.Max = 3
	config.Producer.Retry.Backoff = 100 * time.Millisecond
	config.Producer.Flush.Frequency = 10 * time.Millisecond
	config.Producer.Flush.Bytes = 16384
	config.Producer.Flush.Messages = 100

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("创建异步生产者失败: %v", err)
	}
	defer producer.Close()

	// 处理成功和错误消息
	go func() {
		for {
			select {
			case success := <-producer.Successes():
				fmt.Printf("消息发送成功: Topic=%s, Partition=%d, Offset=%d\n",
					success.Topic, success.Partition, success.Offset)
			case err := <-producer.Errors():
				fmt.Printf("消息发送失败: %v\n", err)
			}
		}
	}()

	// 发送消息
	for i := 0; i < 100; i++ {
		message := &sarama.ProducerMessage{
			Topic: "test-topic",
			Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", i)),
			Value: sarama.StringEncoder(fmt.Sprintf("message-%d", i)),
		}
		producer.Input() <- message
	}

	time.Sleep(5 * time.Second)
}

// AdvancedConsumer 高级消费者示例
func AdvancedConsumer() {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	config.Consumer.MaxWaitTime = 250 * time.Millisecond
	config.Consumer.Fetch.Min = 1
	config.Consumer.Fetch.Default = 1024 * 1024
	config.Consumer.Fetch.Max = 1048576

	consumer, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "advanced-group", config)
	if err != nil {
		log.Fatalf("创建消费者组失败: %v", err)
	}
	defer consumer.Close()

	handler := &AdvancedConsumerGroupHandler{
		handlers: make(map[string]kafka.MessageHandler),
		mu:       sync.RWMutex{},
	}

	// 注册处理器
	handler.RegisterHandler("user-events", func(ctx context.Context, message *sarama.ConsumerMessage) error {
		fmt.Printf("处理用户事件: %s\n", string(message.Value))
		return nil
	})

	handler.RegisterHandler("order-events", func(ctx context.Context, message *sarama.ConsumerMessage) error {
		fmt.Printf("处理订单事件: %s\n", string(message.Value))
		return nil
	})

	// 启动消费
	ctx := context.Background()
	for {
		err := consumer.Consume(ctx, []string{"user-events", "order-events"}, handler)
		if err != nil {
			log.Printf("消费错误: %v", err)
			time.Sleep(5 * time.Second)
		}
	}
}

// AdvancedConsumerGroupHandler 高级消费者组处理器
type AdvancedConsumerGroupHandler struct {
	handlers map[string]kafka.MessageHandler
	mu       sync.RWMutex
}

func (h *AdvancedConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	fmt.Printf("消费者组会话建立: MemberID=%s, GenerationID=%d\n",
		session.MemberID(), session.GenerationID())
	return nil
}

func (h *AdvancedConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	fmt.Printf("消费者组会话清理: MemberID=%s\n", session.MemberID())
	return nil
}

func (h *AdvancedConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Printf("开始消费分区: Topic=%s, Partition=%d\n", claim.Topic(), claim.InitialOffset())

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			h.mu.RLock()
			handler, exists := h.handlers[message.Topic]
			h.mu.RUnlock()

			if !exists {
				fmt.Printf("未找到主题 %s 的处理器\n", message.Topic)
				session.MarkMessage(message, "")
				continue
			}

			// 处理消息
			if err := handler(session.Context(), message); err != nil {
				fmt.Printf("处理消息失败: %v\n", err)
				// 根据错误类型决定是否重试
				if isRetryableError(err) {
					return err // 返回错误，消息不会被标记为已处理
				}
			}

			// 标记消息已处理
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

func (h *AdvancedConsumerGroupHandler) RegisterHandler(topic string, handler kafka.MessageHandler) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handlers[topic] = handler
}

// isRetryableError 判断是否为可重试错误
func isRetryableError(err error) bool {
	// 这里可以根据业务需求实现错误分类逻辑
	return false
}

// BatchProcessor 批量处理器示例
type BatchProcessor struct {
	batchSize    int
	batchTimeout time.Duration
	messages     []*sarama.ConsumerMessage
	mu           sync.Mutex
	timer        *time.Timer
	handler      func([]*sarama.ConsumerMessage) error
}

func NewBatchProcessor(batchSize int, batchTimeout time.Duration, handler func([]*sarama.ConsumerMessage) error) *BatchProcessor {
	return &BatchProcessor{
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		messages:     make([]*sarama.ConsumerMessage, 0, batchSize),
		handler:      handler,
	}
}

func (bp *BatchProcessor) Process(message *sarama.ConsumerMessage) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	bp.messages = append(bp.messages, message)

	// 如果达到批量大小，立即处理
	if len(bp.messages) >= bp.batchSize {
		return bp.flush()
	}

	// 设置定时器
	if bp.timer == nil {
		bp.timer = time.AfterFunc(bp.batchTimeout, func() {
			bp.mu.Lock()
			defer bp.mu.Unlock()
			if len(bp.messages) > 0 {
				bp.flush()
			}
		})
	}

	return nil
}

func (bp *BatchProcessor) flush() error {
	if len(bp.messages) == 0 {
		return nil
	}

	messages := make([]*sarama.ConsumerMessage, len(bp.messages))
	copy(messages, bp.messages)
	bp.messages = bp.messages[:0]

	if bp.timer != nil {
		bp.timer.Stop()
		bp.timer = nil
	}

	return bp.handler(messages)
}

// ExampleBatchProcessing 批量处理示例
func ExampleBatchProcessing() {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "batch-group", config)
	if err != nil {
		log.Fatalf("创建消费者组失败: %v", err)
	}
	defer consumer.Close()

	// 创建批量处理器
	batchProcessor := NewBatchProcessor(100, 5*time.Second, func(messages []*sarama.ConsumerMessage) error {
		fmt.Printf("批量处理 %d 条消息\n", len(messages))
		// 这里实现批量处理逻辑
		return nil
	})

	handler := &BatchConsumerGroupHandler{
		batchProcessor: batchProcessor,
	}

	ctx := context.Background()
	for {
		err := consumer.Consume(ctx, []string{"batch-topic"}, handler)
		if err != nil {
			log.Printf("消费错误: %v", err)
			time.Sleep(5 * time.Second)
		}
	}
}

// BatchConsumerGroupHandler 批量消费者组处理器
type BatchConsumerGroupHandler struct {
	batchProcessor *BatchProcessor
}

func (h *BatchConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *BatchConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *BatchConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			if err := h.batchProcessor.Process(message); err != nil {
				fmt.Printf("批量处理失败: %v\n", err)
			}

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

// TransactionalProducer 事务生产者示例
func TransactionalProducer() {
	config := sarama.NewConfig()
	config.Producer.Idempotent = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Transaction.Retry.Max = 3
	config.Producer.Transaction.Retry.Backoff = 100 * time.Millisecond

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("创建事务生产者失败: %v", err)
	}
	defer producer.Close()

	// 开始事务
	producer.BeginTxn()

	// 发送事务消息
	for i := 0; i < 10; i++ {
		message := &sarama.ProducerMessage{
			Topic: "transaction-topic",
			Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", i)),
			Value: sarama.StringEncoder(fmt.Sprintf("transaction-message-%d", i)),
		}
		producer.Input() <- message
	}

	// 提交事务
	err = producer.CommitTxn()
	if err != nil {
		fmt.Printf("提交事务失败: %v\n", err)
		// 回滚事务
		producer.AbortTxn()
	} else {
		fmt.Println("事务提交成功")
	}
}

// StructuredMessage 结构化消息示例
type StructuredMessage struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

// ExampleStructuredMessages 结构化消息示例
func ExampleStructuredMessages() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Compression = sarama.CompressionGZIP

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("创建生产者失败: %v", err)
	}
	defer producer.Close()

	// 发送结构化消息
	message := StructuredMessage{
		ID:        "msg-001",
		Type:      "user_event",
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"user_id": "12345",
			"action":  "login",
		},
	}

	jsonData, err := json.Marshal(message)
	if err != nil {
		log.Fatalf("序列化消息失败: %v", err)
	}

	producerMessage := &sarama.ProducerMessage{
		Topic: "structured-events",
		Key:   sarama.StringEncoder(message.ID),
		Value: sarama.ByteEncoder(jsonData),
		Headers: []sarama.RecordHeader{
			{Key: []byte("message-type"), Value: []byte(message.Type)},
			{Key: []byte("timestamp"), Value: []byte(message.Timestamp.Format(time.RFC3339))},
		},
	}

	partition, offset, err := producer.SendMessage(producerMessage)
	if err != nil {
		log.Fatalf("发送消息失败: %v", err)
	}

	fmt.Printf("消息发送成功: Partition=%d, Offset=%d\n", partition, offset)
}
