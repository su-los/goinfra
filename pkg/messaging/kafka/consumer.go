package kafka

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/pkg/errors"
)

// Consumer Kafka 消息消费者
type Consumer struct {
	consumer      sarama.ConsumerGroup // 消费者组
	config        *sarama.Config
	handlers      map[string]MessageHandler // 消息处理器，key 为 topic，value 为消息处理器
	mu            sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
	retryInterval time.Duration
}

// MessageHandler 消息处理函数
type MessageHandler func(ctx context.Context, message *sarama.ConsumerMessage) error

type ConsumerGroupHandler interface {
	sarama.ConsumerGroupHandler
	IsRetryableError(err error) bool // 在处理消息失败时，标记是否可重试
}

// ConsumerGroupHandlerImpl 消费者组处理器
type ConsumerGroupHandlerImpl struct {
	consumer *Consumer
}

// Setup 实现 sarama.ConsumerGroupHandler 接口
func (h *ConsumerGroupHandlerImpl) Setup(session sarama.ConsumerGroupSession) error {
	log.Printf("consumer group session setup MemberID=%s, GenerationID=%d\n",
		session.MemberID(), session.GenerationID())
	return nil
}

// Cleanup 实现 sarama.ConsumerGroupHandler 接口
func (h *ConsumerGroupHandlerImpl) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Printf("consumer group session cleanup MemberID=%s\n",
		session.MemberID())
	return nil
}

// ConsumeClaim 实现 sarama.ConsumerGroupHandler 接口
func (h *ConsumerGroupHandlerImpl) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("start consume claim Topic=%s, Partition=%d\n",
		claim.Topic(), claim.Partition())

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			// 获取对应的处理器
			h.consumer.mu.RLock()
			handler, exists := h.consumer.handlers[message.Topic]
			h.consumer.mu.RUnlock()

			if !exists {
				log.Printf("no handler found for topic %s", message.Topic)
				session.MarkMessage(message, "")
				continue
			}

			// 处理消息
			if err := handler(session.Context(), message); err != nil {
				log.Printf("handle message failed: %v", err)
				// 根据错误类型决定是否标记消息
				// 这里可以根据业务需求实现重试逻辑
				if h.IsRetryableError(err) {
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

// IsRetryableError 判断是否为可重试错误
func (h *ConsumerGroupHandlerImpl) IsRetryableError(err error) bool {
	// 这里可以根据业务需求实现错误分类逻辑
	return false
}

// NewConsumer 创建新的消费者
//
// groupID 消费者组ID
func NewConsumer(brokers []string, groupID string, opts ...ConfigOption) (*Consumer, error) {
	cfg := sarama.NewConfig()

	// 设置默认配置
	cfg.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Offsets.AutoCommit.Enable = true
	cfg.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	// 应用自定义配置
	for _, opt := range opts {
		opt(cfg)
	}

	if err := cfg.Validate(); err != nil {
		return nil, errors.WithStack(err)
	}

	// 奇怪的现象：连接 v3.9.1 的 kafka 时（IBM/sarama 版本 1.45.1），会报错，如果提前设置 sarama.Logger，就会连接报错（EOF）
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags|log.Lshortfile)
	consumer, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Consumer{
		consumer: consumer,
		config:   cfg,
		handlers: make(map[string]MessageHandler),
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

// RegisterHandler 注册消息处理器
func (c *Consumer) RegisterHandler(topic string, handler MessageHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers[topic] = handler
}

// UnregisterHandler 注销消息处理器
func (c *Consumer) UnregisterHandler(topic string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.handlers, topic)
}

// consumerStartOption 消费者启动选项
type consumerStartOption struct {
	retryInterval time.Duration
}

func defaultConsumerStartOption() *consumerStartOption {
	return &consumerStartOption{
		retryInterval: 3 * time.Second, // 处理失败时的重试间隔
	}
}

// StartOption 消费者选项
type StartOption func(*consumerStartOption)

// WithRetryInterval 设置重试间隔
func WithRetryInterval(interval time.Duration) StartOption {
	return func(o *consumerStartOption) {
		o.retryInterval = interval
	}
}

// Start 启动消费者
func (c *Consumer) Start(topics []string, opts ...StartOption) error {
	handler := &ConsumerGroupHandlerImpl{consumer: c}
	opt := defaultConsumerStartOption()
	for _, o := range opts {
		o(opt)
	}

	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			default:
				if err := c.consumer.Consume(c.ctx, topics, handler); err != nil {
					log.Printf("consumer error: %v", err)
					time.Sleep(c.retryInterval) // 重试间隔
				}
			}
		}
	}()

	return nil
}

// Stop 停止消费者
func (c *Consumer) Stop() error {
	c.cancel()
	return c.consumer.Close()
}

// GetHandlers 获取所有注册的处理器
func (c *Consumer) GetHandlers() map[string]MessageHandler {
	c.mu.RLock()
	defer c.mu.RUnlock()

	handlers := make(map[string]MessageHandler)
	for topic, handler := range c.handlers {
		handlers[topic] = handler
	}
	return handlers
}

// SimpleConsumer 简单消费者（不使用消费者组）
type SimpleConsumer struct {
	consumer sarama.Consumer
	config   *sarama.Config
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewSimpleConsumer 创建简单消费者
func NewSimpleConsumer(brokers []string, opts ...ConfigOption) (*SimpleConsumer, error) {
	cfg := sarama.NewConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	consumer, err := sarama.NewConsumer(brokers, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create consumer")
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &SimpleConsumer{
		consumer: consumer,
		config:   cfg,
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

// ConsumeTopic 消费指定主题
func (sc *SimpleConsumer) ConsumeTopic(topic string, partition int32, handler MessageHandler) error {
	partitionConsumer, err := sc.consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return errors.Wrap(err, "failed to create partition consumer")
	}
	defer partitionConsumer.Close()

	for {
		select {
		case message, ok := <-partitionConsumer.Messages():
			if !ok {
				return nil
			}

			if err := handler(sc.ctx, message); err != nil {
				log.Printf("处理消息失败: %v", err)
			}

		case err := <-partitionConsumer.Errors():
			log.Printf("分区消费者错误: %v", err)

		case <-sc.ctx.Done():
			return nil
		}
	}
}

// GetPartitions 获取主题的分区列表
func (sc *SimpleConsumer) GetPartitions(topic string) ([]int32, error) {
	return sc.consumer.Partitions(topic)
}

// Close 关闭简单消费者
func (sc *SimpleConsumer) Close() error {
	sc.cancel()
	return sc.consumer.Close()
}

// ConsumerStats 消费者统计信息
type ConsumerStats struct {
	GroupID      string
	Topics       []string
	HandlerCount int
	IsRunning    bool
}

// GetStats 获取消费者统计信息
func (c *Consumer) GetStats() *ConsumerStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return &ConsumerStats{
		HandlerCount: len(c.handlers),
		IsRunning:    c.ctx.Err() == nil,
	}
}
