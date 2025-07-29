//go:build examples
// +build examples

// Package examples 提供 Kafka 的示例代码
package examples

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/su-los/goinfra/pkg/messaging/kafka"
)

// ExampleConsumerGroup 消费者组使用示例
func ExampleConsumerGroup() {
	// 创建消费者
	consumer, err := kafka.NewConsumer(
		[]string{"localhost:9092"},
		"my-consumer-group",
		kafka.WithClientID("example-consumer"),
		kafka.WithDialTimeout(10*time.Second),
		kafka.WithConsumerOffsetsInitial(sarama.OffsetOldest),
		kafka.WithConsumerOffsetsAutoCommit(true),
		kafka.WithConsumerOffsetsAutoCommitInterval(1*time.Second),
	)
	if err != nil {
		log.Fatalf("创建消费者失败: %v", err)
	}
	defer consumer.Stop()

	// 注册消息处理器
	consumer.RegisterHandler("user-events", func(ctx context.Context, message *sarama.ConsumerMessage) error {
		fmt.Printf("收到用户事件: Topic=%s, Partition=%d, Offset=%d, Key=%s, Value=%s\n",
			message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))
		return nil
	})

	consumer.RegisterHandler("order-events", func(ctx context.Context, message *sarama.ConsumerMessage) error {
		fmt.Printf("收到订单事件: Topic=%s, Partition=%d, Offset=%d, Key=%s, Value=%s\n",
			message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))
		return nil
	})

	// 启动消费者
	err = consumer.Start([]string{"user-events", "order-events"})
	if err != nil {
		log.Fatalf("启动消费者失败: %v", err)
	}

	// 运行一段时间
	time.Sleep(30 * time.Second)
}

// ExampleSimpleConsumer 简单消费者使用示例
func ExampleSimpleConsumer() {
	// 创建简单消费者
	consumer, err := kafka.NewSimpleConsumer(
		[]string{"localhost:9092"},
		kafka.WithClientID("simple-consumer"),
		kafka.WithDialTimeout(10*time.Second),
	)
	if err != nil {
		log.Fatalf("创建简单消费者失败: %v", err)
	}
	defer consumer.Close()

	// 获取主题分区
	partitions, err := consumer.GetPartitions("test-topic")
	if err != nil {
		log.Fatalf("获取分区失败: %v", err)
	}

	// 为每个分区启动一个消费者
	for _, partition := range partitions {
		go func(p int32) {
			err := consumer.ConsumeTopic("test-topic", p, func(ctx context.Context, message *sarama.ConsumerMessage) error {
				fmt.Printf("分区 %d 收到消息: Offset=%d, Key=%s, Value=%s\n",
					p, message.Offset, string(message.Key), string(message.Value))
				return nil
			})
			if err != nil {
				log.Printf("消费分区 %d 失败: %v", p, err)
			}
		}(partition)
	}

	// 运行一段时间
	time.Sleep(30 * time.Second)
}

// UserEvent 用户事件结构
type UserEvent struct {
	UserID    string                 `json:"user_id"`
	EventType string                 `json:"event_type"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

// ExampleStructuredMessageHandler 结构化消息处理示例
func ExampleStructuredMessageHandler() {
	consumer, err := kafka.NewConsumer(
		[]string{"localhost:9092"},
		"structured-consumer-group",
		kafka.WithConsumerOffsetsInitial(sarama.OffsetNewest),
	)
	if err != nil {
		log.Fatalf("创建消费者失败: %v", err)
	}
	defer consumer.Stop()

	// 注册结构化消息处理器
	consumer.RegisterHandler("user-events", func(ctx context.Context, message *sarama.ConsumerMessage) error {
		var event UserEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Printf("解析消息失败: %v", err)
			return err
		}

		fmt.Printf("用户事件: UserID=%s, EventType=%s, Timestamp=%v\n",
			event.UserID, event.EventType, event.Timestamp)

		// 根据事件类型处理
		switch event.EventType {
		case "user_registered":
			fmt.Println("处理用户注册事件")
		case "user_login":
			fmt.Println("处理用户登录事件")
		case "user_logout":
			fmt.Println("处理用户登出事件")
		default:
			fmt.Printf("未知事件类型: %s\n", event.EventType)
		}

		return nil
	})

	err = consumer.Start([]string{"user-events"})
	if err != nil {
		log.Fatalf("启动消费者失败: %v", err)
	}

	time.Sleep(30 * time.Second)
}

// ExampleErrorHandling 错误处理示例
func ExampleErrorHandling() {
	consumer, err := kafka.NewConsumer(
		[]string{"localhost:9092"},
		"error-handling-group",
		kafka.WithConsumerOffsetsAutoCommit(false), // 手动提交偏移量
	)
	if err != nil {
		log.Fatalf("创建消费者失败: %v", err)
	}
	defer consumer.Stop()

	consumer.RegisterHandler("error-test", func(ctx context.Context, message *sarama.ConsumerMessage) error {
		// 模拟处理错误
		if string(message.Key) == "error" {
			return fmt.Errorf("模拟处理错误")
		}

		fmt.Printf("成功处理消息: Key=%s, Value=%s\n", string(message.Key), string(message.Value))
		return nil
	})

	err = consumer.Start([]string{"error-test"})
	if err != nil {
		log.Fatalf("启动消费者失败: %v", err)
	}

	time.Sleep(30 * time.Second)
}

// ExampleConsumerWithRetry 带重试的消费者示例
func ExampleConsumerWithRetry() {
	consumer, err := kafka.NewConsumer(
		[]string{"localhost:9092"},
		"retry-consumer-group",
		kafka.WithConsumerRetryBackoff(1*time.Second),
	)
	if err != nil {
		log.Fatalf("创建消费者失败: %v", err)
	}
	defer consumer.Stop()

	consumer.RegisterHandler("retry-test", func(ctx context.Context, message *sarama.ConsumerMessage) error {
		// 检查上下文是否被取消
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 模拟需要重试的处理逻辑
		if string(message.Key) == "retry" {
			// 这里可以实现自定义的重试逻辑
			fmt.Println("需要重试的消息，跳过处理")
			return fmt.Errorf("需要重试")
		}

		fmt.Printf("处理消息: Key=%s, Value=%s\n", string(message.Key), string(message.Value))
		return nil
	})

	err = consumer.Start([]string{"retry-test"})
	if err != nil {
		log.Fatalf("启动消费者失败: %v", err)
	}

	time.Sleep(30 * time.Second)
}

// ExampleConsumerStats 消费者统计信息示例
func ExampleConsumerStats() {
	consumer, err := kafka.NewConsumer(
		[]string{"localhost:9092"},
		"stats-consumer-group",
	)
	if err != nil {
		log.Fatalf("创建消费者失败: %v", err)
	}
	defer consumer.Stop()

	// 注册多个处理器
	consumer.RegisterHandler("topic1", func(ctx context.Context, message *sarama.ConsumerMessage) error {
		fmt.Printf("处理 topic1 消息: %s\n", string(message.Value))
		return nil
	})

	consumer.RegisterHandler("topic2", func(ctx context.Context, message *sarama.ConsumerMessage) error {
		fmt.Printf("处理 topic2 消息: %s\n", string(message.Value))
		return nil
	})

	// 启动消费者
	err = consumer.Start([]string{"topic1", "topic2"})
	if err != nil {
		log.Fatalf("启动消费者失败: %v", err)
	}

	// 定期获取统计信息
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				stats := consumer.GetStats()
				fmt.Printf("消费者统计: HandlerCount=%d, IsRunning=%v\n",
					stats.HandlerCount, stats.IsRunning)
			}
		}
	}()

	time.Sleep(30 * time.Second)
}
