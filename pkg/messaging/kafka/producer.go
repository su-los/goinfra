// Package kafka 提供了 Kafka 消息生产者、消费者、拦截器等工具。
package kafka

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

// Producer Kafka 消息生产者
type Producer struct {
	producer sarama.SyncProducer
	config   *sarama.Config
}

// Config 配置
// type Config struct {
// 	// 基础配置
// 	Brokers []string // Kafka 集群地址列表

// 	// 连接配置
// 	ClientID     string        // 客户端标识
// 	DialTimeout  time.Duration // 连接超时时间
// 	ReadTimeout  time.Duration // 读取超时时间
// 	WriteTimeout time.Duration // 写入超时时间

// 	// 认证配置
// 	Username  string // SASL 用户名
// 	Password  string // SASL 密码
// 	Mechanism string // SASL 机制 (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)

// 	// TLS 配置
// 	TLSEnabled  bool   // 是否启用 TLS
// 	TLSCAFile   string // CA 证书文件路径
// 	TLSCertFile string // 客户端证书文件路径
// 	TLSKeyFile  string // 客户端私钥文件路径

// 	// 生产者配置
// 	RequiredAcks     sarama.RequiredAcks     // 确认机制 (NoResponse, WaitForLocal, WaitForAll)
// 	MaxMessageBytes  int                     // 单条消息最大字节数
// 	Compression      sarama.CompressionCodec // 压缩算法 (None, GZIP, Snappy, LZ4, ZSTD)
// 	CompressionLevel int                     // 压缩级别 (仅对 GZIP 有效)

// 	// 重试配置
// 	RetryMax     int           // 最大重试次数
// 	RetryBackoff time.Duration // 重试间隔时间

// 	// 批量配置
// 	BatchSize    int           // 批量发送大小 (字节)
// 	BatchTimeout time.Duration // 批量发送超时时间

// 	// 缓冲区配置
// 	BufferSize int // 发送缓冲区大小

// 	// 幂等性配置
// 	Idempotent bool // 是否启用幂等性 (需要 acks=all)

// 	// 分区策略
// 	Partitioner sarama.PartitionerConstructor // 分区策略构造函数

// 	// 日志配置
// 	LogLevel string // 日志级别 (debug, info, warn, error)
// }

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

// SendWithContext 带上下文的发送
func (p *Producer) SendWithContext(ctx context.Context, topic, key string, value []byte) error {
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
