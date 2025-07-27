package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"time"

	"github.com/Shopify/sarama"
)

/*
基础配置
- Brokers []string // Kafka 集群地址列表

连接配置
- ClientID     string        // 客户端标识
- DialTimeout  time.Duration // 连接超时时间
- ReadTimeout  time.Duration // 读取超时时间
- WriteTimeout time.Duration // 写入超时时间

认证配置
- Username  string // SASL 用户名
- Password  string // SASL 密码
- Mechanism string // SASL 机制 (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)

TLS 配置
- TLSEnabled  bool   // 是否启用 TLS
- TLSCAFile   string // CA 证书文件路径
- TLSCertFile string // 客户端证书文件路径
- TLSKeyFile  string // 客户端私钥文件路径

生产者配置
- RequiredAcks     sarama.RequiredAcks     // 确认机制 (NoResponse, WaitForLocal, WaitForAll)
- MaxMessageBytes  int                     // 单条消息最大字节数
- Compression      sarama.CompressionCodec // 压缩算法 (None, GZIP, Snappy, LZ4, ZSTD)
- CompressionLevel int                     // 压缩级别 (仅对 GZIP 有效)
- RetryMax     int           // 最大重试次数
- RetryBackoff time.Duration // 重试间隔时间
- BatchSize    int           // 批量发送大小 (字节)
- BatchTimeout time.Duration // 批量发送超时时间
- BufferSize int // 发送缓冲区大小

- Idempotent bool // 是否启用幂等性 (需要 acks=all)
- Partitioner sarama.PartitionerConstructor // 分区策略构造函数

消费者组配置
- ConsumerGroupRebalanceStrategy sarama.BalanceStrategy // 消费者组重平衡策略
- ConsumerOffsetsInitial int64 // 消费者偏移量初始位置
- ConsumerOffsetsAutoCommit bool // 是否自动提交偏移量
- ConsumerOffsetsAutoCommitInterval time.Duration // 自动提交间隔
- ConsumerMaxWaitTime time.Duration // 消费者最大等待时间
- ConsumerMaxProcessingTime time.Duration // 消费者最大处理时间
- ConsumerFetchMin int32 // 消费者最小获取字节数
- ConsumerFetchDefault int32 // 消费者默认获取字节数
- ConsumerFetchMax int32 // 消费者最大获取字节数
- ConsumerRetryBackoff time.Duration // 消费者重试间隔
*/

// ConfigOption 配置选项
type ConfigOption func(config *sarama.Config)

// WithClientID 设置客户端标识
func WithClientID(clientID string) ConfigOption {
	return func(config *sarama.Config) {
		config.ClientID = clientID
	}
}

// WithDialTimeout 设置连接超时时间
func WithDialTimeout(dialTimeout time.Duration) ConfigOption {
	return func(config *sarama.Config) {
		config.Net.DialTimeout = dialTimeout
	}
}

// WithReadTimeout 设置读取超时时间
func WithReadTimeout(readTimeout time.Duration) ConfigOption {
	return func(config *sarama.Config) {
		config.Net.ReadTimeout = readTimeout
	}
}

// WithWriteTimeout 设置写入超时时间
func WithWriteTimeout(writeTimeout time.Duration) ConfigOption {
	return func(config *sarama.Config) {
		config.Net.WriteTimeout = writeTimeout
	}
}

// WithUsername 设置 SASL 用户名
func WithUsername(username string) ConfigOption {
	return func(config *sarama.Config) {
		config.Net.SASL.User = username
	}
}

// WithPassword 设置 SASL 密码
func WithPassword(password string) ConfigOption {
	return func(config *sarama.Config) {
		config.Net.SASL.Password = password
	}
}

// WithMechanism 设置 SASL 机制
func WithMechanism(mechanism sarama.SASLMechanism) ConfigOption {
	return func(config *sarama.Config) {
		config.Net.SASL.Mechanism = mechanism
	}
}

// WithTLSEnabled 设置 TLS 是否启用
func WithTLSEnabled(enabled bool) ConfigOption {
	return func(config *sarama.Config) {
		config.Net.TLS.Enable = enabled
	}
}

// WithTLSCAFile 设置 TLS CA 证书文件
func WithTLSCAFile(caFile string) ConfigOption {
	return func(config *sarama.Config) {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			panic(err)
		}
		config.Net.TLS.Config.RootCAs = x509.NewCertPool()
		config.Net.TLS.Config.RootCAs.AppendCertsFromPEM(caCert)
	}
}

// WithTLSCertFile 设置 TLS 客户端证书文件路径
func WithTLSCertFile(certFile, keyFile string) ConfigOption {
	return func(config *sarama.Config) {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			panic(err)
		}
		config.Net.TLS.Config.Certificates = []tls.Certificate{cert}
	}
}

// WithTLSKeyFile 设置 TLS 客户端私钥文件路径
func WithTLSKeyFile(certFile, keyFile string) ConfigOption {
	return func(config *sarama.Config) {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			panic(err)
		}
		config.Net.TLS.Config.Certificates = []tls.Certificate{cert}
	}
}

// WithProducerRequiredAcks 设置生产者确认机制
func WithProducerRequiredAcks(requiredAcks sarama.RequiredAcks) ConfigOption {
	return func(config *sarama.Config) {
		config.Producer.RequiredAcks = requiredAcks
	}
}

// WithProducerMaxMessageBytes 设置生产者单条消息最大字节数
func WithProducerMaxMessageBytes(maxMessageBytes int) ConfigOption {
	return func(config *sarama.Config) {
		config.Producer.MaxMessageBytes = maxMessageBytes
	}
}

// WithProducerCompression 设置生产者压缩算法
func WithProducerCompression(compression sarama.CompressionCodec) ConfigOption {
	return func(config *sarama.Config) {
		config.Producer.Compression = compression
	}
}

// WithProducerCompressionLevel 设置生产者压缩级别，针对 GZIP 有效
func WithProducerCompressionLevel(compressionLevel int) ConfigOption {
	return func(config *sarama.Config) {
		config.Producer.CompressionLevel = compressionLevel
	}
}

// WithProducerRetryMax 设置生产者重试最大次数
func WithProducerRetryMax(retryMax int) ConfigOption {
	return func(config *sarama.Config) {
		config.Producer.Retry.Max = retryMax
	}
}

// WithProducerRetryBackoff 设置生产者重试间隔时间
func WithProducerRetryBackoff(retryBackoff time.Duration) ConfigOption {
	return func(config *sarama.Config) {
		config.Producer.Retry.Backoff = retryBackoff
	}
}

// WithProducerBatchSize 设置生产者批量发送大小 (字节)
func WithProducerBatchSize(batchSize int) ConfigOption {
	return func(config *sarama.Config) {
		config.Producer.Flush.Bytes = batchSize
	}
}

// WithProducerBatchTimeout 设置生产者批量发送超时时间
func WithProducerBatchTimeout(batchTimeout time.Duration) ConfigOption {
	return func(config *sarama.Config) {
		config.Producer.Flush.Frequency = batchTimeout
	}
}

// WithProducerBufferSize 设置生产者发送缓冲区大小
func WithProducerBufferSize(bufferSize int) ConfigOption {
	return func(config *sarama.Config) {
		config.Producer.Flush.Messages = bufferSize
	}
}

// WithProducerIdempotent 设置生产者幂等性
func WithProducerIdempotent(idempotent bool) ConfigOption {
	return func(config *sarama.Config) {
		config.Producer.Idempotent = idempotent
	}
}

// WithProducerPartitioner 设置生产者分区策略
func WithProducerPartitioner(partitioner sarama.PartitionerConstructor) ConfigOption {
	return func(config *sarama.Config) {
		config.Producer.Partitioner = partitioner
	}
}

// WithConsumerGroupRebalanceStrategy 设置消费者组重平衡策略
func WithConsumerGroupRebalanceStrategy(strategy sarama.BalanceStrategy) ConfigOption {
	return func(config *sarama.Config) {
		config.Consumer.Group.Rebalance.Strategy = strategy
	}
}

// WithConsumerOffsetsInitial 设置消费者偏移量初始位置
func WithConsumerOffsetsInitial(offset int64) ConfigOption {
	return func(config *sarama.Config) {
		config.Consumer.Offsets.Initial = offset
	}
}

// WithConsumerOffsetsAutoCommit 设置自动提交偏移量
func WithConsumerOffsetsAutoCommit(enable bool) ConfigOption {
	return func(config *sarama.Config) {
		config.Consumer.Offsets.AutoCommit.Enable = enable
	}
}

// WithConsumerOffsetsAutoCommitInterval 设置自动提交间隔
func WithConsumerOffsetsAutoCommitInterval(interval time.Duration) ConfigOption {
	return func(config *sarama.Config) {
		config.Consumer.Offsets.AutoCommit.Interval = interval
	}
}

// WithConsumerMaxWaitTime 设置消费者最大等待时间
func WithConsumerMaxWaitTime(waitTime time.Duration) ConfigOption {
	return func(config *sarama.Config) {
		config.Consumer.MaxWaitTime = waitTime
	}
}

// WithConsumerMaxProcessingTime 设置消费者最大处理时间
func WithConsumerMaxProcessingTime(processingTime time.Duration) ConfigOption {
	return func(config *sarama.Config) {
		config.Consumer.MaxProcessingTime = processingTime
	}
}

// WithConsumerFetchMin 设置消费者最小获取字节数
func WithConsumerFetchMin(minBytes int32) ConfigOption {
	return func(config *sarama.Config) {
		config.Consumer.Fetch.Min = minBytes
	}
}

// WithConsumerFetchDefault 设置消费者默认获取字节数
func WithConsumerFetchDefault(defaultBytes int32) ConfigOption {
	return func(config *sarama.Config) {
		config.Consumer.Fetch.Default = defaultBytes
	}
}

// WithConsumerFetchMax 设置消费者最大获取字节数
func WithConsumerFetchMax(maxBytes int32) ConfigOption {
	return func(config *sarama.Config) {
		config.Consumer.Fetch.Max = maxBytes
	}
}

// WithConsumerRetryBackoff 设置消费者重试间隔
func WithConsumerRetryBackoff(backoff time.Duration) ConfigOption {
	return func(config *sarama.Config) {
		config.Consumer.Retry.Backoff = backoff
	}
}
