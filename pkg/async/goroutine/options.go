package goroutine

import (
	"time"

	"go.uber.org/zap"
)

// options goroutine 包装器选项
type goOptions struct {
	// 是否启用 panic 恢复
	EnableRecovery bool
	// 是否启用超时控制
	EnableTimeout bool
	// 超时时间
	Timeout time.Duration
	// 日志记录器
	Logger *zap.Logger
	// 错误回调函数
	OnError func(error)
	// 完成回调函数
	OnComplete func()
}

// defaultOptions 默认选项
func defaultOptions() *goOptions {
	return &goOptions{
		EnableRecovery: true,
		EnableTimeout:  false,
		Timeout:        30 * time.Second,
		Logger:         nil,
		OnError:        nil,
		OnComplete:     nil,
	}
}

type Option func(*goOptions)

// WithRecovery 启用 panic 恢复
func WithRecovery(enable bool) Option {
	return func(o *goOptions) {
		o.EnableRecovery = enable
	}
}

// WithTimeout 启用超时控制
func WithTimeout(timeout time.Duration) Option {
	return func(o *goOptions) {
		o.EnableTimeout = true
		o.Timeout = timeout
	}
}

// WithLogger 设置日志记录器
func WithLogger(logger *zap.Logger) Option {
	return func(o *goOptions) {
		o.Logger = logger
	}
}

// WithErrorHandler 设置错误处理函数
func WithErrorHandler(handler func(error)) Option {
	return func(o *goOptions) {
		o.OnError = handler
	}
}

// WithCompleteHandler 设置完成处理函数
func WithCompleteHandler(handler func()) Option {
	return func(o *goOptions) {
		o.OnComplete = handler
	}
}
