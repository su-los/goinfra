package goroutine

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/pkg/errors"
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

func (o *goOptions) doRecovery() {
	if !o.EnableRecovery {
		return
	}

	if r := recover(); r != nil {
		err := errors.Errorf("goroutine panic: %v\n%s", r, debug.Stack())
		if o.Logger != nil {
			o.Logger.Error("goroutine panic recovered",
				zap.Any("panic", r),
				zap.String("stack", string(debug.Stack())))
		}
		if o.OnError != nil {
			o.OnError(err)
		}
	}
}

func (o *goOptions) doSelect(ctx context.Context, done <-chan error) {
	// 超时控制
	if o.EnableTimeout {
		select {
		case err := <-done:
			if err != nil && o.OnError != nil {
				o.OnError(err)
			}
		case <-time.After(o.Timeout):
			err := fmt.Errorf("goroutine timeout after %v", o.Timeout)
			if o.Logger != nil {
				o.Logger.Error("goroutine timeout",
					zap.Duration("timeout", o.Timeout))
			}
			if o.OnError != nil {
				o.OnError(err)
			}
		case <-ctx.Done():
			err := ctx.Err()
			if o.Logger != nil {
				o.Logger.Error("goroutine context done", zap.Error(err))
			}
			if o.OnError != nil {
				o.OnError(err)
			}
		}
	} else {
		// 直接执行
		select {
		case err := <-done:
			if err != nil && o.OnError != nil {
				o.OnError(err)
			}
		case <-ctx.Done():
			err := ctx.Err()
			if o.Logger != nil {
				o.Logger.Error("goroutine context done", zap.Error(err))
			}
			if o.OnError != nil {
				o.OnError(err)
			}
		}
	}
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
