package goroutine

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"time"

	"github.com/pkg/errors"
)

// Logger 用于记录错误信息以及关键信息到日志.
type Logger interface {
	Fatal(v ...any)
	Fatalf(format string, v ...any)
	Fatalln(v ...any)
	Print(v ...any)
	Printf(format string, v ...any)
	Println(v ...any)
}

// options goroutine 包装器选项.
type goOptions struct {
	// 是否启用 panic 恢复
	EnableRecovery bool
	// 是否启用超时控制
	EnableTimeout bool
	// 超时时间
	Timeout time.Duration
	// 日志记录器
	Logger Logger
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
			o.Logger.Printf("goroutine panic recovered: %v\nstack: %s", r, string(debug.Stack()))
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
		case err, ok := <-done:
			if !ok {
				// 外层发生了 panic，done channel 被提前关闭
				o.Logger.Printf("goroutine done channel closed")

				return
			}
			if err != nil && o.OnError != nil {
				o.OnError(err)
			}
		case <-time.After(o.Timeout):
			err := fmt.Errorf("goroutine timeout after %v", o.Timeout)
			o.Logger.Printf("goroutine timeout after %v", o.Timeout)
			if o.OnError != nil {
				o.OnError(err)
			}
		case <-ctx.Done():
			err := ctx.Err()
			if o.Logger != nil {
				o.Logger.Printf("goroutine context done: %v", err)
			}
			if o.OnError != nil {
				o.OnError(err)
			}
		}
	} else {
		// 直接执行
		select {
		case err, ok := <-done:
			// 外层发生了 panic，done channel 被提前关闭
			if !ok {
				o.Logger.Printf("goroutine done channel closed")

				return
			}
			if err != nil && o.OnError != nil {
				o.OnError(err)
			}
		case <-ctx.Done():
			err := ctx.Err()
			o.Logger.Printf("goroutine context done: %v", err)
			if o.OnError != nil {
				o.OnError(err)
			}
		}
	}
}

// defaultOptions 默认选项.
func defaultOptions() *goOptions {
	return &goOptions{
		EnableRecovery: true,
		EnableTimeout:  false,
		Timeout:        30 * time.Second,
		Logger:         log.New(os.Stderr, "", log.LstdFlags),
		OnError:        nil,
		OnComplete:     nil,
	}
}

type Option func(*goOptions)

// WithRecovery 启用 panic 恢复.
func WithRecovery(enable bool) Option {
	return func(o *goOptions) {
		o.EnableRecovery = enable
	}
}

// WithTimeout 启用超时控制.
func WithTimeout(timeout time.Duration) Option {
	return func(o *goOptions) {
		o.EnableTimeout = true
		o.Timeout = timeout
	}
}

// WithLogger 设置日志记录器.
func WithLogger(logger Logger) Option {
	return func(o *goOptions) {
		o.Logger = logger
	}
}

// WithErrorHandler 设置错误处理函数.
func WithErrorHandler(handler func(error)) Option {
	return func(o *goOptions) {
		o.OnError = handler
	}
}

// WithCompleteHandler 设置完成处理函数.
func WithCompleteHandler(handler func()) Option {
	return func(o *goOptions) {
		o.OnComplete = handler
	}
}
