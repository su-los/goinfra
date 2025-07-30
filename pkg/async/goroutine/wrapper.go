// Package goroutine 提供 goroutine 包装器，用于启动安全的 goroutine
package goroutine

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Go 启动一个安全的 goroutine
func Go(ctx context.Context, fn func() error, opts ...Option) {
	goOpt := defaultOptions()
	for _, o := range opts {
		o(goOpt)
	}

	go func() {
		defer func() {
			if goOpt.OnComplete != nil {
				goOpt.OnComplete()
			}
		}()

		// panic 恢复
		if goOpt.EnableRecovery {
			defer func() {
				if r := recover(); r != nil {
					err := errors.Errorf("goroutine panic: %v\n%s", r, debug.Stack())
					if goOpt.Logger != nil {
						goOpt.Logger.Error("goroutine panic recovered",
							zap.Any("panic", r),
							zap.String("stack", string(debug.Stack())))
					}
					if goOpt.OnError != nil {
						goOpt.OnError(err)
					}
				}
			}()
		}

		// 超时控制
		if goOpt.EnableTimeout {
			done := make(chan error, 1)
			go func() {
				done <- fn()
			}()

			select {
			case err := <-done:
				if err != nil && goOpt.OnError != nil {
					goOpt.OnError(err)
				}
			case <-time.After(goOpt.Timeout):
				err := fmt.Errorf("goroutine timeout after %v", goOpt.Timeout)
				if goOpt.Logger != nil {
					goOpt.Logger.Error("goroutine timeout",
						zap.Duration("timeout", goOpt.Timeout))
				}
				if goOpt.OnError != nil {
					goOpt.OnError(err)
				}
			}
		} else {
			// 直接执行
			if err := fn(); err != nil {
				if goOpt.Logger != nil {
					goOpt.Logger.Error("goroutine error", zap.Error(err))
				}
				if goOpt.OnError != nil {
					goOpt.OnError(err)
				}
			}
		}
	}()
}

// GoWithContext 启动一个带上下文的 goroutine
func GoWithContext(ctx context.Context, fn func(context.Context) error, opts ...Option) {
	Go(ctx, func() error {
		return fn(ctx)
	}, opts...)
}

// GoSimple 启动一个简单的 goroutine（无返回值）
func GoSimple(ctx context.Context, fn func(), opts ...Option) {
	Go(ctx, func() error {
		fn()
		return nil
	}, opts...)
}
