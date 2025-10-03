// Package goroutine 提供 goroutine 包装器，用于启动安全的 goroutine
package goroutine

import (
	"context"
)

// Go 启动一个安全的 goroutine.
func Go(ctx context.Context, handler func() error, opts ...Option) {
	goOpt := defaultOptions()
	for _, o := range opts {
		if o == nil {
			continue
		}
		o(goOpt)
	}

	innerGo(ctx, handler, goOpt)
}

func innerGo(ctx context.Context, handler func() error, goOpt *goOptions) {
	go func() {
		defer func() {
			if goOpt.OnComplete != nil {
				goOpt.OnComplete()
			}
		}()

		// panic 恢复
		defer goOpt.doRecovery()

		done := make(chan error, 1)
		go func() {
			// 确保在 doSelect 完成后关闭 done channel 以避免泄漏
			defer close(done)
			// 先恢复 panic，再 close done channel
			defer goOpt.doRecovery()
			done <- handler()
		}()

		goOpt.doSelect(ctx, done)
	}()
}

// GoWithContext 启动一个带上下文的 goroutine.
func GoWithContext(ctx context.Context, handler func(context.Context) error, opts ...Option) {
	Go(ctx, func() error {
		return handler(ctx)
	}, opts...)
}

// GoSimple 启动一个简单的 goroutine（无返回值）.
func GoSimple(ctx context.Context, handler func(), opts ...Option) {
	Go(ctx, func() error {
		handler()

		return nil
	}, opts...)
}
