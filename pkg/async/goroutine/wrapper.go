// Package goroutine 提供 goroutine 包装器，用于启动安全的 goroutine
package goroutine

import (
	"context"
)

// Go 启动一个安全的 goroutine
func Go(ctx context.Context, fn func() error, opts ...Option) {
	goOpt := defaultOptions()
	for _, o := range opts {
		o(goOpt)
	}

	innerGo(ctx, fn, goOpt)
}

func innerGo(ctx context.Context, fn func() error, goOpt *goOptions) {
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
			defer goOpt.doRecovery()
			done <- fn()
		}()

		goOpt.doSelect(ctx, done)
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
