// Package slicepipe 提供了一个通用的切片并发处理管道，可以将一个数据切片分发到多个协程中进行并发处理。
package slicepipe

import (
	"context"

	"github.com/go-kratos/kratos/pkg/sync/errgroup"
	"github.com/pkg/errors"

	"github.com/su-los/goinfra/pkg/async/goroutine"
)

func SlicePipeline[T any](
	ctx context.Context,
	datas []T, // 需要并发处理的数据切片
	workerCount int, // 并发协程数
	handler func(ctx context.Context, data T) error, // 每个数据的处理函数
) error {
	done := make(chan struct{})
	defer close(done)

	jobs := distribute(ctx, done, datas, workerCount)

	var errgp errgroup.Group
	for range workerCount {
		errgp.Go(func(ctx context.Context) error {
			return wrapHandler(ctx, done, jobs, handler)
		})
	}

	if err := errgp.Wait(); err != nil {
		return errors.Wrap(err, "SlicePipeline failed")
	}

	return nil
}

// distribute 函数将数据切片中的数据分发到一个通道中，以便多个协程可以并发地从该通道中接收数据进行处理。
func distribute[T any](
	ctx context.Context,
	done <-chan struct{},
	datas []T,
	workerCount int,
) <-chan T {
	// 通过将 workerCount 作为通道的缓冲区大小，限制执行任务的并发协程的数量
	jobs := make(chan T, workerCount)
	goroutine.Go(ctx, func() error {
		defer close(jobs)
		for i := range datas {
			select {
			case jobs <- datas[i]:
			case <-done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return nil
	}, nil)

	return jobs
}

// wrapHandler 包装处理函数，监听 done 和 ctx.Done 信号，优雅退出.
func wrapHandler[T any](
	ctx context.Context,
	done <-chan struct{},
	jobs <-chan T,
	handler func(ctx context.Context, data T) error,
) error {
	for {
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case job, ok := <-jobs:
			if !ok {
				return nil
			}

			if err := handler(ctx, job); err != nil {
				return err
			}
		}
	}
}
