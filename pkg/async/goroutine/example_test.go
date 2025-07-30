package goroutine

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// ExampleUsage 使用示例
func ExampleUsage() {
	// 创建日志记录器
	logger, _ := zap.NewDevelopment()

	// 示例 1: 基本使用
	ctx := context.Background()
	Go(ctx, func() error {
		fmt.Println("执行任务 1")
		return nil
	}, WithRecovery(true),
		WithTimeout(5*time.Second),
		WithLogger(logger),
		WithErrorHandler(func(err error) {
			logger.Error("goroutine error", zap.Error(err))
		}),
		WithCompleteHandler(func() {
			logger.Info("goroutine completed")
		}))

	// 示例 2: 带错误的函数
	Go(ctx, func() error {
		fmt.Println("执行任务 2")
		return fmt.Errorf("模拟错误")
	}, WithRecovery(true),
		WithTimeout(5*time.Second),
		WithLogger(logger),
		WithErrorHandler(func(err error) {
			logger.Error("goroutine error", zap.Error(err))
		}),
		WithCompleteHandler(func() {
			logger.Info("goroutine completed")
		}))

	// 示例 3: 会 panic 的函数
	Go(ctx, func() error {
		fmt.Println("执行任务 3")
		panic("模拟 panic")
	}, WithRecovery(true),
		WithTimeout(5*time.Second),
		WithLogger(logger),
		WithErrorHandler(func(err error) {
			logger.Error("goroutine error", zap.Error(err))
		}),
		WithCompleteHandler(func() {
			logger.Info("goroutine completed")
		}))

	// 示例 4: 超时的函数
	Go(ctx, func() error {
		fmt.Println("执行任务 4")
		time.Sleep(10 * time.Second) // 超过 5 秒超时
		return nil
	}, WithRecovery(true),
		WithTimeout(5*time.Second),
		WithLogger(logger),
		WithErrorHandler(func(err error) {
			logger.Error("goroutine error", zap.Error(err))
		}),
		WithCompleteHandler(func() {
			logger.Info("goroutine completed")
		}))

	// 示例 5: 使用 goroutine 池
	pool := NewPool(3, WithRecovery(true),
		WithTimeout(5*time.Second),
		WithLogger(logger),
		WithErrorHandler(func(err error) {
			logger.Error("goroutine error", zap.Error(err))
		}),
		WithCompleteHandler(func() {
			logger.Info("goroutine completed")
		}))
	defer pool.Close()

	for i := 0; i < 5; i++ {
		i := i
		pool.Submit(func() {
			fmt.Printf("池任务 %d\n", i)
		})
	}

	// 等待一段时间让所有 goroutine 完成
	time.Sleep(2 * time.Second)
}

// ExampleWithContext 带上下文的示例
func ExampleWithContext() {
	logger, _ := zap.NewDevelopment()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	GoWithContext(ctx, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			fmt.Println("上下文任务完成")
			return nil
		}
	}, WithRecovery(true),
		WithLogger(logger))

	time.Sleep(2 * time.Second)
}

// ExamplePool 池使用示例
func ExamplePool() {
	logger, _ := zap.NewDevelopment()

	// 创建池
	pool := NewPool(5, WithRecovery(true),
		WithLogger(logger),
		WithErrorHandler(func(err error) {
			logger.Error("pool error", zap.Error(err))
		}))
	defer pool.Close()

	// 提交任务
	for i := 0; i < 10; i++ {
		i := i
		pool.SubmitWithError(func() error {
			if i%3 == 0 {
				return fmt.Errorf("任务 %d 失败", i)
			}
			fmt.Printf("任务 %d 成功\n", i)
			return nil
		})
	}

	// 等待任务完成
	time.Sleep(1 * time.Second)

	fmt.Printf("池状态: 工作协程=%d, 队列大小=%d, 队列容量=%d\n",
		pool.WorkerCount(), pool.QueueSize(), pool.QueueCapacity())
}
