package goroutine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestGoWithRecovery(t *testing.T) {
	// 测试 panic 恢复
	Go(context.Background(), func() error {
		panic("test panic")
	}, WithRecovery(true),
		WithLogger(zap.NewNop()),
		WithErrorHandler(func(err error) {
			fmt.Println("error handler", err)
			require.Error(t, err)
		}))

	// 等待 goroutine 执行
	time.Sleep(100 * time.Millisecond)
}

func TestGoWithTimeout(t *testing.T) {
	// 测试超时
	Go(context.Background(), func() error {
		time.Sleep(200 * time.Millisecond)
		return nil
	}, WithTimeout(100*time.Millisecond), WithLogger(zap.NewNop()), WithErrorHandler(func(err error) {
		require.Error(t, err)
	}))

	// 等待超时
	time.Sleep(300 * time.Millisecond)

	errorCalled := false
	// 没有超时，正常返回成功
	Go(context.Background(), func() error {
		return nil
	}, WithTimeout(100*time.Millisecond), WithLogger(zap.NewNop()), WithErrorHandler(func(err error) {
		errorCalled = true
	}))

	// 等待 goroutine 执行
	time.Sleep(200 * time.Millisecond)
	require.False(t, errorCalled)

	// 没有超时，返回错误
	Go(context.Background(), func() error {
		return fmt.Errorf("test error")
	}, WithTimeout(100*time.Millisecond), WithLogger(zap.NewNop()), WithErrorHandler(func(err error) {
		errorCalled = true
	}))

	// 等待 goroutine 执行
	time.Sleep(200 * time.Millisecond)
	require.True(t, errorCalled)
}

func TestGoWithError(t *testing.T) {
	errorCalled := false
	// 测试错误处理
	Go(context.Background(), func() error {
		return fmt.Errorf("test error")
	}, WithLogger(zap.NewNop()), WithErrorHandler(func(err error) {
		errorCalled = true
	}))

	// 等待 goroutine 执行
	time.Sleep(100 * time.Millisecond)
	require.True(t, errorCalled)

	errorCalled = false
	// 带超时的错误处理
	Go(context.Background(), func() error {
		time.Sleep(100 * time.Millisecond)
		return fmt.Errorf("test error")
	}, WithTimeout(50*time.Millisecond), WithLogger(zap.NewNop()), WithErrorHandler(func(err error) {
		errorCalled = true
	}))

	time.Sleep(150 * time.Millisecond)
	require.True(t, errorCalled)
}

func TestGoWithCompleteHandler(t *testing.T) {
	completeCalled := false
	Go(context.Background(), func() error {
		return nil
	}, WithLogger(zap.NewNop()), WithCompleteHandler(func() {
		completeCalled = true
	}))

	time.Sleep(100 * time.Millisecond)
	require.True(t, completeCalled)
}

func TestGoWithContext(t *testing.T) {
	completeCalled := false
	GoWithContext(context.Background(), func(ctx context.Context) error {
		return nil
	}, WithLogger(zap.NewNop()), WithCompleteHandler(func() {
		completeCalled = true
	}))

	time.Sleep(100 * time.Millisecond)
	require.True(t, completeCalled)
}

func TestGoSimple(t *testing.T) {
	simpleCalled := false
	GoSimple(context.Background(), func() {
		simpleCalled = true
	}, WithLogger(zap.NewNop()))

	time.Sleep(100 * time.Millisecond)
	require.True(t, simpleCalled)
}

func TestGoWithContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	Go(ctx, func() error {
		return nil
	}, WithLogger(zap.NewNop()), WithErrorHandler(func(err error) {
		fmt.Println("error handler", err)
		require.Error(t, err)
	}))

	cancel()

	time.Sleep(100 * time.Millisecond)

	// 带超时
	Go(ctx, func() error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}, WithTimeout(50*time.Millisecond), WithLogger(zap.NewNop()), WithErrorHandler(func(err error) {
		fmt.Println("error handler", err)
		require.Error(t, err)
	}))

	time.Sleep(150 * time.Millisecond)
}
