package goroutine

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestPool(t *testing.T) {
	defer goleak.VerifyNone(t)
	errCalled := false
	stdLogger := log.New(os.Stdout, "", log.LstdFlags)
	pool := NewPool(2, WithRecovery(true), WithLogger(stdLogger), WithErrorHandler(func(err error) {
		errCalled = true
		require.Error(t, err)
	}))
	defer pool.Close()

	// 提交任务
	for i := range 5 {
		innerIdx := i
		pool.Submit(func() {
			if innerIdx%2 == 0 {
				panic("test panic")
			}
		})
	}

	// 等待任务执行
	time.Sleep(300 * time.Millisecond)
	require.True(t, errCalled)
}

func TestPool_OnComplete(t *testing.T) {
	defer goleak.VerifyNone(t)
	completeCalled := false
	stdLogger := log.New(os.Stdout, "", log.LstdFlags)
	pool := NewPool(2, WithRecovery(true), WithLogger(stdLogger), WithCompleteHandler(func() {
		completeCalled = true
	}))
	defer pool.Close()

	pool.Submit(func() {
		time.Sleep(100 * time.Millisecond)
	})

	time.Sleep(200 * time.Millisecond)
	require.True(t, completeCalled)
}

func TestPool_SubmitWithError(t *testing.T) {
	defer goleak.VerifyNone(t)
	errCalled := false
	stdLogger := log.New(os.Stdout, "", log.LstdFlags)
	pool := NewPool(2, WithRecovery(true), WithLogger(stdLogger), WithErrorHandler(func(err error) {
		errCalled = true
		require.Error(t, err)
	}))
	defer pool.Close()

	pool.SubmitWithError(func() error {
		return assert.AnError
	})

	time.Sleep(200 * time.Millisecond)
	require.True(t, errCalled)
}

// 测试多协程异步获取 workerCount、QueueSize、QueueCapacity
func TestPool_AsyncGet(t *testing.T) {
	defer goleak.VerifyNone(t)
	stdLogger := log.New(os.Stdout, "", log.LstdFlags)
	pool := NewPool(2, WithRecovery(true), WithLogger(stdLogger))
	defer pool.Close()

	const cnt = 120
	wg := sync.WaitGroup{}
	wg.Add(cnt)

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range cnt {
		innerIdx := i
		go func() {
			defer wg.Done()
			switch innerIdx % 3 {
			case 0:
				pool.Submit(func() {
					time.Sleep(time.Duration(rnd.Intn(100)) * time.Millisecond)
				})
			case 1:
				pool.SubmitWithError(func() error {
					if rnd.Int31n(100) > 50 {
						return assert.AnError
					}
					time.Sleep(time.Duration(rnd.Intn(100)) * time.Millisecond)
					return nil
				})
			case 2:
				fmt.Println("QueueSize", pool.QueueSize())
				// 初始化后，这两个属性不会被写
				pool.WorkerCount()
				pool.QueueCapacity()
			default:
				panic("test panic")
			}
		}()
	}
	wg.Wait()
}
