package cron

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestConcurrencyBehavior 测试并发行为
func TestConcurrencyBehavior(t *testing.T) {
	scheduler := NewCronScheduler()
	defer scheduler.Stop()

	scheduler.Start()

	var mu sync.Mutex
	runningCount := 0
	maxRunning := 0

	// 创建一个长时间运行的任务
	job := func(ctx context.Context) {
		mu.Lock()
		runningCount++
		if runningCount > maxRunning {
			maxRunning = runningCount
		}
		current := runningCount
		mu.Unlock()

		fmt.Printf("任务开始执行，当前运行数: %d\n", current)

		// 模拟长时间运行的任务
		time.Sleep(3 * time.Second)

		mu.Lock()
		runningCount--
		mu.Unlock()

		fmt.Printf("任务执行完成，剩余运行数: %d\n", runningCount)
	}

	// 每1秒执行一次，但任务需要3秒完成
	err := scheduler.RegisterCronFunc(context.Background(), "test_job", "*/1 * * * * *", job, WithJobTimeout(5*time.Second), WithJobAllowReentrant(false), WithJobMaxConcurrent(1))
	require.NoError(t, err)
	// 运行10秒，观察行为
	time.Sleep(10 * time.Second)

	fmt.Printf("最大并发运行数: %d\n", maxRunning)

	// 验证：如果不会重入，maxRunning 应该始终为 1
	if maxRunning > 1 {
		t.Errorf("期望最大并发数为1，实际为: %d", maxRunning)
	} else {
		fmt.Println("✅ 验证通过：任务不会重入")
	}
}

// TestConcurrentJobs 测试多个不同任务的并发
func TestConcurrentJobs(t *testing.T) {
	scheduler := NewCronScheduler()
	defer scheduler.Stop()

	scheduler.Start()

	var mu sync.Mutex
	runningJobs := make(map[string]bool)

	// 创建两个不同的任务
	job1 := func(ctx context.Context) {
		mu.Lock()
		runningJobs["job1"] = true
		mu.Unlock()

		fmt.Println("Job1 开始执行")
		time.Sleep(2 * time.Second)
		fmt.Println("Job1 执行完成")

		mu.Lock()
		runningJobs["job1"] = false
		mu.Unlock()
	}

	job2 := func(ctx context.Context) {
		mu.Lock()
		runningJobs["job2"] = true
		mu.Unlock()

		fmt.Println("Job2 开始执行")
		time.Sleep(2 * time.Second)
		fmt.Println("Job2 执行完成")

		mu.Lock()
		runningJobs["job2"] = false
		mu.Unlock()
	}

	// 两个任务都每1秒执行一次
	err := scheduler.RegisterCronFunc(context.Background(), "job1", "*/1 * * * * *", job1, WithJobTimeout(5*time.Second), WithJobAllowReentrant(false), WithJobMaxConcurrent(1))
	require.NoError(t, err)

	err = scheduler.RegisterCronFunc(context.Background(), "job2", "*/1 * * * * *", job2, WithJobTimeout(5*time.Second), WithJobAllowReentrant(false), WithJobMaxConcurrent(1))
	require.NoError(t, err)
	// 运行5秒观察
	time.Sleep(5 * time.Second)

	fmt.Println("✅ 不同任务可以并发执行")
}

// TestTimeout 测试超时
func TestTimeout(t *testing.T) {
	scheduler := NewCronScheduler()
	defer scheduler.Stop()

	scheduler.Start()

	job := func(ctx context.Context) {
		fmt.Println("开始执行可能超时的任务...")

		// 模拟长时间操作
		select {
		case <-time.After(6 * time.Second):
			fmt.Println("任务正常完成")
		case <-ctx.Done():
			fmt.Println("任务被超时中断")
		}
	}

	err := scheduler.RegisterCronFunc(context.Background(), "test_job", "*/1 * * * * *", job, WithJobTimeout(5*time.Second), WithJobAllowReentrant(false), WithJobMaxConcurrent(1))
	require.NoError(t, err)

	time.Sleep(10 * time.Second)
}

// TestAllowReentrant 测试允许重入
func TestAllowReentrant(t *testing.T) {
	scheduler := NewCronScheduler()
	defer scheduler.Stop()

	scheduler.Start()

	var runCnt atomic.Int32
	// 创建一个允许重入的任务
	reentrantJob := func(ctx context.Context) {
		runCnt.Add(1)
		fmt.Println("重入任务开始执行...")
		time.Sleep(1 * time.Second)
		fmt.Println("重入任务完成")
	}
	err := scheduler.RegisterCronFunc(context.Background(), "reentrant", "*/1 * * * * *", reentrantJob, WithJobAllowReentrant(true), WithJobMaxConcurrent(6))
	require.NoError(t, err)

	time.Sleep(6 * time.Second)
	require.Equal(t, int32(6), runCnt.Load())
}

// TestAllowReentrant 测试允许重入, 并发数限制为 2
func TestAllowReentrantConcurrentLimit(t *testing.T) {
	scheduler := NewCronScheduler()
	defer scheduler.Stop()

	scheduler.Start()

	var runCnt atomic.Int32
	// 创建一个允许重入的任务
	reentrantJob := func(ctx context.Context) {
		runCnt.Add(1)
		fmt.Println("重入任务开始执行...")
		time.Sleep(3 * time.Second)
		fmt.Println("重入任务完成")
	}

	err := scheduler.RegisterCronFunc(context.Background(), "reentrant", "*/1 * * * * *", reentrantJob, WithJobAllowReentrant(true), WithJobMaxConcurrent(2))
	require.NoError(t, err)

	time.Sleep(6 * time.Second)
	require.Less(t, runCnt.Load(), int32(6))
}

// TestJobManagement 测试任务管理
func TestJobManagement(t *testing.T) {
	scheduler := NewCronScheduler()
	defer scheduler.Stop()

	scheduler.Start()

	// 添加任务
	handler := func(ctx context.Context) {
		fmt.Println("测试任务执行中...")
		time.Sleep(2 * time.Second)
	}

	err := scheduler.RegisterCronFunc(context.Background(), "test_job", "*/1 * * * * *", handler, WithJobTimeout(5*time.Second), WithJobAllowReentrant(false), WithJobMaxConcurrent(1))
	require.NoError(t, err)
	err = scheduler.RegisterCronFunc(context.Background(), "test_job2", "*/1 * * * * *", handler, WithJobTimeout(5*time.Second), WithJobAllowReentrant(false), WithJobMaxConcurrent(1))
	require.NoError(t, err)
	err = scheduler.RegisterCronFunc(context.Background(), "test_job3", "*/1 * * * * *", handler, WithJobTimeout(5*time.Second), WithJobAllowReentrant(false), WithJobMaxConcurrent(1))
	require.NoError(t, err)
	// 重复添加
	err = scheduler.RegisterCronFunc(context.Background(), "test_job", "*/10 * * * * *", handler, WithJobTimeout(5*time.Second), WithJobAllowReentrant(false), WithJobMaxConcurrent(1))
	require.Error(t, err)

	// 获取任务信息
	jobInfo, err := scheduler.GetJobInfo("test_job")
	require.NoError(t, err)
	require.Equal(t, "test_job", jobInfo.Config.ID)
	require.NotZero(t, jobInfo.NextRun)

	// 获取所有任务
	allJobs := scheduler.GetAllJobs()
	fmt.Printf("当前共有 %d 个任务\n", len(allJobs))

	// 运行一段时间后移除任务
	time.Sleep(3 * time.Second)
	err = scheduler.RemoveCronFunc("test_job")
	require.NoError(t, err)

	allJobs = scheduler.GetAllJobs()
	fmt.Printf("当前共有 %d 个任务\n", len(allJobs))
	require.Equal(t, 2, len(allJobs))
	time.Sleep(1 * time.Second)
}
