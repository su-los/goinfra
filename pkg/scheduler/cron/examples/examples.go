//go:build examples
// +build examples

// 基于 robfig/cron 的 cron 调度器，特点：
// 1. 支持秒级调度
// 2. 支持任务超时、重入、并发控制（robfig/cron 所有任务都会重入）
// Package examples 提供不同场景的使用示例
package examples

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/su-los/goinfra/pkg/scheduler/cron"
)

// ExampleDifferentScenarios 不同场景的使用示例
func ExampleDifferentScenarios() {
	scheduler := cron.NewCronScheduler()
	defer scheduler.Stop()

	scheduler.Start()

	// 场景1: 数据清理任务 - 不允许重入，有超时
	cleanupJob := func(ctx context.Context) {
		fmt.Println("开始数据清理...")
		// 模拟清理过程
		time.Sleep(5 * time.Second)
		fmt.Println("数据清理完成")
	}
	scheduler.RegisterCronFunc(context.Background(), "data_cleanup", "0 2 * * * *", cleanupJob, cron.WithJobTimeout(30*time.Minute), cron.WithJobAllowReentrant(false), cron.WithJobMaxConcurrent(1))

	// 场景2: 监控任务 - 允许重入，有超时
	monitorJob := func(ctx context.Context) {
		fmt.Println("执行系统监控检查...")
		// 模拟监控检查
		time.Sleep(2 * time.Second)
		fmt.Println("监控检查完成")
	}
	err := scheduler.RegisterCronFunc(context.Background(), "system_monitor", "*/30 * * * * *", monitorJob, cron.WithJobTimeout(10*time.Second), cron.WithJobAllowReentrant(true), cron.WithJobMaxConcurrent(3))
	if err != nil {
		log.Printf("注册任务失败: %v", err)
	}

	// 场景3: 批处理任务 - 不允许重入，无超时
	batchJob := func(ctx context.Context) {
		fmt.Println("开始批处理...")
		// 模拟长时间批处理
		time.Sleep(10 * time.Second)
		fmt.Println("批处理完成")
	}
	err = scheduler.RegisterCronFunc(context.Background(), "batch_processing", "0 0 3 * * *", batchJob, cron.WithJobTimeout(0), cron.WithJobAllowReentrant(false), cron.WithJobMaxConcurrent(1))
	if err != nil {
		log.Printf("注册任务失败: %v", err)
	}

	// 场景4: 数据采集任务 - 允许重入，无并发限制
	dataCollectionJob := func(ctx context.Context) {
		fmt.Println("采集数据...")
		// 模拟数据采集
		time.Sleep(1 * time.Second)
		fmt.Println("数据采集完成")
	}
	err = scheduler.RegisterCronFunc(context.Background(), "data_collection", "*/10 * * * * *", dataCollectionJob, cron.WithJobTimeout(5*time.Second), cron.WithJobAllowReentrant(true), cron.WithJobMaxConcurrent(0))
	if err != nil {
		log.Printf("注册任务失败: %v", err)
	}

	// 运行一段时间观察效果
	time.Sleep(60 * time.Second)
}

// ExampleTimeoutHandling 超时处理示例
//
// 需要在传入的 handler 中处理 ctx.Done() 的逻辑，超时后，上一个 handler 依然会继续执行。
func ExampleTimeoutHandling() {
	scheduler := cron.NewCronScheduler()
	defer scheduler.Stop()

	scheduler.Start()

	// 创建一个会超时的任务
	timeoutJob := func(ctx context.Context) {
		fmt.Println("开始执行可能超时的任务...")

		// 模拟长时间操作
		select {
		case <-time.After(5 * time.Second):
			fmt.Println("任务正常完成")
		case <-ctx.Done():
			fmt.Println("任务被超时中断")
		}
	}
	err := scheduler.RegisterCronFunc(context.Background(), "timeout_test", "*/15 * * * * *", timeoutJob, cron.WithJobTimeout(3*time.Second), cron.WithJobAllowReentrant(false), cron.WithJobMaxConcurrent(1))
	if err != nil {
		log.Printf("注册任务失败: %v", err)
	}

	time.Sleep(30 * time.Second)
}

// ExampleReentrantBehavior 重入行为示例
func ExampleReentrantBehavior() {
	scheduler := cron.NewCronScheduler()
	defer scheduler.Stop()

	scheduler.Start()

	// 创建一个不允许重入的任务
	nonReentrantJob := func(ctx context.Context) {
		fmt.Println("非重入任务开始执行...")
		time.Sleep(6 * time.Second) // 任务需要6秒
		fmt.Println("非重入任务完成")
	}
	err := scheduler.RegisterCronFunc(context.Background(), "non_reentrant", "*/5 * * * * *", nonReentrantJob, cron.WithJobTimeout(8*time.Second), cron.WithJobAllowReentrant(false), cron.WithJobMaxConcurrent(1))
	if err != nil {
		log.Printf("注册任务失败: %v", err)
	}

	// 创建一个允许重入的任务
	reentrantJob := func(ctx context.Context) {
		fmt.Println("重入任务开始执行...")
		time.Sleep(1 * time.Second)
		fmt.Println("重入任务完成")
	}
	err = scheduler.RegisterCronFunc(context.Background(), "reentrant", "*/3 * * * * *", reentrantJob, cron.WithJobTimeout(2*time.Second), cron.WithJobAllowReentrant(true), cron.WithJobMaxConcurrent(5))
	if err != nil {
		log.Printf("注册任务失败: %v", err)
	}

	time.Sleep(30 * time.Second)
}

// ExampleJobManagement 任务管理示例
func ExampleJobManagement() {
	scheduler := cron.NewCronScheduler()
	defer scheduler.Stop()

	scheduler.Start()

	// 添加任务
	handler := func(ctx context.Context) {
		fmt.Println("测试任务执行中...")
		time.Sleep(2 * time.Second)
	}

	err := scheduler.RegisterCronFunc(context.Background(), "test_job", "*/10 * * * * *", handler, cron.WithJobTimeout(5*time.Second), cron.WithJobAllowReentrant(false), cron.WithJobMaxConcurrent(1))
	if err != nil {
		log.Printf("注册任务失败: %v", err)
	}

	// 获取任务信息
	jobInfo, err := scheduler.GetJobInfo("test_job")
	if err != nil {
		log.Printf("获取任务信息失败: %v", err)
	} else {
		fmt.Printf("任务信息: ID=%s, 下次运行=%v\n",
			jobInfo.Config.ID, jobInfo.NextRun)
	}

	// 获取所有任务
	allJobs := scheduler.GetAllJobs()
	fmt.Printf("当前共有 %d 个任务\n", len(allJobs))

	// 运行一段时间后移除任务
	time.Sleep(20 * time.Second)

	err = scheduler.RemoveCronFunc("test_job")
	if err != nil {
		log.Printf("移除任务失败: %v", err)
	} else {
		fmt.Println("任务已移除")
	}
}
