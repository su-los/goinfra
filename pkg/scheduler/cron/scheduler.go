// Package cron 提供基于成熟的第三方 cron 库的 cron 调度器封装
package cron

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
)

// JobConfig 任务配置
type JobConfig struct {
	ID             string        // 任务ID
	Schedule       string        // 调度表达式
	Handler        CronHandler   // 任务处理函数
	Timeout        time.Duration // 超时时间，0表示不超时
	AllowReentrant bool          // 是否允许重入
	MaxConcurrent  int           // 最大并发数，0表示不限制
}

type JobOption func(job *JobConfig)

// WithJobTimeout 设置任务超时时间
func WithJobTimeout(timeout time.Duration) JobOption {
	return func(job *JobConfig) {
		job.Timeout = timeout
	}
}

// WithJobAllowReentrant 设置任务是否允许重入
func WithJobAllowReentrant(allowReentrant bool) JobOption {
	return func(job *JobConfig) {
		job.AllowReentrant = allowReentrant
	}
}

// WithJobMaxConcurrent 设置任务最大并发数
func WithJobMaxConcurrent(maxConcurrent int) JobOption {
	return func(job *JobConfig) {
		job.MaxConcurrent = maxConcurrent
	}
}

// CronScheduler Cron 任务调度器
type CronScheduler struct {
	cron   *cron.Cron
	jobs   map[string]*JobInfo
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

// JobInfo 任务信息
type JobInfo struct {
	Config  *JobConfig
	EntryID cron.EntryID
	Running int32     // 当前运行数
	LastRun time.Time // 上次运行时间
	NextRun time.Time // 下次运行时间
}

// CronHandler 任务处理函数
type CronHandler func(ctx context.Context)

// NewCronScheduler 创建新的调度器
func NewCronScheduler() *CronScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &CronScheduler{
		cron:   cron.New(cron.WithSeconds()),
		jobs:   make(map[string]*JobInfo),
		ctx:    ctx,
		cancel: cancel,
	}
}

// RegisterJob 注册任务
func (s *CronScheduler) registerJob(ctx context.Context, config *JobConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 检查任务是否已存在
	if _, exists := s.jobs[config.ID]; exists {
		return errors.Errorf("job with ID %s already exists", config.ID)
	}

	// 创建任务处理器
	handler := s.createJobHandler(ctx, config)

	entryID, err := s.cron.AddFunc(config.Schedule, handler)
	if err != nil {
		return errors.Wrap(err, "failed to add job")
	}

	// 获取下次运行时间
	entry := s.cron.Entry(entryID)

	s.jobs[config.ID] = &JobInfo{
		Config:  config,
		EntryID: entryID,
		NextRun: entry.Next,
	}

	return nil
}

// createJobHandler 创建任务处理器
func (s *CronScheduler) createJobHandler(ctx context.Context, config *JobConfig) func() {
	return func() {
		jobInfo := s.jobs[config.ID]
		if jobInfo == nil {
			return
		}

		// 检查是否允许重入
		if !config.AllowReentrant {
			// 使用原子操作检查是否正在运行
			if !atomic.CompareAndSwapInt32(&jobInfo.Running, 0, 1) {
				log.Printf("Job %s is already running, skipping", config.ID)
				return
			}
			defer atomic.StoreInt32(&jobInfo.Running, 0)
		}

		// 检查最大并发数
		if config.AllowReentrant && config.MaxConcurrent > 0 {
			current := atomic.LoadInt32(&jobInfo.Running)
			if current >= int32(config.MaxConcurrent) {
				log.Printf("Job %s has reached max concurrent limit (%d), skipping", config.ID, config.MaxConcurrent)
				return
			}
			atomic.AddInt32(&jobInfo.Running, 1)
			defer atomic.AddInt32(&jobInfo.Running, -1)
		}

		// 更新运行时间
		jobInfo.LastRun = time.Now()

		// 执行任务
		if config.Timeout > 0 {
			// 带超时的执行
			// 注意：jobCtx 是 ctx 的子上下文，如果外部调用 ctx 的 cancel 函数，
			// jobCtx 也会收到取消信号，因为子上下文会继承父上下文的取消行为
			jobCtx, cancel := context.WithTimeout(ctx, config.Timeout)
			defer cancel()

			done := make(chan struct{}, 1)
			go func() {
				config.Handler(jobCtx)
				close(done)
			}()

			select {
			case <-done:
			case <-jobCtx.Done():
				log.Printf("Job %s timed out after %v", config.ID, config.Timeout)
			case <-s.ctx.Done():
				log.Printf("Scheduler stopped, Job %s cancelled", config.ID)
			}
		} else {
			// 无超时的执行
			config.Handler(ctx)
		}

		// 更新下次运行时间
		entry := s.cron.Entry(jobInfo.EntryID)
		jobInfo.NextRun = entry.Next
	}
}

// RegisterCronFunc 注册简单任务（向后兼容）
func (s *CronScheduler) RegisterCronFunc(ctx context.Context, jobName, schedule string, handler CronHandler, opts ...JobOption) error {
	config := &JobConfig{
		ID:             jobName,
		Schedule:       schedule,
		Handler:        handler,
		AllowReentrant: false, // 默认不允许重入
		MaxConcurrent:  1,     // 默认最大并发数为1
	}

	for _, opt := range opts {
		opt(config)
	}

	return s.registerJob(ctx, config)
}

// RemoveCronFunc 移除任务
func (s *CronScheduler) RemoveCronFunc(jobName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	jobInfo, exists := s.jobs[jobName]
	if !exists {
		return fmt.Errorf("job with ID %s not found", jobName)
	}

	s.cron.Remove(jobInfo.EntryID)
	delete(s.jobs, jobName)
	return nil
}

// GetJobInfo 获取任务信息
func (s *CronScheduler) GetJobInfo(jobID string) (*JobInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobInfo, exists := s.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job with ID %s not found", jobID)
	}

	return jobInfo, nil
}

// GetAllJobs 获取所有任务信息
func (s *CronScheduler) GetAllJobs() map[string]*JobInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make(map[string]*JobInfo)
	for jobID, jobInfo := range s.jobs {
		jobs[jobID] = jobInfo
	}
	return jobs
}

// Start 启动调度器
func (s *CronScheduler) Start() {
	s.cron.Start()
}

// Stop 停止调度器
func (s *CronScheduler) Stop() {
	s.cron.Stop()
	s.cancel()
}
