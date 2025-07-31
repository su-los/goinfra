package goroutine

import (
	"runtime/debug"

	"github.com/pkg/errors"
)

// Pool goroutine 池
type Pool struct {
	workerCount int
	jobQueue    chan func()
	opts        *goOptions
}

// NewPool 创建新的 goroutine 池
func NewPool(workerCount int, opts ...Option) *Pool {
	goOpt := defaultOptions()
	for _, o := range opts {
		o(goOpt)
	}

	pool := &Pool{
		workerCount: workerCount,
		jobQueue:    make(chan func(), workerCount*2),
		opts:        goOpt,
	}

	// 启动工作协程
	for range workerCount {
		go pool.worker()
	}

	return pool
}

// worker 工作协程
func (p *Pool) worker() {
	for job := range p.jobQueue {
		func() {
			defer func() {
				if p.opts.OnComplete != nil {
					p.opts.OnComplete()
				}
			}()

			// panic 恢复
			if p.opts.EnableRecovery {
				defer func() {
					if r := recover(); r != nil {
						err := errors.Errorf("worker panic: %v\n%s", r, debug.Stack())
						if p.opts.Logger != nil {
							p.opts.Logger.Printf("[ERROR] worker panic recovered: %v\nstack: %s", r, debug.Stack())
						}
						if p.opts.OnError != nil {
							p.opts.OnError(err)
						}
					}
				}()
			}

			// 执行任务
			job()
		}()
	}
}

// Submit 提交任务到池
func (p *Pool) Submit(job func()) {
	p.jobQueue <- job
}

// SubmitWithError 提交带错误返回的任务到池
func (p *Pool) SubmitWithError(job func() error) {
	p.jobQueue <- func() {
		if err := job(); err != nil {
			if p.opts.Logger != nil {
				p.opts.Logger.Printf("[ERROR] pool job error: %v", err)
			}
			if p.opts.OnError != nil {
				p.opts.OnError(err)
			}
		}
	}
}

// Close 关闭池
func (p *Pool) Close() {
	close(p.jobQueue)
}

// WorkerCount 获取工作协程数量
func (p *Pool) WorkerCount() int {
	return p.workerCount
}

// QueueSize 获取队列大小
func (p *Pool) QueueSize() int {
	return len(p.jobQueue)
}

// QueueCapacity 获取队列容量
func (p *Pool) QueueCapacity() int {
	return cap(p.jobQueue)
}
