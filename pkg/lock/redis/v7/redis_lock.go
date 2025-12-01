// Package redis 实现 Redis 分布式锁（v7版本）
package redis

import (
	"context"
	"sync"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v7"
	"github.com/pkg/errors"
)

// RedisLock 带自动续期的 Redis 锁实现
type RedisLock struct {
	client      *redislock.Client
	locker      *redislock.Lock
	key         string
	expiry      time.Duration
	renewPeriod time.Duration
	needDog     bool // 是否需要 WatchDog 自动续期
	stopChan    chan struct{}
	wg          sync.WaitGroup
}

type RedisLockOption func(*RedisLock) *RedisLock

func NewRedisLock(client *redis.Client, key string, expiry time.Duration, opts ...RedisLockOption) *RedisLock {
	lock := &RedisLock{
		client:      redislock.New(client),
		key:         key,
		expiry:      expiry,
		renewPeriod: expiry / 3, // 续期间隔为过期时间的 1/3
		stopChan:    make(chan struct{}),
	}
	for _, opt := range opts {
		opt(lock)
	}
	return lock
}

// WithWatchDog 设置自动续期，默认不开启自动续期
func WithWatchDog(lock *RedisLock) *RedisLock {
	lock.needDog = true
	return lock
}

// Lock 获取锁并启动看门狗
//
// Note  行为未知：多次短时间内重复调用
func (l *RedisLock) Lock(ctx context.Context) error {
	if l.locker != nil {
		return nil // 锁已被获取，无需重复获取
	}

	// 尝试获取锁
	ttl := l.expiry
	opt := &redislock.Options{
		RetryStrategy: redislock.ExponentialBackoff(100*time.Millisecond, 1*time.Second),
	}
	lock, err := l.client.Obtain(l.key, ttl, opt)
	if err != nil {
		return errors.Wrap(err, "failed to obtain redis lock")
	}
	l.locker = lock

	if l.needDog {
		// 成功获取锁，启动看门狗
		l.startWatchdog(ctx)
	}
	return nil
}

// Unlock 释放锁并停止看门狗
func (l *RedisLock) Unlock(ctx context.Context) error {
	if l.locker == nil {
		return errors.New("lock is not acquired")
	}

	if l.needDog {
		// 停止 WatchDog
		close(l.stopChan)
		l.wg.Wait()
	}

	// 释放锁
	err := l.locker.Release()
	if err != nil {
		return errors.WithStack(err)
	}
	l.locker = nil // 清空锁对象
	return nil
}

// 启动看门狗后台续期
func (l *RedisLock) startWatchdog(ctx context.Context) {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()

		ticker := time.NewTicker(l.renewPeriod)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// 续期锁
				err := l.locker.Refresh(l.renewPeriod, nil)
				if err != nil {
					return // 续期失败，退出
				}
			case <-ctx.Done():
				return // 上下文取消，退出
			case <-l.stopChan:
				return // 收到停止信号，退出
			}
		}
	}()
}
