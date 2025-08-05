// Package redis 实现 Redis 分布式锁(v8 版本)
package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// Lock Redis 分布式锁
type Lock struct {
	client     *redis.Client
	key        string
	expiration time.Duration
	value      string
}

// NewLock 创建新的分布式锁
func NewLock(client *redis.Client, key string, expiration time.Duration) *Lock {
	return &Lock{
		client:     client,
		key:        key,
		expiration: expiration,
		value:      generateLockValue(),
	}
}

// Lock 获取锁
func (l *Lock) Lock() error {
	return l.LockWithContext(context.Background())
}

// LockWithContext 带上下文的获取锁
func (l *Lock) LockWithContext(ctx context.Context) error {
	success, err := l.client.SetNX(ctx, l.key, l.value, l.expiration).Result()
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !success {
		return fmt.Errorf("failed to acquire lock: key %s is already locked", l.key)
	}
	return nil
}

// TryLock 尝试获取锁（非阻塞）
func (l *Lock) TryLock() (bool, error) {
	return l.TryLockWithContext(context.Background())
}

// TryLockWithContext 带上下文的尝试获取锁
func (l *Lock) TryLockWithContext(ctx context.Context) (bool, error) {
	success, err := l.client.SetNX(ctx, l.key, l.value, l.expiration).Result()
	if err != nil {
		return false, fmt.Errorf("failed to try lock: %w", err)
	}
	return success, nil
}

// Unlock 释放锁
func (l *Lock) Unlock() error {
	return l.UnlockWithContext(context.Background())
}

// UnlockWithContext 带上下文的释放锁
func (l *Lock) UnlockWithContext(ctx context.Context) error {
	// 使用 Lua 脚本确保原子性操作
	script := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`

	result, err := l.client.Eval(ctx, script, []string{l.key}, []interface{}{l.value}).Result()
	if err != nil {
		return fmt.Errorf("failed to unlock: %w", err)
	}

	if result.(int64) == 0 {
		return fmt.Errorf("lock not owned by this instance")
	}

	return nil
}

// Renew 续期锁
func (l *Lock) Renew() error {
	return l.RenewWithContext(context.Background())
}

// RenewWithContext 带上下文的续期锁
func (l *Lock) RenewWithContext(ctx context.Context) error {
	script := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("expire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`

	result, err := l.client.Eval(ctx, script, []string{l.key}, []interface{}{l.value, int(l.expiration.Seconds())}).Result()
	if err != nil {
		return fmt.Errorf("failed to renew lock: %w", err)
	}

	if result.(int64) == 0 {
		return fmt.Errorf("lock not owned by this instance")
	}

	return nil
}

// IsLocked 检查锁是否被持有
func (l *Lock) IsLocked() (bool, error) {
	return l.IsLockedWithContext(context.Background())
}

// IsLockedWithContext 带上下文的检查锁状态
func (l *Lock) IsLockedWithContext(ctx context.Context) (bool, error) {
	exists, err := l.client.Exists(ctx, l.key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check lock status: %w", err)
	}
	return exists > 0, nil
}

// generateLockValue 生成锁的唯一值
func generateLockValue() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), time.Now().Unix())
}
