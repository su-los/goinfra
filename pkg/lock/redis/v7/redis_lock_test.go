package redis

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redismock/v7"
	"github.com/stretchr/testify/require"
)

func TestLock(t *testing.T) {
	ctx := context.Background()
	gclient, mock := redismock.NewClientMock()
	defer gclient.Close()
	// 1. Acquire 成功

	t.Run("AcquireSuccess", func(t *testing.T) {
		key := "test-lock"
		expiration := time.Second
		mock.Regexp().ExpectSetNX(key, ".*", expiration).SetVal(true)

		lock := NewRedisLock(gclient, key, time.Second)
		err := lock.Lock(ctx)
		require.NoError(t, err)
		require.NotNil(t, lock.locker)

		mock.Regexp().ExpectEvalSha(".*", []string{key}, ".*").SetVal(int64(1))
		err = lock.Unlock(ctx)
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	// 2. 多次加锁
	t.Run("MultipleLockCalls", func(t *testing.T) {
		key := "test-lock"
		expiration := time.Second
		mock.Regexp().ExpectSetNX(key, ".*", expiration).SetVal(true)

		lock := NewRedisLock(gclient, key, time.Second)
		err := lock.Lock(ctx)
		require.NoError(t, err)
		require.NotNil(t, lock.locker)

		// 再次调用 Lock 应该无操作
		err = lock.Lock(ctx)
		require.NoError(t, err)

		mock.Regexp().ExpectEvalSha(".*", []string{key}, ".*").SetVal(int64(1))
		err = lock.Unlock(ctx)
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestWatchDog(t *testing.T) {
	ctx := context.Background()
	gclient, mock := redismock.NewClientMock()
	defer gclient.Close()
	mock.MatchExpectationsInOrder(true)

	key := "test-lock"
	expiration := time.Millisecond * 300
	mock.Regexp().ExpectSetNX(key, ".*", expiration).SetVal(true)

	lock := NewRedisLock(gclient, key, expiration, WithWatchDog)
	err := lock.Lock(ctx)
	require.NoError(t, err)
	require.NotNil(t, lock.locker)

	// 模拟续期
	mock.Regexp().ExpectEvalSha(".*", []string{key}, ".*", "100").SetVal(int64(1))
	time.Sleep(lock.renewPeriod + 20*time.Millisecond)

	// 解锁
	mock.Regexp().ExpectEvalSha(".*", []string{key}, ".*").SetVal(int64(1))
	err = lock.Unlock(ctx)
	require.NoError(t, err)
}
