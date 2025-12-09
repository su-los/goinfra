package slicepipe

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWrapHandlerProcessesAllJobs(t *testing.T) {
	ctx := context.Background()
	done := make(chan struct{})
	jobs := make(chan int, 3)
	expected := []int{1, 2, 3}
	for _, v := range expected {
		jobs <- v
	}
	close(jobs)

	var processed []int
	err := wrapHandler(ctx, done, jobs, func(ctx context.Context, data int) error {
		processed = append(processed, data)
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, len(expected), len(processed))
	for i, v := range expected {
		require.Equal(t, v, processed[i])
	}
}

func TestWrapHandlerHandlerError(t *testing.T) {
	ctx := context.Background()
	done := make(chan struct{})
	jobs := make(chan int, 3)
	for _, v := range []int{1, 2, 3} {
		jobs <- v
	}
	close(jobs)

	sentinel := errors.New("handler error")
	var processed []int
	err := wrapHandler(ctx, done, jobs, func(ctx context.Context, data int) error {
		processed = append(processed, data)
		if data == 2 {
			return sentinel
		}
		return nil
	})

	require.ErrorIs(t, err, sentinel)
	require.Len(t, processed, 2)
	require.Contains(t, processed, 1)
	require.Contains(t, processed, 2)
}

func TestWrapHandlerDoneChannel(t *testing.T) {
	ctx := context.Background()
	done := make(chan struct{})
	jobs := make(chan int) // unbuffered; no jobs sent
	close(done)

	err := wrapHandler(ctx, done, jobs, func(ctx context.Context, data int) error {
		t.Fatalf("handler should not be invoked")
		return nil
	})
	require.NoError(t, err)
}

func TestWrapHandlerContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	jobs := make(chan int) // unbuffered; no jobs sent
	cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- wrapHandler(ctx, done, jobs, func(ctx context.Context, data int) error {
			return assert.AnError
		})
	}()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("wrapHandler did not return after context cancellation")
	}
}

func TestDistributeAllJobsOrder(t *testing.T) {
	ctx := context.Background()
	done := make(chan struct{})
	datas := []int{1, 2, 3, 4, 5}
	gocnt := 3

	jobs := distribute(ctx, done, datas, gocnt)

	var received []int
	timeout := time.After(2 * time.Second)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case v, ok := <-jobs:
				if !ok {
					return
				}
				received = append(received, v)
			case <-timeout:
				require.Fail(t, "timeout waiting for jobs")
			}
		}
	}()

	wg.Wait()
	require.Equal(t, datas, received)
}

func TestDistributeStopsOnDone(t *testing.T) {
	ctx := context.Background()
	done := make(chan struct{})
	datas := []int{10, 20, 30, 40, 50}

	// gocnt = 0 creates an unbuffered channel ensuring determinism.
	jobs := distribute(ctx, done, datas, 0)

	// Read first job
	var first int
	select {
	case v, ok := <-jobs:
		require.True(t, ok)
		first = v
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for first job")
	}
	require.Equal(t, datas[0], first)

	// Signal early stop
	close(done)

	// Expect channel to close without further items
	select {
	case v, ok := <-jobs:
		if ok {
			t.Fatalf("expected no more jobs after done closed, got %v", v)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for channel close after done")
	}
}

func TestDistributeStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	datas := []int{100, 200, 300}

	jobs := distribute(ctx, done, datas, 0)

	// First job
	select {
	case v, ok := <-jobs:
		require.True(t, ok)
		require.Equal(t, datas[0], v)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for first job")
	}

	// Cancel context before more reads possible
	cancel()

	// Expect closure
	select {
	case v, ok := <-jobs:
		if ok {
			t.Fatalf("expected channel closed after context cancel, got %v", v)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for channel close after context cancel")
	}
}

func TestDistributeEmptySlice(t *testing.T) {
	ctx := context.Background()
	done := make(chan struct{})
	var datas []int

	jobs := distribute(ctx, done, datas, 2)

	select {
	case v, ok := <-jobs:
		require.False(t, ok, "expected closed channel for empty slice, got value: %v", v)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for channel close on empty slice")
	}
}

func TestSlicePipelineAllItemsProcessed(t *testing.T) {
	ctx := context.Background()
	datas := []int{1, 2, 3, 4, 5, 6, 7, 8}
	gocnt := 3

	processed := make(map[int]int)
	var mu sync.Mutex

	err := SlicePipeline(ctx, datas, gocnt, func(ctx context.Context, v int) error {
		mu.Lock()
		processed[v]++
		mu.Unlock()
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, len(datas), len(processed))
	for _, v := range datas {
		require.Equalf(t, 1, processed[v], "value %d processed unexpected times", v)
	}
}

func TestSlicePipelineHandlerErrorPropagation(t *testing.T) {
	ctx := context.Background()
	datas := []int{10, 20, 30, 40, 50}
	gocnt := 4

	sentinel := errors.New("boom")
	var mu sync.Mutex
	var processed []int

	err := SlicePipeline(ctx, datas, gocnt, func(ctx context.Context, v int) error {
		mu.Lock()
		processed = append(processed, v)
		mu.Unlock()
		if v == 30 {
			return sentinel
		}
		return nil
	})

	require.ErrorIs(t, err, sentinel)
	require.NotEmpty(t, processed)
	require.True(t, slices.Contains(processed, 30), "error-triggering item not recorded")
}

func TestSlicePipelineEmptySlice(t *testing.T) {
	ctx := context.Background()
	var datas []int
	gocnt := 5

	called := false
	err := SlicePipeline(ctx, datas, gocnt, func(ctx context.Context, v int) error {
		called = true
		return nil
	})
	require.NoError(t, err)
	require.False(t, called, "handler should not be called for empty slice")
}

func TestSlicePipelineConcurrentExecution(t *testing.T) {
	ctx := context.Background()
	datas := []int{1, 2, 3, 4, 5, 6, 7, 8}
	gocnt := 6

	var mu sync.Mutex
	current := 0
	maxConcurrent := 0

	err := SlicePipeline(ctx, datas, gocnt, func(ctx context.Context, v int) error {
		mu.Lock()
		current++
		if current > maxConcurrent {
			maxConcurrent = current
		}
		mu.Unlock()

		time.Sleep(30 * time.Millisecond) // simulate work

		mu.Lock()
		current--
		mu.Unlock()
		return nil
	})

	require.NoError(t, err)
	require.GreaterOrEqual(t, maxConcurrent, 2, "expected at least some parallelism, got %d", maxConcurrent)
}

func TestSlicePipelineStructType(t *testing.T) {
	type item struct {
		ID int
	}
	ctx := context.Background()
	datas := []item{{1}, {2}, {3}}
	gocnt := 2

	var mu sync.Mutex
	var ids []int

	err := SlicePipeline(ctx, datas, gocnt, func(ctx context.Context, it item) error {
		mu.Lock()
		ids = append(ids, it.ID)
		mu.Unlock()
		return nil
	})
	require.NoError(t, err)
	require.Len(t, ids, len(datas))
	sum := 0
	for _, v := range ids {
		sum += v
	}
	require.Equal(t, 1+2+3, sum)
}
