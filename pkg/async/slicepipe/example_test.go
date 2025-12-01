package slicepipe

import (
	"context"
	"fmt"
	"time"
)

func Example() {
	ctx := context.Background()
	datas := []int{1, 2, 3, 4, 5, 6}
	gocnt := 3

	err := SlicePipeline(ctx, datas, gocnt, func(ctx context.Context, data int) error {
		// 模拟处理数据
		fmt.Printf("Processing data: %d\n", data)
		time.Sleep(1 * time.Second)
		return nil
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}
