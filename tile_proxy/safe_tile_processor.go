// safe_tile_processor.go
package tile_proxy

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/GrainArc/Gogeo"
)

// SafeTileProcessor 安全的瓦片处理器
type SafeTileProcessor struct {
	maxConcurrent int
	semaphore     chan struct{}
	processPool   sync.Pool
}

// NewSafeTileProcessor 创建安全处理器
func NewSafeTileProcessor(maxConcurrent int) *SafeTileProcessor {
	return &SafeTileProcessor{
		maxConcurrent: maxConcurrent,
		semaphore:     make(chan struct{}, maxConcurrent),
	}
}

// ProcessTileResult 处理结果
type ProcessTileResult struct {
	Data []byte
	Err  error
}

// ProcessWithRecover 带恢复的处理
func (s *SafeTileProcessor) ProcessWithRecover(
	ctx context.Context,
	processFn func() ([]byte, error),
) (result ProcessTileResult) {
	// 获取信号量
	select {
	case s.semaphore <- struct{}{}:
		defer func() { <-s.semaphore }()
	case <-ctx.Done():
		return ProcessTileResult{Err: ctx.Err()}
	}

	// 使用 defer recover 捕获 panic
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			result.Err = fmt.Errorf("panic recovered: %v\nstack: %s", r, string(stack))
			fmt.Printf("⚠️ Tile processing panic: %v\n", r)
		}
	}()

	// 设置超时
	done := make(chan ProcessTileResult, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- ProcessTileResult{Err: fmt.Errorf("goroutine panic: %v", r)}
			}
		}()

		data, err := processFn()
		done <- ProcessTileResult{Data: data, Err: err}
	}()

	select {
	case result = <-done:
		return result
	case <-ctx.Done():
		return ProcessTileResult{Err: ctx.Err()}
	case <-time.After(60 * time.Second):
		return ProcessTileResult{Err: fmt.Errorf("processing timeout")}
	}
}

// SafeImageProcess 安全的图像处理
func SafeImageProcess(
	ctx context.Context,
	tiles []FetchedTile,
	tileRange TileRange,
	tileSize int,
	cropX, cropY int,
	outputFormat string,
) ([]byte, error) {
	// 计算画布尺寸
	canvasWidth := (tileRange.MaxX - tileRange.MinX + 1) * tileSize
	canvasHeight := (tileRange.MaxY - tileRange.MinY + 1) * tileSize

	// 限制画布大小
	if canvasWidth > 4096 || canvasHeight > 4096 {
		return nil, fmt.Errorf("canvas too large: %dx%d", canvasWidth, canvasHeight)
	}

	var processor *Gogeo.ImageProcessor
	var err error

	// 创建处理器（带重试）
	for attempt := 0; attempt < 3; attempt++ {
		processor, err = Gogeo.NewImageProcessor(canvasWidth, canvasHeight, 4)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create processor after retries: %v", err)
	}

	// 确保处理器被关闭
	defer func() {
		if processor != nil {
			processor.Close()
		}
	}()

	// 添加瓦片
	successCount := 0
	for _, tile := range tiles {
		// 检查上下文
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if tile.Err != nil || len(tile.Data) == 0 {
			continue
		}
		dstX := (tile.X - tileRange.MinX) * tileSize
		dstY := (tile.Y - tileRange.MinY) * tileSize

		if err := processor.AddTileFromBuffer(tile.Data, tile.Format, dstX, dstY); err != nil {
			fmt.Printf("Warning: failed to add tile %d/%d: %v\n", tile.X, tile.Y, err)
			continue
		}
		successCount++
	}

	if successCount == 0 {
		return nil, fmt.Errorf("no tiles were successfully added")
	}

	// 裁剪并导出
	return processor.CropAndExport(cropX, cropY, tileSize, tileSize, outputFormat)
}
