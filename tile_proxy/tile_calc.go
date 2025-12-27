// tile_utils.go
package tile_proxy

import (
	"math"
)

const (
	EarthRadius = 6378137.0
	OriginShift = math.Pi * EarthRadius // 20037508.342789244
)

// TileBounds 瓦片边界（WGS84经纬度）
type TileBounds struct {
	MinLon float64
	MinLat float64
	MaxLon float64
	MaxLat float64
}

// PixelCoord 像素坐标
type PixelCoord struct {
	X float64
	Y float64
}

// TileCoord 瓦片坐标
type TileCoord struct {
	Z int
	X int
	Y int
}

// TileRange 瓦片范围
type TileRange struct {
	MinX int
	MaxX int
	MinY int
	MaxY int
}

// TileConfig 瓦片配置
type TileConfig struct {
	TileSize int // 瓦片大小，如256或512
}

// DefaultTileConfig 默认瓦片配置
func DefaultTileConfig() *TileConfig {
	return &TileConfig{TileSize: 256}
}

// GetTileBoundsWGS84 获取瓦片的WGS84边界
func GetTileBoundsWGS84(z, x, y int) TileBounds {
	n := math.Pow(2, float64(z))

	minLon := float64(x)/n*360.0 - 180.0
	maxLon := float64(x+1)/n*360.0 - 180.0

	minLatRad := math.Atan(math.Sinh(math.Pi * (1 - 2*float64(y+1)/n)))
	maxLatRad := math.Atan(math.Sinh(math.Pi * (1 - 2*float64(y)/n)))

	minLat := minLatRad * 180.0 / math.Pi
	maxLat := maxLatRad * 180.0 / math.Pi

	return TileBounds{
		MinLon: minLon,
		MinLat: minLat,
		MaxLon: maxLon,
		MaxLat: maxLat,
	}
}

// LonLatToGlobalPixel 经纬度转全局像素坐标
func LonLatToGlobalPixel(lon, lat float64, z int, tileSize int) PixelCoord {
	n := math.Pow(2, float64(z))

	x := (lon + 180.0) / 360.0 * n * float64(tileSize)

	latRad := lat * math.Pi / 180.0
	y := (1.0 - math.Log(math.Tan(latRad)+1.0/math.Cos(latRad))/math.Pi) / 2.0 * n * float64(tileSize)

	return PixelCoord{X: x, Y: y}
}

// GlobalPixelToTile 全局像素坐标转瓦片坐标
func GlobalPixelToTile(px, py float64, tileSize int) (tileX, tileY int, offsetX, offsetY float64) {
	tileX = int(math.Floor(px / float64(tileSize)))
	tileY = int(math.Floor(py / float64(tileSize)))
	offsetX = px - float64(tileX)*float64(tileSize)
	offsetY = py - float64(tileY)*float64(tileSize)
	return
}

// CalculateRequiredTilesAndCrop 计算需要请求的瓦片范围和裁剪信息
type CropInfo struct {
	// 在拼接画布上的裁剪起点
	CropX float64
	CropY float64
	// 裁剪宽高
	CropWidth  float64
	CropHeight float64
}

// CalculateRequiredTilesWithCrop 计算需要请求的瓦片范围和精确裁剪信息
func CalculateRequiredTilesWithCrop(
	topLeftPixel, bottomRightPixel PixelCoord,
	tileSize int,
) (TileRange, CropInfo) {
	// 计算瓦片范围
	minTileX := int(math.Floor(topLeftPixel.X / float64(tileSize)))
	minTileY := int(math.Floor(topLeftPixel.Y / float64(tileSize)))
	maxTileX := int(math.Floor(bottomRightPixel.X / float64(tileSize)))
	maxTileY := int(math.Floor(bottomRightPixel.Y / float64(tileSize)))

	// 计算裁剪信息
	// 裁剪起点 = 目标像素坐标 - 瓦片范围起点的像素坐标
	cropX := topLeftPixel.X - float64(minTileX)*float64(tileSize)
	cropY := topLeftPixel.Y - float64(minTileY)*float64(tileSize)

	// 裁剪宽高 = 目标区域的像素范围
	cropWidth := bottomRightPixel.X - topLeftPixel.X
	cropHeight := bottomRightPixel.Y - topLeftPixel.Y

	return TileRange{
			MinX: minTileX,
			MaxX: maxTileX,
			MinY: minTileY,
			MaxY: maxTileY,
		}, CropInfo{
			CropX:      cropX,
			CropY:      cropY,
			CropWidth:  cropWidth,
			CropHeight: cropHeight,
		}
}

// LonLatToTileCoord 经纬度转瓦片坐标
func LonLatToTileCoord(lon, lat float64, z int) TileCoord {
	n := math.Pow(2, float64(z))

	x := int(math.Floor((lon + 180.0) / 360.0 * n))

	latRad := lat * math.Pi / 180.0
	y := int(math.Floor((1.0 - math.Log(math.Tan(latRad)+1.0/math.Cos(latRad))/math.Pi) / 2.0 * n))

	// 边界处理
	maxTile := int(n) - 1
	if x < 0 {
		x = 0
	} else if x > maxTile {
		x = maxTile
	}
	if y < 0 {
		y = 0
	} else if y > maxTile {
		y = maxTile
	}

	return TileCoord{Z: z, X: x, Y: y}
}
