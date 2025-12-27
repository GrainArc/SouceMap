package tile_proxy

import (
	"math"
)

const (
	TileSize    = 256
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
func LonLatToGlobalPixel(lon, lat float64, z int) PixelCoord {
	n := math.Pow(2, float64(z))

	x := (lon + 180.0) / 360.0 * n * TileSize

	latRad := lat * math.Pi / 180.0
	y := (1.0 - math.Log(math.Tan(latRad)+1.0/math.Cos(latRad))/math.Pi) / 2.0 * n * TileSize

	return PixelCoord{X: x, Y: y}
}

// GlobalPixelToTile 全局像素坐标转瓦片坐标
func GlobalPixelToTile(px, py float64) (tileX, tileY int, offsetX, offsetY float64) {
	tileX = int(math.Floor(px / TileSize))
	tileY = int(math.Floor(py / TileSize))
	offsetX = px - float64(tileX)*TileSize
	offsetY = py - float64(tileY)*TileSize
	return
}

// CalculateRequiredTiles 计算需要请求的瓦片范围
func CalculateRequiredTiles(topLeftPixel, bottomRightPixel PixelCoord) (TileRange, PixelCoord) {
	minTileX, minTileY, offsetX, offsetY := GlobalPixelToTile(topLeftPixel.X, topLeftPixel.Y)
	maxTileX := int(math.Floor(bottomRightPixel.X / TileSize))
	maxTileY := int(math.Floor(bottomRightPixel.Y / TileSize))

	return TileRange{
		MinX: minTileX,
		MaxX: maxTileX,
		MinY: minTileY,
		MaxY: maxTileY,
	}, PixelCoord{X: offsetX, Y: offsetY}
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
