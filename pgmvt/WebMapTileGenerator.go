package pgmvt

import (
	"math"
)

const HEMI_MAP_WIDTH = math.Pi * float64(6378137)
const PRECISION = 6

type tile struct {
	Z int64
	X int64
	Y int64
}

func generate(zoomLevel int64, tileSize float64, rows [][3]float64, cbeg int64, cend int64) []tile {
	//  Calculate  x-direction  tile  origins
	cols := make([][3]float64, cend-cbeg+1)
	for c := cbeg; c <= cend; c++ {
		cols[c-cbeg] = [3]float64{float64(c), math.Round(float64(c)*tileSize-HEMI_MAP_WIDTH*1.0) / 1.0e6, math.Round(float64(c+1)*tileSize-HEMI_MAP_WIDTH*1.0) / 1.0e6}
	}
	tile_json := make([]tile, 0)
	//  Generate  and  output  tile  features.
	for _, row := range rows {
		for _, col := range cols {
			tile_json = append(tile_json, tile{
				Z: zoomLevel,
				X: int64(col[0]),
				Y: int64(row[0]),
			})
		}
	}
	return tile_json
}

func TileGenerate(xmin, ymin, xmax, ymax float64) []tile {
	west := float64(xmin)
	east := float64(xmax)
	south := float64(ymin)
	north := float64(ymax)
	tile := make([]tile, 0)
	for i := int64(6); i <= int64(18); i++ {
		zoomLevel := i
		if zoomLevel < 0 {
			zoomLevel = 0
		}
		numColumns := int64(math.Pow(2, float64(zoomLevel)))
		tileSize := 2.0 * HEMI_MAP_WIDTH / float64(numColumns)
		rbeg := int64(math.Floor((HEMI_MAP_WIDTH - north) / tileSize))
		rend := int64(math.Ceil((HEMI_MAP_WIDTH - south) / tileSize))
		rows := make([][3]float64, rend-rbeg+1)
		for r := rbeg; r <= rend; r++ {
			rows[r-rbeg] = [3]float64{float64(r), math.Round(HEMI_MAP_WIDTH-float64(r)*tileSize) / 1.0e6, math.Round(HEMI_MAP_WIDTH-float64(r+1)*tileSize) / 1.0e6}
		}

		cbeg := int64(math.Floor((HEMI_MAP_WIDTH + west) / tileSize))
		cend := int64(math.Ceil((HEMI_MAP_WIDTH + east) / tileSize))
		if cbeg < cend {
			tile_json := generate(zoomLevel, tileSize, rows, cbeg, cend)
			tile = append(tile, tile_json...)
		} else {
			tile_json := generate(zoomLevel, tileSize, rows, cbeg, numColumns)
			tile_json1 := generate(zoomLevel, tileSize, rows, 0, cend)
			tile_json = append(tile_json, tile_json1...)
			tile = append(tile, tile_json...)
		}
	}
	return tile
}

func GetPointTile(x float64, y float64) []tile {
	tiles := make([]tile, 0)

	// 遍历6-18级
	for zoom := int64(6); zoom <= int64(18); zoom++ {
		// 使用现有的 LonLatToTile 函数计算瓦片坐标
		tileX, tileY := LonLatToTile(x, y, zoom)

		// 创建瓦片对象并添加到结果中
		tiles = append(tiles, tile{
			Z: zoom,
			X: tileX,
			Y: tileY,
		})
	}

	return tiles
}

func pixelToLatLon(px, py, z int) [2]float64 {
	// 将像素坐标转换为经纬度
	tileSize := 256
	mapSize := tileSize * int(math.Pow(2, float64(z)))
	lonDeg := float64(px)/float64(mapSize)*360.0 - 180.0
	latRad := math.Atan(math.Sinh(math.Pi * (1 - 2*float64(py)/float64(mapSize))))
	latDeg := latRad * 180.0 / math.Pi
	var data [2]float64
	data[0] = lonDeg
	data[1] = latDeg
	return data
}

func TileToLatLon(z, x, y int) (topLeft, topRight, bottomLeft, bottomRight [2]float64) {
	tileSize := 256
	// 计算瓦片四个角的像素坐标
	topLeftPx := x * tileSize
	topLeftPy := y * tileSize
	bottomRightPx := (x + 1) * tileSize
	bottomRightPy := (y + 1) * tileSize
	// 计算并返回四个角的经纬度
	topLeft = pixelToLatLon(topLeftPx, topLeftPy, z)
	topRight = pixelToLatLon(bottomRightPx, topLeftPy, z)
	bottomLeft = pixelToLatLon(topLeftPx, bottomRightPy, z)
	bottomRight = pixelToLatLon(bottomRightPx, bottomRightPy, z)
	return topLeft, topRight, bottomLeft, bottomRight
}

func LonLatToTile(lon, lat float64, zoom int64) (x, y int64) {
	// 1. 先转换为Web墨卡托坐标
	const (
		EarthRadius = 6378137.0
		OriginShift = 2 * math.Pi * EarthRadius / 2.0
	)

	mercX := lon * OriginShift / 180.0
	mercY := math.Log(math.Tan((90+lat)*math.Pi/360.0)) * OriginShift / math.Pi

	// 2. 计算瓦片坐标
	resolution := (2 * OriginShift) / math.Exp2(float64(zoom))
	x = int64(math.Floor((mercX + OriginShift) / resolution))
	y = int64(math.Floor((OriginShift - mercY) / resolution))

	// 3. 处理边界情况
	maxTile := int64(math.Exp2(float64(zoom))) - 1
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

	return
}
