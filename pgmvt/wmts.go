package pgmvt

import (
	"encoding/json"
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"gorm.io/gorm"
)

type ColorData struct {
	LayerName string
	AttName   string
	ColorMap  []CMap
}
type CMap struct {
	Property string
	Color    string
}

// GenerateWMTSTile 生成 WMTS 瓦片（带缓存）
func GenerateWMTSTile(x int, y int, z int, layerName string, config models.WmtsSchema, db *gorm.DB) []byte {
	cacheTableName := layerName + "_wmts"

	// 1. 先查询缓存
	cachedTile := queryTileCache(db, cacheTableName, x, y, z)
	if cachedTile != nil {
		return cachedTile
	}

	// 2. 缓存未命中，生成瓦片
	// 计算瓦片边界
	boundboxMin := XyzLonLat(float64(x), float64(y), float64(z))
	boundboxMax := XyzLonLat(float64(x)+1, float64(y)+1, float64(z))

	// 解析颜色配置
	var colorData []ColorData
	if err := json.Unmarshal(config.ColorConfig, &colorData); err != nil {
		return nil
	}

	// 构建 CASE WHEN 语句用于颜色映射
	colorCaseSQL := buildColorCaseSQL(colorData)

	// 构建 ST_AsRaster SQL
	tileSize := config.TileSize
	if tileSize == 0 {
		tileSize = 256
	}

	sql := fmt.Sprintf(`
		WITH tile_geom AS (
			SELECT ST_MakeEnvelope(%v, %v, %v, %v, 4326) AS bbox
		),
		rasterized AS (
			SELECT ST_AsRaster(
				ST_Transform(geom, 4326),
				(SELECT bbox FROM tile_geom),
				%d, %d,
				ARRAY['8BUI', '8BUI', '8BUI', '8BUI']::text[],
				ARRAY[
					%s,  -- R
					%s,  -- G
					%s,  -- B
					%d   -- A (opacity)
				]::double precision[],
				ARRAY[0, 0, 0, 0]::double precision[]
			) AS rast
			FROM "%s"
			WHERE geom && ST_MakeEnvelope(%v, %v, %v, %v, 4326)
		),
		merged AS (
			SELECT ST_Union(rast) AS rast
			FROM rasterized
		)
		SELECT ST_AsPNG(rast) AS png
		FROM merged
		WHERE rast IS NOT NULL
	`,
		boundboxMin[0], boundboxMin[1], boundboxMax[0], boundboxMax[1],
		tileSize, tileSize,
		colorCaseSQL, colorCaseSQL, colorCaseSQL,
		int(config.Opacity*255),
		layerName,
		boundboxMin[0], boundboxMin[1], boundboxMax[0], boundboxMax[1],
	)

	// 执行查询
	var result struct {
		PNG []byte
	}

	db.Raw(sql).Scan(&result)

	// 3. 保存到缓存（即使是空瓦片也缓存，避免重复查询）
	if result.PNG != nil {
		saveTileCache(db, cacheTableName, x, y, z, result.PNG)
	}

	return result.PNG
}

// queryTileCache 查询瓦片缓存
func queryTileCache(db *gorm.DB, tableName string, x, y, z int) []byte {
	var cache struct {
		Byte []byte
	}

	sql := fmt.Sprintf(`SELECT byte FROM "%s" WHERE z = ? AND x = ? AND y = ? LIMIT 1`, tableName)
	result := db.Raw(sql, z, x, y).Scan(&cache)

	if result.Error != nil || result.RowsAffected == 0 {
		return nil
	}

	return cache.Byte
}

// saveTileCache 保存瓦片到缓存
func saveTileCache(db *gorm.DB, tableName string, x, y, z int, data []byte) error {
	sql := fmt.Sprintf(`
		INSERT INTO "%s" (x, y, z, byte) 
		VALUES (?, ?, ?, ?) 
		ON CONFLICT (z, x, y) DO UPDATE SET byte = EXCLUDED.byte
	`, tableName)

	return db.Exec(sql, x, y, z, data).Error
}

// buildColorCaseSQL 构建颜色映射的 CASE WHEN SQL
func buildColorCaseSQL(colorData []ColorData) string {
	if len(colorData) == 0 {
		return "128" // 默认灰色
	}

	// 使用第一个属性的颜色配置
	if len(colorData[0].ColorMap) == 0 {
		return "128"
	}

	attName := colorData[0].AttName
	caseSQL := "CASE "

	for _, cmap := range colorData[0].ColorMap {
		// 解析颜色 (假设格式为 #RRGGBB)
		color := cmap.Color
		if len(color) == 7 && color[0] == '#' {
			r, _, _ := hexToRGB(color)
			caseSQL += fmt.Sprintf("WHEN \"%s\" = '%s' THEN %d ", attName, cmap.Property, r)
		}
	}

	caseSQL += "ELSE 128 END" // 默认值

	return caseSQL
}

// hexToRGB 将十六进制颜色转换为 RGB
func hexToRGB(hex string) (int, int, int) {
	var r, g, b int
	fmt.Sscanf(hex, "#%02x%02x%02x", &r, &g, &b)
	return r, g, b
}

// GetEmptyTile 返回空白透明瓦片
func GetEmptyTile() []byte {
	// 返回 1x1 透明 PNG
	emptyPNG := []byte{
		0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,
		0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
		0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
		0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4,
		0x89, 0x00, 0x00, 0x00, 0x0A, 0x49, 0x44, 0x41,
		0x54, 0x78, 0x9C, 0x63, 0x00, 0x01, 0x00, 0x00,
		0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00,
		0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE,
		0x42, 0x60, 0x82,
	}
	return emptyPNG
}
