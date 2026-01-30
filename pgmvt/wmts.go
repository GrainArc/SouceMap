package pgmvt

import (
	"encoding/json"
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"gorm.io/gorm"
	"math"
	"regexp"
	"strconv"
	"strings"
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

type RGB struct {
	R int
	G int
	B int
	A int
}

// GenerateWMTSTile 生成 WMTS 瓦片（性能优化版）
func GenerateWMTSTile(x int, y int, z int, layerName string, config models.WmtsSchema, db *gorm.DB) []byte {
	cacheTableName := layerName + "_wmts"

	// 1. 先查询缓存
	cachedTile := queryTileCache(db, cacheTableName, x, y, z)
	if cachedTile != nil {
		return cachedTile
	}

	// 2. 计算瓦片经纬度边界
	boundboxMin := XyzLonLat(float64(x), float64(y), float64(z))
	boundboxMax := XyzLonLat(float64(x)+1, float64(y)+1, float64(z))

	minLon := math.Min(boundboxMin[0], boundboxMax[0])
	maxLon := math.Max(boundboxMin[0], boundboxMax[0])
	minLat := math.Min(boundboxMin[1], boundboxMax[1])
	maxLat := math.Max(boundboxMin[1], boundboxMax[1])

	tileSize := config.TileSize
	if tileSize == 0 {
		tileSize = 256
	}

	// 3. 计算像素分辨率
	scaleX := (maxLon - minLon) / float64(tileSize)
	scaleY := (maxLat - minLat) / float64(tileSize)

	// 4. 扩展范围（各边扩展1像素）
	extMinLon := minLon - scaleX
	extMaxLon := maxLon + scaleX
	extMinLat := minLat - scaleY
	extMaxLat := maxLat + scaleY
	extTileSize := tileSize + 2

	// 5. 根据缩放级别计算简化容差（关键优化）
	simplifyTolerance := calcSimplifyTolerance(z, scaleX, scaleY)

	// 6. 解析颜色配置
	var colorData []ColorData
	if err := json.Unmarshal(config.ColorConfig, &colorData); err != nil {
		return nil
	}

	alpha := int(config.Opacity * 255)

	// 7. 构建优化后的 SQL
	sql := buildOptimizedSQL(
		layerName, colorData,
		extMinLon, extMinLat, extMaxLon, extMaxLat,
		minLon, minLat, maxLon, maxLat,
		int(extTileSize), scaleX, scaleY,
		simplifyTolerance, alpha,
	)

	var result struct {
		PNG []byte
	}

	err := db.Raw(sql).Scan(&result).Error
	if err != nil {
		return nil
	}

	if result.PNG != nil && len(result.PNG) > 0 {
		saveTileCache(db, cacheTableName, x, y, z, result.PNG)
	}

	return result.PNG
}

// calcSimplifyTolerance 根据缩放级别计算几何简化容差
func calcSimplifyTolerance(z int, scaleX, scaleY float64) float64 {
	// 低于像素分辨率的细节不需要保留
	pixelSize := math.Max(scaleX, scaleY)

	// 缩放级别越低，简化程度越高
	switch {
	case z <= 8:
		return pixelSize * 4
	case z <= 12:
		return pixelSize * 2
	case z <= 16:
		return pixelSize
	default:
		return pixelSize * 0.5
	}
}

// buildOptimizedSQL 构建优化后的 SQL
func buildOptimizedSQL(
	layerName string, colorData []ColorData,
	extMinLon, extMinLat, extMaxLon, extMaxLat float64,
	minLon, minLat, maxLon, maxLat float64,
	extTileSize int, scaleX, scaleY float64,
	simplifyTolerance float64, alpha int,
) string {
	// 构建颜色 CASE 表达式
	rCase, gCase, bCase := buildColorCaseExpr(colorData)

	sql := fmt.Sprintf(`
        WITH 
        ext_raster AS (
            SELECT ST_AddBand(
                ST_MakeEmptyRaster(%d, %d, %v, %v, %v, -%v, 0, 0, 4326),
                ARRAY[
                    ROW(1, '8BUI', 0, 0),
                    ROW(2, '8BUI', 0, 0),
                    ROW(3, '8BUI', 0, 0),
                    ROW(4, '8BUI', 0, 0)
                ]::addbandarg[]
            ) AS rast
        ),
        features AS (
            SELECT 
                ST_SimplifyPreserveTopology(geom, %v) AS geom,
                (%s)::int AS r, (%s)::int AS g, (%s)::int AS b
            FROM "%s"
            WHERE geom && ST_MakeEnvelope(%v, %v, %v, %v, 4326)
        ),
        grouped AS (
            SELECT r, g, b, ST_Collect(geom) AS geom
            FROM features
            WHERE geom IS NOT NULL
            GROUP BY r, g, b
        ),
        rasterized AS (
            SELECT ST_AsRaster(
                g.geom, e.rast,
                ARRAY['8BUI', '8BUI', '8BUI', '8BUI'],
                ARRAY[g.r, g.g, g.b, %d]::float8[],
                ARRAY[0, 0, 0, 0]::float8[],
                true
            ) AS rast
            FROM grouped g, ext_raster e
            WHERE NOT ST_IsEmpty(g.geom)
        ),
        merged AS (
            SELECT ST_Union(rast, 'LAST') AS rast
            FROM (
                SELECT rast FROM ext_raster
                UNION ALL
                SELECT rast FROM rasterized WHERE rast IS NOT NULL
            ) t
        )
        SELECT ST_AsPNG(
            ST_Clip(rast, ST_MakeEnvelope(%v, %v, %v, %v, 4326), ARRAY[0,0,0,0]::float8[], true)
        ) AS png
        FROM merged
    `,
		extTileSize, extTileSize, extMinLon, extMaxLat, scaleX, scaleY,
		simplifyTolerance,
		rCase, gCase, bCase,
		layerName,
		extMinLon, extMinLat, extMaxLon, extMaxLat,
		alpha,
		minLon, minLat, maxLon, maxLat,
	)

	return sql
}

// buildColorCaseExpr 构建颜色 CASE 表达式
func buildColorCaseExpr(colorData []ColorData) (string, string, string) {
	if len(colorData) == 0 || len(colorData[0].ColorMap) == 0 {
		return "128", "128", "128"
	}

	attName := colorData[0].AttName
	colorMap := colorData[0].ColorMap

	if attName == "默认" && len(colorMap) > 0 && colorMap[0].Property == "默认" {
		rgb := parseColor(colorMap[0].Color)
		return fmt.Sprintf("%d", rgb.R), fmt.Sprintf("%d", rgb.G), fmt.Sprintf("%d", rgb.B)
	}

	var rParts, gParts, bParts []string
	for _, cm := range colorMap {
		rgb := parseColor(cm.Color)
		prop := strings.ReplaceAll(cm.Property, "'", "''")
		rParts = append(rParts, fmt.Sprintf("WHEN \"%s\"='%s' THEN %d", attName, prop, rgb.R))
		gParts = append(gParts, fmt.Sprintf("WHEN \"%s\"='%s' THEN %d", attName, prop, rgb.G))
		bParts = append(bParts, fmt.Sprintf("WHEN \"%s\"='%s' THEN %d", attName, prop, rgb.B))
	}

	return "CASE " + strings.Join(rParts, " ") + " ELSE 128 END",
		"CASE " + strings.Join(gParts, " ") + " ELSE 128 END",
		"CASE " + strings.Join(bParts, " ") + " ELSE 128 END"
}

// buildColorGroupSQL 构建按颜色分组合并几何的 SQL
func buildColorGroupSQL(colorData []ColorData, layerName string) string {
	baseTemplate := `
        SELECT 
            %d AS r_val, %d AS g_val, %d AS b_val,
            ST_Union(geom) AS geom
        FROM "%s"
        WHERE geom && (SELECT geom FROM ext_bounds)%s
        GROUP BY 1, 2, 3
    `

	if len(colorData) == 0 || len(colorData[0].ColorMap) == 0 {
		return fmt.Sprintf(baseTemplate, 128, 128, 128, layerName, "")
	}

	attName := colorData[0].AttName
	colorMap := colorData[0].ColorMap

	if attName == "默认" && len(colorMap) > 0 && colorMap[0].Property == "默认" {
		rgb := parseColor(colorMap[0].Color)
		return fmt.Sprintf(baseTemplate, rgb.R, rgb.G, rgb.B, layerName, "")
	}

	var unionParts []string
	for _, cmap := range colorMap {
		rgb := parseColor(cmap.Color)
		escapedProperty := strings.ReplaceAll(cmap.Property, "'", "''")
		condition := fmt.Sprintf(` AND "%s" = '%s'`, attName, escapedProperty)
		part := fmt.Sprintf(baseTemplate, rgb.R, rgb.G, rgb.B, layerName, condition)
		unionParts = append(unionParts, part)
	}

	var conditions []string
	for _, cmap := range colorMap {
		escapedProperty := strings.ReplaceAll(cmap.Property, "'", "''")
		conditions = append(conditions, fmt.Sprintf(`"%s" = '%s'`, attName, escapedProperty))
	}
	defaultCondition := fmt.Sprintf(` AND NOT (%s)`, strings.Join(conditions, " OR "))
	defaultPart := fmt.Sprintf(baseTemplate, 128, 128, 128, layerName, defaultCondition)
	unionParts = append(unionParts, defaultPart)

	return strings.Join(unionParts, "\nUNION ALL\n")
}

// buildColorCaseSQL 构建颜色映射的 CASE WHEN SQL，返回 R、G、B 三个通道
func buildColorCaseSQL(colorData []ColorData) (string, string, string) {
	// 默认灰色
	defaultR, defaultG, defaultB := "128", "128", "128"

	if len(colorData) == 0 || len(colorData[0].ColorMap) == 0 {
		return defaultR, defaultG, defaultB
	}

	attName := colorData[0].AttName
	colorMap := colorData[0].ColorMap

	// 检查是否为"默认"单一颜色模式
	if attName == "默认" && len(colorMap) > 0 && colorMap[0].Property == "默认" {
		rgb := parseColor(colorMap[0].Color)
		return fmt.Sprintf("%d", rgb.R), fmt.Sprintf("%d", rgb.G), fmt.Sprintf("%d", rgb.B)
	}

	// 构建 CASE WHEN 语句
	var rCases, gCases, bCases []string

	for _, cmap := range colorMap {
		rgb := parseColor(cmap.Color)

		// 转义属性值中的单引号
		escapedProperty := strings.ReplaceAll(cmap.Property, "'", "''")

		rCases = append(rCases, fmt.Sprintf("WHEN \"%s\" = '%s' THEN %d", attName, escapedProperty, rgb.R))
		gCases = append(gCases, fmt.Sprintf("WHEN \"%s\" = '%s' THEN %d", attName, escapedProperty, rgb.G))
		bCases = append(bCases, fmt.Sprintf("WHEN \"%s\" = '%s' THEN %d", attName, escapedProperty, rgb.B))
	}

	rCaseSQL := "CASE " + strings.Join(rCases, " ") + " ELSE 128 END"
	gCaseSQL := "CASE " + strings.Join(gCases, " ") + " ELSE 128 END"
	bCaseSQL := "CASE " + strings.Join(bCases, " ") + " ELSE 128 END"

	return rCaseSQL, gCaseSQL, bCaseSQL
}

// parseColor 解析颜色字符串，支持 hex、rgb、rgba 格式（大小写不敏感）
func parseColor(color string) RGB {
	color = strings.TrimSpace(color)
	colorLower := strings.ToLower(color)

	// 1. 尝试解析 hex 格式：#RRGGBB 或 #RGB
	if strings.HasPrefix(colorLower, "#") {
		return parseHexColor(color)
	}

	// 2. 尝试解析 rgba 格式：rgba(r, g, b, a) 或 RGBA(r, g, b, a)
	if strings.HasPrefix(colorLower, "rgba") {
		return parseRGBAColor(color)
	}

	// 3. 尝试解析 rgb 格式：rgb(r, g, b) 或 RGB(r, g, b)
	if strings.HasPrefix(colorLower, "rgb") {
		return parseRGBColor(color)
	}

	// 默认返回灰色
	return RGB{R: 128, G: 128, B: 128, A: 255}
}

// parseHexColor 解析十六进制颜色（支持大小写）
func parseHexColor(hex string) RGB {
	hex = strings.TrimPrefix(hex, "#")
	hex = strings.ToLower(hex) // 转换为小写统一处理

	// 处理简写格式 #RGB -> #RRGGBB
	if len(hex) == 3 {
		hex = string([]byte{hex[0], hex[0], hex[1], hex[1], hex[2], hex[2]})
	}

	if len(hex) != 6 {
		return RGB{R: 128, G: 128, B: 128, A: 255}
	}

	var r, g, b int
	fmt.Sscanf(hex, "%02x%02x%02x", &r, &g, &b)
	return RGB{R: r, G: g, B: b, A: 255}
}

// parseRGBColor 解析 rgb(r, g, b) 格式（支持大小写）
func parseRGBColor(color string) RGB {
	// 使用正则提取数字（大小写不敏感）
	re := regexp.MustCompile(`(?i)rgb\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)`)
	matches := re.FindStringSubmatch(color)

	if len(matches) != 4 {
		return RGB{R: 128, G: 128, B: 128, A: 255}
	}

	r, _ := strconv.Atoi(matches[1])
	g, _ := strconv.Atoi(matches[2])
	b, _ := strconv.Atoi(matches[3])

	return RGB{
		R: clamp(r, 0, 255),
		G: clamp(g, 0, 255),
		B: clamp(b, 0, 255),
		A: 255,
	}
}

// parseRGBAColor 解析 rgba(r, g, b, a) 格式（支持大小写）
func parseRGBAColor(color string) RGB {
	// 使用正则提取数字（大小写不敏感）
	re := regexp.MustCompile(`(?i)rgba\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*([\d.]+)\s*\)`)
	matches := re.FindStringSubmatch(color)

	if len(matches) != 5 {
		return RGB{R: 128, G: 128, B: 128, A: 255}
	}

	r, _ := strconv.Atoi(matches[1])
	g, _ := strconv.Atoi(matches[2])
	b, _ := strconv.Atoi(matches[3])
	a, _ := strconv.ParseFloat(matches[4], 64)

	// alpha 可能是 0-1 的小数或 0-255 的整数
	alphaInt := int(a)
	if a <= 1.0 {
		alphaInt = int(a * 255)
	}

	return RGB{
		R: clamp(r, 0, 255),
		G: clamp(g, 0, 255),
		B: clamp(b, 0, 255),
		A: clamp(alphaInt, 0, 255),
	}
}

// clamp 限制值在指定范围内
func clamp(value, min, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
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

// GetEmptyTile 返回空白透明瓦片
func GetEmptyTile() []byte {
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
