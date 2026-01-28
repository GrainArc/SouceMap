package pgmvt

import (
	"encoding/json"
	"fmt"
	"github.com/GrainArc/Gogeo"
	"github.com/GrainArc/SouceMap/models"
	"gorm.io/gorm"
	"log"

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

func GenerateWMTSTile(x int, y int, z int, layerName string, config models.WmtsSchema, db *gorm.DB) []byte {
	cacheTableName := layerName + "_wmts"
	// 1. 查询缓存
	cachedTile := queryTileCache(db, cacheTableName, x, y, z)
	if cachedTile != nil {
		return cachedTile
	}
	// 2. 计算瓦片边界
	bounds := Gogeo.CalculateTileBounds(x, y, z)
	// 3. 预检查数据
	var dataCount int64
	checkSQL := fmt.Sprintf(`
        SELECT COUNT(*) 
        FROM "%s" 
        WHERE geom && ST_MakeEnvelope(%v, %v, %v, %v, 4326)
    `, layerName, bounds.MinLon, bounds.MinLat, bounds.MaxLon, bounds.MaxLat)
	db.Raw(checkSQL).Scan(&dataCount)
	if dataCount == 0 {
		return GetEmptyTile()
	}
	// 4. 解析颜色配置
	var colorData []ColorData
	if err := json.Unmarshal(config.ColorConfig, &colorData); err != nil {
		log.Printf("解析颜色配置失败: %v", err)
		return nil
	}
	// 5. 从PostgreSQL查询矢量数据
	features, err := queryVectorFeatures(db, layerName, bounds)

	if err != nil {
		log.Printf("查询矢量数据失败: %v", err)
		return nil
	}
	if len(features) == 0 {
		return GetEmptyTile()
	}
	// 6. 创建Gogeo配置
	tileSize := config.TileSize
	if tileSize == 0 {
		tileSize = 256
	}
	gogeoConfig := Gogeo.VectorTileConfig{
		TileSize: int(tileSize),
		Opacity:  config.Opacity,
		ColorMap: convertColorConfig(colorData),
	}
	// 7. 创建瓦片生成器
	generator := Gogeo.NewVectorTileGenerator(gogeoConfig)
	// 8. 创建矢量图层
	vectorLayer, err := generator.CreateVectorLayerFromWKB(features, 4326)

	if err != nil {
		log.Printf("创建矢量图层失败: %v", err)
		return nil
	}

	defer vectorLayer.Close()
	// 9. 栅格化生成PNG
	pngData, err := generator.RasterizeVectorLayer(vectorLayer, Gogeo.VectorTileBounds{
		MinLon: bounds.MinLon,
		MinLat: bounds.MinLat,
		MaxLon: bounds.MaxLon,
		MaxLat: bounds.MaxLat,
	})
	fmt.Println(pngData)
	if err != nil {
		log.Printf("栅格化失败: %v", err)
		return nil
	}
	// 10. 保存缓存
	if pngData != nil && len(pngData) > 0 {
		saveTileCache(db, cacheTableName, x, y, z, pngData)
	}
	return pngData
}

func queryVectorFeatures(db *gorm.DB, tableName string, bounds Gogeo.VectorTileBounds) ([]Gogeo.VectorFeature, error) {
	// 构建边界框WKT
	boxWKT := fmt.Sprintf("POLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))",
		bounds.MinLon, bounds.MinLat,
		bounds.MaxLon, bounds.MinLat,
		bounds.MaxLon, bounds.MaxLat,
		bounds.MinLon, bounds.MaxLat,
		bounds.MinLon, bounds.MinLat,
	)
	// 获取字段列表
	var columns []struct {
		ColumnName string `gorm:"column:column_name"`
	}
	err := db.Raw(fmt.Sprintf(`
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = '%s' 
		AND column_name != 'geom'
		ORDER BY ordinal_position
	`, tableName)).Scan(&columns).Error
	if err != nil {
		return nil, fmt.Errorf("获取字段列表失败: %v", err)
	}
	var fieldList []string
	for _, col := range columns {
		fieldList = append(fieldList, col.ColumnName)
	}
	fieldListStr := strings.Join(fieldList, ", ")
	// 查询数据
	query := fmt.Sprintf(`
		SELECT 
			ST_AsBinary(
				ST_Transform(
					ST_Intersection(
						ST_Transform(geom, 4326),
						ST_GeomFromText('%s', 4326)
					),
					4326
				)
			) as geom,
			%s
		FROM "%s"
		WHERE ST_Intersects(
			ST_Transform(geom, 4326),
			ST_GeomFromText('%s', 4326)
		)
		AND NOT ST_IsEmpty(
			ST_Intersection(
				ST_Transform(geom, 4326),
				ST_GeomFromText('%s', 4326)
			)
		)
	`, boxWKT, fieldListStr, tableName, boxWKT, boxWKT)
	rows, err := db.Raw(query).Rows()
	if err != nil {
		return nil, fmt.Errorf("执行查询失败: %v", err)
	}
	defer rows.Close()
	columnNames, _ := rows.Columns()
	columnMap := make(map[string]int)
	for i, name := range columnNames {
		columnMap[name] = i
	}
	var features []Gogeo.VectorFeature
	for rows.Next() {
		values := make([]interface{}, len(columnNames))
		valuePtrs := make([]interface{}, len(columnNames))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}
		feature := Gogeo.VectorFeature{
			Attributes: make(map[string]string),
		}
		// 提取几何
		if geomIdx, ok := columnMap["geom"]; ok {
			if wkb, ok := values[geomIdx].([]byte); ok {
				feature.WKB = wkb
			}
		}
		// 提取属性
		for _, col := range columns {
			if idx, ok := columnMap[col.ColumnName]; ok {
				if values[idx] != nil {
					feature.Attributes[col.ColumnName] = fmt.Sprintf("%v", values[idx])
				}
			}
		}
		features = append(features, feature)
	}
	return features, nil
}
func convertColorConfig(colorData []ColorData) []Gogeo.VectorColorRule {
	if len(colorData) == 0 {
		return []Gogeo.VectorColorRule{}
	}
	cd := colorData[0]

	// 单一默认颜色
	if cd.AttName == "默认" && len(cd.ColorMap) > 0 && cd.ColorMap[0].Property == "默认" {
		return []Gogeo.VectorColorRule{
			{
				AttributeName:  "默认",
				AttributeValue: "默认",
				Color:          cd.ColorMap[0].Color,
			},
		}
	}
	// 按属性值映射
	colorValues := make(map[string]string)
	for _, cmap := range cd.ColorMap {
		colorValues[cmap.Property] = cmap.Color
	}
	return []Gogeo.VectorColorRule{
		{
			AttributeName: cd.AttName,
			ColorValues:   colorValues,
		},
	}
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
