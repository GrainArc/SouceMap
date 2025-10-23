package pgmvt

import (
	"encoding/hex"
	"fmt"
	"gitee.com/LJ_COOL/go-shp"
	"github.com/GrainArc/Gogeo"
	"github.com/GrainArc/SouceMap/Transformer"
	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/models"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/saintfish/chardet"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"
)

func judgeSlice(validEN []string, ENs []string) (bool, string) {
	for _, EN := range ENs {
		for _, valid := range validEN {
			if EN == valid {
				return true, EN
				break // 找到后可以直接跳出循环
			}
		}
	}
	return false, ""
}
func AddGeoDirectly(DB *gorm.DB, dirpath string, EN string, CN string, Main string, Color string, Opacity string, Userunits string, addType string, LineWidth string) string {
	shpfiles := Transformer.FindFiles(dirpath, "shp")

	replacer := strings.NewReplacer(
		"POINT", "point",
		"LINESTRING", "line",
		"MULTIPOLYGON", "polygon",
	)

	if len(shpfiles) != 0 {
		parts := strings.Split(EN, "_")
		ENs := []string{"lngd", "tdxz", "lnbzfw", "zxcqztgh", "xzpqgh", "sthx", "yjjbnt", "ldbhyzt", "czkfbj", "kzxxxgh"}
		isos, newEN := judgeSlice(parts, ENs)

		if isos == true {
			// 判断为更新图层，提前清空内容
			if addType == "覆盖" {
				DB.Exec(fmt.Sprintf("DELETE FROM %s", newEN))
				if isEndWithNumber(newEN) {
					DB.Exec(fmt.Sprintf("DELETE FROM %s", newEN+"_mvt"))
				} else {
					DB.Exec(fmt.Sprintf("DELETE FROM %s", newEN+"mvt"))
				}
			}

			for _, item := range shpfiles {
				ConvertSHPDirectlyToPG(item, DB, newEN)
				// 创建schema记录（如果需要）
				createSchemaIfNotExists(DB, newEN)
			}
			return newEN
		} else {
			var count int64
			DB.Model(&models.MySchema{}).Where("en = ? AND cn != ?", EN, CN).Count(&count)
			if count > 0 {
				EN = EN + "_1"
			}
			var count2 int64
			DB.Model(&models.MySchema{}).Where("en = ? AND cn = ?", EN, CN).Count(&count2)
			if count2 > 0 {
				if addType == "覆盖" {
					DB.Exec(fmt.Sprintf("DELETE FROM %s", EN))
					if isEndWithNumber(EN) {
						DB.Exec(fmt.Sprintf("DELETE FROM %s", EN+"_mvt"))
					} else {
						DB.Exec(fmt.Sprintf("DELETE FROM %s", EN+"mvt"))
					}
				}

			}
			for _, item := range shpfiles {
				GEOTYPE := ConvertSHPDirectlyToPG(item, DB, EN)
				// 处理schema记录
				handleSchemaRecord(DB, EN, CN, Main, Color, Opacity, GEOTYPE, replacer, Userunits, LineWidth)
			}

			return EN
		}
	}
	return EN
}
func removeDuplicates(input []string) []string {
	m := make(map[string]bool)
	var result []string

	for _, item := range input {
		if _, found := m[item]; !found {
			m[item] = true
			result = append(result, item)
		}
	}

	return result
}

// 直接将SHP文件写入PostgreSQL数据库
func ConvertSHPDirectlyToPG(shpfileFilePath string, DB *gorm.DB, tablename string) string {
	shape, err := shp.Open(shpfileFilePath)
	if err != nil {
		log.Println("Error opening shapefile:", err)
		return ""
	}
	defer shape.Close()

	// 获取字段信息
	fields := shape.Fields()

	// 读取编码信息
	CPG := getCPGEncoding(shpfileFilePath)

	// 获取几何类型并创建表
	geoType := getGeometryType(shpfileFilePath)
	createTable(DB, tablename, fields, geoType, CPG)

	// 直接写入数据
	writeShapefileDataToDB(shape, DB, tablename, fields, CPG)

	return geoType
}

// 获取几何类型
func getGeometryType(shpfilePath string) string {
	shape, err := shp.Open(shpfilePath)
	if err != nil {
		log.Println("Error opening shapefile for geometry type:", err)
		return "GEOMETRY"
	}
	defer shape.Close()

	if shape.Next() {
		_, p := shape.Shape()
		switch p.(type) {
		case *shp.Point, *shp.PointZ, *shp.PointM:
			return "POINT"
		case *shp.PolyLine, *shp.PolyLineZ, *shp.PolyLineM:
			return "LINESTRING"
		case *shp.Polygon, *shp.PolygonZ, *shp.PolygonM:
			return "MULTIPOLYGON"
		}
	}
	return "GEOMETRY"
}

// 创建数据库表
func createTable(DB *gorm.DB, tablename string, fields []shp.Field, geoType string, CPG string) {
	var columns []string

	for _, field := range fields {
		size := int(field.Size)

		var fieldName string
		if CPG == "GBK" {
			fieldName = methods.ConvertToInitials(Transformer.GbkToUtf8(field.String()))
		} else {
			fieldName = methods.ConvertToInitials(field.String())
		}

		if size < 50 {
			size = size + 20
		} else if size < 244 {
			size = size + 10
		}
		if fieldName != "id" {
			columns = append(columns, fmt.Sprintf("%s VARCHAR(%d)", fieldName, size))
		}
	}

	columns = append(columns, "id SERIAL PRIMARY KEY")
	columns = removeDuplicates(columns)

	// 创建主表
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s, geom GEOMETRY(%s, 4326))",
		tablename, strings.Join(columns, ","), geoType)

	if err := DB.Exec(query).Error; err != nil {
		log.Println("Error creating table:", err)
	}

	// 创建MVT表
	mvtTableName := tablename + "mvt"
	if isEndWithNumber(tablename) {
		mvtTableName = tablename + "_mvt"
	}

	mvtQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (ID SERIAL PRIMARY KEY, X INT8, Y INT8, Z INT8, Byte BYTEA)", mvtTableName)
	if err := DB.Exec(mvtQuery).Error; err != nil {
		log.Println("Error creating MVT table:", err)
	}
}

// 直接写入shapefile数据到数据库
func writeShapefileDataToDB(shape *shp.Reader, DB *gorm.DB, tablename string, fields []shp.Field, CPG string) {
	const batchSize = 1000
	const workerCount = 8

	// 动态计算安全的批次大小（考虑参数限制）
	fieldCount := len(fields) + 1 // +1 for geom field
	maxSafeBatchSize := calculateSafeBatchSize(fieldCount)
	actualBatchSize := min(batchSize, maxSafeBatchSize)

	// 获取数据库表的字段信息
	dbColumns, err := getTableColumns(DB, tablename)
	if err != nil {
		log.Printf("Error getting table columns: %v", err)
		return
	}

	// 创建通道用于批量处理
	recordChan := make(chan []map[string]interface{}, workerCount*2) // 增加缓冲
	var wg sync.WaitGroup
	var insertErrors int64 // 用于统计错误

	// 启动工作协程 - 使用CreateInBatches
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localBatchSize := actualBatchSize / 2 // 进一步细分批次

			for batch := range recordChan {
				// 使用事务和CreateInBatches确保数据一致性和避免参数限制
				err := DB.Transaction(func(tx *gorm.DB) error {
					return tx.Table(tablename).CreateInBatches(batch, localBatchSize).Error
				})

				if err != nil {
					atomic.AddInt64(&insertErrors, 1)
					log.Printf("Worker %d - Error inserting batch of %d records: %v",
						workerID, len(batch), err)

					// 如果批量插入失败，尝试单条插入以找出问题记录
					if len(batch) <= 10 { // 只对小批次尝试单条插入
						for i, record := range batch {
							if err := DB.Table(tablename).Create(record).Error; err != nil {
								log.Printf("Worker %d - Failed to insert record %d: %v",
									workerID, i, err)
							}
						}
					}
				}
			}
		}(i)
	}

	// 读取和处理数据
	go func() {
		defer close(recordChan)

		var batch []map[string]interface{}
		recordCount := 0
		errorCount := 0

		for shape.Next() {
			n, p := shape.Shape()

			// 构建记录
			record := make(map[string]interface{})

			// 处理属性 - 添加字段过滤
			for k, field := range fields {
				var fieldName, fieldValue string
				if CPG == "GBK" {
					fieldName = methods.ConvertToInitials(Transformer.GbkToUtf8(field.String()))
					fieldValue = Transformer.GbkToUtf8(shape.ReadAttribute(n, k))
				} else {
					fieldName = methods.ConvertToInitials(field.String())
					fieldValue = shape.ReadAttribute(n, k)
				}

				// 转换为小写进行比较
				lowerFieldName := strings.ToLower(fieldName)

				// 只处理数据库表中存在的字段，且不是id字段
				if lowerFieldName != "id" && dbColumns[lowerFieldName] {
					// 清理字符串
					cleanValue := cleanString(fieldValue)
					record[lowerFieldName] = Transformer.TrimTrailingZeros(cleanValue)
				}
			}

			// 处理几何数据
			wkb, isTransormer := convertShapeToWKB(p)
			if wkb != "" {
				if isTransormer != "4326" {
					record["geom"] = clause.Expr{
						SQL:  fmt.Sprintf("ST_Transform(ST_Force2D(ST_SetSRID(ST_GeomFromWKB(decode(?, 'hex')),%s)), 4326)", isTransormer),
						Vars: []interface{}{wkb},
					}
				} else {
					record["geom"] = clause.Expr{
						SQL:  "ST_Force2D(ST_SetSRID(ST_GeomFromWKB(decode(?, 'hex')), 4326))",
						Vars: []interface{}{wkb},
					}
				}

				batch = append(batch, record)
				recordCount++

				// 当批次达到指定大小时发送到通道
				if len(batch) >= actualBatchSize {
					// 非阻塞发送，避免死锁
					select {
					case recordChan <- batch:
						batch = make([]map[string]interface{}, 0, actualBatchSize) // 重新分配
					default:
						log.Printf("Channel full, waiting...")
						recordChan <- batch
						batch = make([]map[string]interface{}, 0, actualBatchSize)
					}
				}
			} else {
				errorCount++
			}
		}

		// 发送剩余的记录
		if len(batch) > 0 {
			recordChan <- batch
		}

		log.Printf("Processed %d records, %d geometry errors for table %s",
			recordCount, errorCount, tablename)
	}()

	wg.Wait()

	// 报告最终统计
	if insertErrors > 0 {
		log.Printf("Completed with %d insert errors for table %s", insertErrors, tablename)
	}
}

// 计算安全的批次大小
func calculateSafeBatchSize(fieldCount int) int {
	const maxParams = 60000 // 留一些安全余量，避免接近65535限制
	safeBatchSize := maxParams / fieldCount

	// 确保批次大小在合理范围内
	if safeBatchSize > 2000 {
		return 2000
	}
	if safeBatchSize < 100 {
		return 100
	}
	return safeBatchSize
}

// min 函数
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// 将shapefile几何对象转换为WKB
func convertShapeToWKB(shape shp.Shape) (string, string) {
	var geometry orb.Geometry
	isTransform := ""
	switch s := shape.(type) {
	case *shp.Point:
		x := s.X
		isTransform = detectCoordinateSystem(x)

		geometry = orb.Point{s.X, s.Y}
	case *shp.PointZ:
		x := s.X
		isTransform = detectCoordinateSystem(x)
		geometry = orb.Point{s.X, s.Y}
	case *shp.PointM:
		x := s.X
		isTransform = detectCoordinateSystem(x)
		geometry = orb.Point{s.X, s.Y}
	case *shp.PolyLine:
		coords := make([]orb.Point, len(s.Points))
		for i, vertex := range s.Points {
			x := vertex.X
			isTransform = detectCoordinateSystem(x)
			coords[i] = orb.Point{vertex.X, vertex.Y}
		}
		geometry = orb.LineString(coords)
	case *shp.PolyLineZ:
		coords := make([]orb.Point, len(s.Points))
		for i, vertex := range s.Points {
			x := vertex.X
			isTransform = detectCoordinateSystem(x)
			coords[i] = orb.Point{vertex.X, vertex.Y}
		}
		geometry = orb.LineString(coords)
	case *shp.PolyLineM:
		coords := make([]orb.Point, len(s.Points))
		for i, vertex := range s.Points {
			x := vertex.X
			isTransform = detectCoordinateSystem(x)
			coords[i] = orb.Point{vertex.X, vertex.Y}
		}
		geometry = orb.LineString(coords)
	case *shp.Polygon:
		geometry, isTransform = Transformer.ConvertPolygonToMultiPolygon(s.Points, s.Parts)
	case *shp.PolygonZ:
		geometry, isTransform = Transformer.ConvertPolygonToMultiPolygon(s.Points, s.Parts)
	case *shp.PolygonM:
		geometry, isTransform = Transformer.ConvertPolygonToMultiPolygon(s.Points, s.Parts)
	default:
		return "", ""
	}

	return geometryToWKB(geometry), isTransform
}

// 辅助函数：获取CPG编码
func getCPGEncoding(shpfilePath string) string {
	dir := filepath.Dir(shpfilePath)
	base := filepath.Base(shpfilePath)
	cpgPath := filepath.Join(dir, strings.TrimSuffix(base, filepath.Ext(base))+".cpg")

	if data, err := os.ReadFile(cpgPath); err == nil {
		return string(data)
	}
	return "GBK"
}

// 辅助函数：清理字符串
func cleanString(s string) string {
	// 先清理无效字符
	cleaned := strings.Map(func(r rune) rune {
		if r == 0x00 || !utf8.ValidRune(r) {
			return -1
		}
		return r
	}, s)

	// 检查是否全为数字（允许前导零）
	isAllDigits := true
	for _, r := range cleaned {
		if !unicode.IsDigit(r) {
			isAllDigits = false
			break
		}
	}

	// 如果是纯数字（如 "0102"），直接返回，不解析
	if isAllDigits {
		return cleaned
	}

	// 尝试解析为浮点数（适用于 "123.45" 等情况）
	if f, err := strconv.ParseFloat(cleaned, 64); err == nil {
		return fmt.Sprintf("%g", f) // %g 自动优化格式（如 123.4500 → "123.45"）
	}

	// 其他情况：返回清理后的字符串
	return cleaned
}

// 辅助函数：处理schema记录
func handleSchemaRecord(DB *gorm.DB, EN, CN, Main, Color, Opacity, GEOTYPE string, replacer *strings.Replacer, Userunits string, LineWidth string) {
	var count int64
	DB.Model(&models.MySchema{}).Where("en = ? AND cn = ?", EN, CN).Count(&count)

	if count == 0 {
		var maxID int64
		DB.Model(&models.MySchema{}).Select("MAX(id)").Scan(&maxID)
		output := replacer.Replace(GEOTYPE)
		result := models.MySchema{
			Main:        Main,
			CN:          CN,
			EN:          EN,
			Color:       Color,
			Opacity:     Opacity,
			Userunits:   Userunits,
			LineWidth:   LineWidth,
			Type:        output,
			ID:          maxID + 1,
			UpdatedDate: time.Now().Format("2006-01-02 15:04:05"),
		}
		DB.Create(&result)
	}
}

// 辅助函数：为预定义图层创建schema
func createSchemaIfNotExists(DB *gorm.DB, newEN string) {
	var count int64
	DB.Model(&models.MySchema{}).Where("en = ?", newEN).Count(&count)
	if count == 0 {
		switch newEN {
		case "tdxz":
			DB.Create(&models.MySchema{
				Main:    "管理数据",
				CN:      "土地性质",
				EN:      newEN,
				Type:    "polygon",
				Opacity: "1",
				Color:   "",
			})
		case "lngd":
			DB.Create(&models.MySchema{
				Main:    "管理数据",
				CN:      "历年供地数据",
				EN:      newEN,
				Type:    "polygon",
				Opacity: "1",
				Color:   "#d21710",
			})
		case "lnbzfw":
			DB.Create(&models.MySchema{
				Main:    "管理数据",
				CN:      "历年报征数据",
				EN:      newEN,
				Type:    "polygon",
				Opacity: "1",
				Color:   "#042bfa",
			})
		case "zxcqztgh":
			DB.Create(&models.MySchema{
				Main:    "中心城区规划",
				CN:      "中心城区规划用地用海",
				EN:      newEN,
				Type:    "polygon",
				Opacity: "1",
				Color:   "#042bfa",
			})
		case "xzpqgh":
			DB.Create(&models.MySchema{
				Main:    "其他规划数据",
				CN:      "乡镇片区规划",
				EN:      newEN,
				Type:    "polygon",
				Opacity: "1",
				Color:   "#042bfa",
			})
		case "sthx":
			DB.Create(&models.MySchema{
				Main:    "县域国土空间规划",
				CN:      "生态红线",
				EN:      newEN,
				Type:    "polygon",
				Opacity: "1",
				Color:   "rgb(77,151,87)",
			})
		case "yjjbnt":
			DB.Create(&models.MySchema{
				Main:    "县域国土空间规划",
				CN:      "永久基本农田",
				EN:      newEN,
				Type:    "polygon",
				Opacity: "1",
				Color:   "rgb(254,254,96)",
			})
		case "ldbhyzt":
			DB.Create(&models.MySchema{
				Main:    "林业数据",
				CN:      "林地保护一张图",
				EN:      newEN,
				Type:    "polygon",
				Opacity: "1",
				Color:   "",
			})
		case "czkfbj":
			DB.Create(&models.MySchema{
				Main:    "县域国土空间规划",
				CN:      "城镇开发边界",
				EN:      newEN,
				Type:    "polygon",
				Opacity: "1",
				Color:   "rgb(77,151,87)",
			})
		case "kzxxxgh":
			DB.Create(&models.MySchema{
				Main:    "其他规划数据",
				CN:      "控制性详细规划",
				EN:      newEN,
				Type:    "polygon",
				Opacity: "1",
				Color:   "rgb(228,139,139)",
			})
		}

	}
}

// 将几何对象转换为WKB格式
func geometryToWKB(geometry orb.Geometry) string {
	if geometry == nil {
		return ""
	}

	// 使用encoding/wkb包将几何对象转换为WKB
	wkbBytes, err := wkb.Marshal(geometry)
	if err != nil {
		log.Printf("Error converting geometry to WKB: %v", err)
		return ""
	}

	// 将字节数组转换为十六进制字符串

	return hex.EncodeToString(wkbBytes)
}

func detectCoordinateSystem(x float64) string {
	if x >= 100000 && x <= 10000000 {
		return "4544"
	} else if x <= 1000 {
		return "4326"
	} else if x >= 33000000 && x <= 34000000 {
		return "4521"
	} else if x >= 34000000 && x <= 35000000 {
		return "4522"
	} else if x >= 35000000 && x <= 36000000 {
		return "4523"
	} else if x >= 36000000 && x <= 37000000 {
		return "4524"
	}
	return "4326"
}

func detectString(data []byte) string {
	detector := chardet.NewTextDetector()
	result, err := detector.DetectBest(data)
	if err != nil {
		log.Println("编码检测失败: %v", err)
	}
	return result.Charset
}

// 获取数据库表的字段信息
func getTableColumns(DB *gorm.DB, tablename string) (map[string]bool, error) {
	var columns []struct {
		ColumnName string `gorm:"column:column_name"`
	}

	query := `
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_name = ? AND table_schema = 'public'
	`

	err := DB.Raw(query, tablename).Scan(&columns).Error
	if err != nil {
		return nil, err
	}

	columnMap := make(map[string]bool)
	for _, col := range columns {
		columnMap[strings.ToLower(col.ColumnName)] = true
	}

	return columnMap, nil
}

// 使用Gdal实现
// UpdateSHPDirectly 将SHP文件直接更新到数据库中
func UpdateSHPDirectly(DB *gorm.DB, shpPath string, EN, CN, Main string, Color string, Opacity string, Userunits, AddType string, LineWidth string) []string {

	// 读取SHP文件
	shpLayer, err := Gogeo.SHPToPostGIS(shpPath)
	if err != nil {
		log.Printf("读取SHP文件失败: %v", err)
		return nil
	}

	var processedTables []string
	replacer := strings.NewReplacer(
		"POINT", "point",
		"LINESTRING", "line",
		"MULTIPOLYGON", "polygon",
		"Point", "point",
		"LineString", "line",
		"Polygon", "polygon",
		"MultiPoint", "point",
		"MultiLineString", "line",
		"MultiPolygon", "polygon",
	)

	// 处理表名，转换为合适的数据库表名
	tableName := EN

	// 检查是否为预定义图层
	parts := strings.Split(tableName, "_")
	validEN := []string{"lngd", "tdxz", "lnbzfw", "zxcqztgh", "xzpqgh", "sthx", "yjjbnt", "ldbhyzt", "czkfbj", "kzxxxgh"}
	isPreDefined, newEN := judgeSlice(validEN, parts)

	if isPreDefined {
		// 预定义图层，清空现有数据
		if AddType == "覆盖" {
			DB.Exec(fmt.Sprintf("DELETE FROM %s", newEN))
			if isEndWithNumber(newEN) {
				DB.Exec(fmt.Sprintf("DELETE FROM %s", newEN+"_mvt"))
			} else {
				DB.Exec(fmt.Sprintf("DELETE FROM %s", newEN+"mvt"))
			}
		}

		// 直接转换并写入数据
		ConvertSHPLayerToPGDirect(shpLayer, DB, newEN)
		createSchemaIfNotExists(DB, newEN)
		processedTables = append(processedTables, newEN)
	} else {
		// 普通图层，检查重名
		var count int64
		DB.Model(&models.MySchema{}).Where("en = ? AND cn != ?", tableName, CN).Count(&count)
		if count > 0 {
			tableName = tableName + "_1"
		}

		var count2 int64
		DB.Model(&models.MySchema{}).Where("en = ? AND cn = ?", tableName, CN).Count(&count2)
		if count2 > 0 {
			// 清空现有数据
			if AddType == "覆盖" {
				DB.Exec(fmt.Sprintf("DELETE FROM %s", tableName))
				if isEndWithNumber(tableName) {
					DB.Exec(fmt.Sprintf("DELETE FROM %s", tableName+"_mvt"))
				} else {
					DB.Exec(fmt.Sprintf("DELETE FROM %s", tableName+"mvt"))
				}
			}
		}

		// 直接转换并写入数据
		ConvertSHPLayerToPGDirect(shpLayer, DB, tableName)

		// 处理schema记录
		geoType := mapGeoTypeToStandard(shpLayer.GeoType)
		handleSchemaRecord(DB, tableName, CN, Main, Color, Opacity, geoType, replacer, Userunits, LineWidth)
		processedTables = append(processedTables, tableName)
	}

	return processedTables
}

// ConvertSHPLayerToPGDirect 直接将SHP图层数据写入PostgreSQL
func ConvertSHPLayerToPGDirect(layer Gogeo.SHPLayerInfo, DB *gorm.DB, tableName string) {
	if len(layer.FeatureData) == 0 {
		log.Printf("SHP文件 %s 没有要素数据", layer.LayerName)
		return
	}

	// 处理字段信息，转换字段名
	processedFields := processSHPFieldInfos(layer.FieldInfos, tableName)

	// 检查表是否存在
	var validFields map[string]string
	if tableExists(DB, tableName) {
		// 获取数据库表的现有字段
		existingFields := getTableColumns2(DB, tableName)
		// 只保留匹配的字段
		validFields = filterMatchingFieldsDirect(processedFields, existingFields)
		log.Printf("表 %s 已存在，匹配到 %d 个字段", tableName, len(validFields))
	} else {
		// 表不存在，创建新表
		validFields = make(map[string]string)
		for _, field := range processedFields {
			validFields[field.ProcessedName] = field.DBType
		}
		createSHPTableDirect(DB, tableName, validFields, layer.GeoType)
		log.Printf("创建新表 %s，包含 %d 个字段", tableName, len(validFields))
	}

	// 直接写入数据
	writeSHPDataToDBDirect(layer.FeatureData, DB, tableName, validFields, processedFields)

	log.Printf("成功导入SHP文件 %s 到表 %s，共 %d 条记录",
		layer.LayerName, tableName, len(layer.FeatureData))
}

// processSHPFieldInfos 处理SHP字段信息，转换字段名
func processSHPFieldInfos(fieldInfos []Gogeo.FieldInfo, tableName string) []ProcessedFieldInfo {
	var processedFields []ProcessedFieldInfo

	for _, field := range fieldInfos {
		if field.Name == "id" || field.Name == "ID" {
			continue
		}

		// 转换字段名
		processedName := methods.ConvertToInitials(field.Name)
		processedName = strings.ToLower(processedName)

		// 添加中文字段映射
		if containsChinese(field.Name) {
			DB := models.DB
			attColor := models.ChineseProperty{
				CName:     field.Name,
				EName:     processedName,
				LayerName: tableName,
			}
			if err := DB.Create(&attColor).Error; err != nil {
				log.Printf("Failed to create Chinese property mapping: %v", err)
			}
		}

		processedFields = append(processedFields, ProcessedFieldInfo{
			OriginalName:  field.Name,
			ProcessedName: processedName,
			DBType:        field.DBType,
		})
	}

	return processedFields
}

// createSHPTableDirect 直接创建SHP表
func createSHPTableDirect(DB *gorm.DB, tableName string, fields map[string]string, geoType string) {
	var columns []string

	// 添加属性字段
	for fieldName, fieldType := range fields {
		columns = append(columns, fmt.Sprintf("%s %s", fieldName, fieldType))
	}

	// 添加ID字段
	columns = append(columns, "id SERIAL PRIMARY KEY")

	// 创建主表
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s, geom GEOMETRY(%s, 4326))",
		tableName, strings.Join(columns, ","), geoType)

	if err := DB.Exec(query).Error; err != nil {
		log.Printf("创建表 %s 失败: %v", tableName, err)
		return
	}

	// 创建MVT表
	mvtTableName := tableName + "mvt"
	if isEndWithNumber(tableName) {
		mvtTableName = tableName + "_mvt"
	}

	mvtQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (ID SERIAL PRIMARY KEY, X INT8, Y INT8, Z INT8, Byte BYTEA)", mvtTableName)
	if err := DB.Exec(mvtQuery).Error; err != nil {
		log.Printf("创建MVT表 %s 失败: %v", mvtTableName, err)
	}
}

// writeSHPDataToDBDirect 直接将SHP数据写入数据库
func writeSHPDataToDBDirect(featureData []Gogeo.FeatureData, DB *gorm.DB, tableName string,
	validFields map[string]string, processedFields []ProcessedFieldInfo) {

	const batchSize = 1000
	const workerCount = 8
	// 动态计算安全的批次大小（考虑参数限制）
	fieldCount := len(validFields) + 1 // +1 for geom field
	maxSafeBatchSize := calculateSafeBatchSize(fieldCount)
	actualBatchSize := min(batchSize, maxSafeBatchSize)

	// 创建字段映射表
	fieldMap := make(map[string]string) // originalName -> processedName
	for _, field := range processedFields {
		fieldMap[field.OriginalName] = field.ProcessedName
	}

	// 创建通道用于批量处理
	recordChan := make(chan []map[string]interface{}, workerCount)
	var wg sync.WaitGroup
	var insertErrors int64 // 用于统计错误
	// 启动工作协程
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localBatchSize := actualBatchSize / 2 // 进一步细分批次

			for batch := range recordChan {
				// 使用事务和CreateInBatches确保数据一致性和避免参数限制
				err := DB.Transaction(func(tx *gorm.DB) error {
					return tx.Table(tableName).CreateInBatches(batch, localBatchSize).Error
				})

				if err != nil {
					atomic.AddInt64(&insertErrors, 1)
					log.Printf("Worker %d - Error inserting batch of %d records: %v",
						workerID, len(batch), err)

					// 如果批量插入失败，尝试单条插入以找出问题记录
					if len(batch) <= 10 { // 只对小批次尝试单条插入
						for i, record := range batch {
							if err := DB.Table(tableName).Create(record).Error; err != nil {
								log.Printf("Worker %d - Failed to insert record %d: %v",
									workerID, i, err)
							}
						}
					}
				}
			}
		}(i)
	}

	// 处理数据
	go func() {
		defer close(recordChan)

		var batch []map[string]interface{}

		for _, feature := range featureData {
			record := make(map[string]interface{})

			// 处理属性数据
			for originalName, value := range feature.Properties {
				if originalName == "id" || originalName == "ID" {
					continue
				}

				// 获取处理后的字段名
				if processedName, exists := fieldMap[originalName]; exists {
					// 只处理有效的字段
					if targetType, valid := validFields[processedName]; valid {
						// 根据目标字段类型转换值
						convertedValue := convertValueToTargetType(value, targetType)
						record[processedName] = convertedValue
					}
				}
			}

			// 处理几何数据
			if feature.WKBHex != "" {
				record["geom"] = clause.Expr{
					SQL:  "ST_Force2D(ST_SetSRID(ST_GeomFromWKB(decode(?, 'hex')), 4326))",
					Vars: []interface{}{feature.WKBHex},
				}

				batch = append(batch, record)

				// 批量发送
				if len(batch) >= batchSize {
					batchCopy := make([]map[string]interface{}, len(batch))
					copy(batchCopy, batch)
					recordChan <- batchCopy
					batch = batch[:0]
				}
			}
		}

		// 发送剩余记录
		if len(batch) > 0 {
			recordChan <- batch
		}
	}()

	wg.Wait()
}

// AddSHPDirectlyOptimized 优化版本：直接将SHP文件导入到PostGIS数据库
func AddSHPDirectlyOptimized(DB *gorm.DB, shpPath string, EN, CN, Main string, Color string, Opacity string, Userunits string, LineWidth string) string {

	shpLayer, err := Gogeo.SHPToPostGIS(shpPath)
	if err != nil {
		log.Printf("读取SHP文件失败: %v", err)
		return ""
	}

	var processedTables string
	replacer := strings.NewReplacer(
		"POINT", "point",
		"LINESTRING", "line",
		"MULTIPOLYGON", "polygon",
		"Point", "point",
		"LineString", "line",
		"Polygon", "polygon",
		"MultiPoint", "point",
		"MultiLineString", "line",
		"MultiPolygon", "polygon",
	)

	// 处理表名，转换为合适的数据库表名
	tableName := EN

	// 检查是否为预定义图层
	parts := strings.Split(tableName, "_")
	validEN := []string{"lngd", "tdxz", "lnbzfw", "zxcqztgh", "xzpqgh", "sthx", "yjjbnt", "ldbhyzt", "czkfbj", "kzxxxgh"}
	isPreDefined, newEN := judgeSlice(validEN, parts)

	if isPreDefined {
		// 预定义图层，清空现有数据
		DB.Exec(fmt.Sprintf("DELETE FROM %s", newEN))
		if isEndWithNumber(newEN) {
			DB.Exec(fmt.Sprintf("DELETE FROM %s", newEN+"_mvt"))
		} else {
			DB.Exec(fmt.Sprintf("DELETE FROM %s", newEN+"mvt"))
		}

		// 直接转换并写入数据
		ConvertSHPLayerToPGDirect(shpLayer, DB, newEN)
		createSchemaIfNotExists(DB, newEN)
		processedTables = newEN
	} else {
		// 普通图层，检查重名
		var count int64
		DB.Model(&models.MySchema{}).Where("en = ? AND cn != ?", tableName, CN).Count(&count)
		if count > 0 {
			tableName = tableName + "_1"
		}

		var count2 int64
		DB.Model(&models.MySchema{}).Where("en = ? AND cn = ?", tableName, CN).Count(&count2)
		if count2 > 0 {
			// 清空现有数据
			DB.Exec(fmt.Sprintf("DELETE FROM %s", tableName))
			if isEndWithNumber(tableName) {
				DB.Exec(fmt.Sprintf("DELETE FROM %s", tableName+"_mvt"))
			} else {
				DB.Exec(fmt.Sprintf("DELETE FROM %s", tableName+"mvt"))
			}
		}

		// 直接转换并写入数据
		ConvertSHPLayerToPGDirect(shpLayer, DB, tableName)

		// 处理schema记录
		geoType := mapGeoTypeToStandard(shpLayer.GeoType)
		handleSchemaRecord(DB, tableName, CN, Main, Color, Opacity, geoType, replacer, Userunits, LineWidth)
		processedTables = tableName
	}

	return processedTables
}
