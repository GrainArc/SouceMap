package pgmvt

import (
	"fmt"
	"github.com/GrainArc/Gogeo"
	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/models"
	"github.com/mozillazg/go-pinyin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"log"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
)

// tableExists 检查表是否存在
func tableExists(DB *gorm.DB, tableName string) bool {
	var count int64
	query := `
		SELECT COUNT(*) 
		FROM information_schema.tables 
		WHERE table_name = ? AND table_schema = 'public'
	`
	DB.Raw(query, tableName).Scan(&count)
	return count > 0
}

// getTableColumns 获取表的所有列信息
func getTableColumns2(DB *gorm.DB, tableName string) map[string]string {
	columns := make(map[string]string)

	query := `
		SELECT column_name, data_type, 
		       COALESCE(character_maximum_length, numeric_precision, 0) as max_length
		FROM information_schema.columns 
		WHERE table_name = ? AND table_schema = 'public' 
		AND column_name NOT IN ('id', 'geom')
	`

	rows, err := DB.Raw(query, tableName).Rows()
	if err != nil {
		log.Printf("获取表 %s 字段信息失败: %v", tableName, err)
		return columns
	}
	defer rows.Close()

	for rows.Next() {
		var columnName, dataType string
		var maxLength int
		if err := rows.Scan(&columnName, &dataType, &maxLength); err != nil {
			continue
		}

		// 转换数据类型为标准格式
		switch strings.ToUpper(dataType) {
		case "INTEGER", "BIGINT", "SMALLINT":
			columns[columnName] = "INTEGER"
		case "DOUBLE PRECISION", "NUMERIC", "REAL":
			columns[columnName] = "DOUBLE PRECISION"
		case "CHARACTER VARYING", "VARCHAR":
			if maxLength > 0 {
				columns[columnName] = fmt.Sprintf("VARCHAR(%d)", maxLength)
			} else {
				columns[columnName] = "TEXT" // 使用TEXT避免长度限制
			}
		case "TEXT":
			columns[columnName] = "TEXT"
		default:
			columns[columnName] = "TEXT" // 默认使用TEXT而不是VARCHAR(254)
		}
	}

	return columns
}

// convertValueToTargetType 根据目标字段类型转换值
func convertValueToTargetType(value interface{}, targetType string) interface{} {
	if value == nil {
		return nil
	}

	targetType = strings.ToUpper(targetType)

	switch targetType {
	case "INTEGER", "BIGINT", "SMALLINT":
		// 转换为整数
		switch v := value.(type) {
		case int, int32, int64:
			return v
		case float32, float64:
			return int(v.(float64))
		case string:
			if intVal, err := strconv.Atoi(v); err == nil {
				return intVal
			}
			return 0
		default:
			return 0
		}

	case "DOUBLE PRECISION", "NUMERIC", "REAL":
		// 转换为浮点数
		switch v := value.(type) {
		case float32, float64:
			return v
		case int, int32, int64:
			return float64(v.(int))
		case string:
			if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
				return floatVal
			}
			return 0.0
		default:
			return 0.0
		}

	case "CHARACTER VARYING", "VARCHAR", "TEXT", "CHARACTER":
		// 转换为字符串
		switch v := value.(type) {
		case string:
			return cleanString(v)
		case int, int32, int64:
			return strconv.Itoa(v.(int))
		case float32, float64:
			return strconv.FormatFloat(v.(float64), 'f', -1, 64)
		default:
			return fmt.Sprintf("%v", v)
		}

	case "BOOLEAN":
		// 转换为布尔值
		switch v := value.(type) {
		case bool:
			return v
		case string:
			return strings.ToLower(v) == "true" || v == "1"
		case int, int32, int64:
			return v.(int) != 0
		default:
			return false
		}

	default:
		// 默认转换为字符串
		if strValue, ok := value.(string); ok {
			return cleanString(strValue)
		}
		return fmt.Sprintf("%v", value)
	}
}
func containsChinese(s string) bool {
	for _, r := range s {
		// 判断是否为中日韩统一表意文字（CJK Unified Ideographs）
		if unicode.Is(unicode.Scripts["Han"], r) {
			return true
		}
	}
	return false
}

func moveLeadingNumbersToEnd(s string) string {
	// 定义正则表达式，匹配字符串开头的数字
	re := regexp.MustCompile(`^(\d+)(.*)$`)
	// 使用正则表达式提取匹配部分
	match := re.FindStringSubmatch(s)
	// match[0] 是整个匹配字符串，match[1] 是前导数字，match[2] 是剩余部分
	if len(match) == 3 {
		return match[2] + match[1]
	}
	// 如果没有找到匹配的前导数字，就返回原字符串
	return s
}
func filterString(str string) string {
	// 定义正则表达式，匹配中文、英文、数字和下划线
	reg := regexp.MustCompile("[^\\p{Han}\\p{Latin}\\p{N}_]")

	// 使用正则表达式替换掉非中文、英文、数字和下划线的字符
	result := reg.ReplaceAllString(str, "")

	// 去除字符串中的空格
	result = strings.ReplaceAll(result, " ", "")

	return result
}

// 辅助函数

func convertToInitials(hanzi string) string {
	// 配置选项，选择带声调和不带声调的组合，并提取首字母
	hanzi = filterString(hanzi)
	a := pinyin.NewArgs()
	a.Style = pinyin.FirstLetter // 设置拼音风格为首字母
	var result string
	for _, runeValue := range hanzi {
		if unicode.Is(unicode.Han, runeValue) {
			// 如果是汉字，则获取拼音首字母
			pinyinSlice := pinyin.SinglePinyin(runeValue, a)
			if len(pinyinSlice) > 0 {
				result += pinyinSlice[0]
			}
		} else {
			// 如果不是汉字，则直接保留字符
			result += string(runeValue)
		}
	}
	processed := moveLeadingNumbersToEnd(result)
	str := strings.ToLower(processed)
	return str
}

// sanitizeTableName 清理表名，确保符合数据库命名规范
func sanitizeTableName(name string) string {
	// 转换为小写
	name = strings.ToLower(convertToInitials(name))
	// 移除特殊字符，只保留字母、数字和下划线
	result := ""
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			result += string(r)
		} else {
			result += "_"
		}
	}
	// 确保不以数字开头
	if len(result) > 0 && result[0] >= '0' && result[0] <= '9' {
		result = "t_" + result
	}
	return result
}

// mapGeoTypeToStandard 将几何类型映射为标准类型（用于schema）
func mapGeoTypeToStandard(geoType string) string {
	switch strings.ToUpper(geoType) {
	case "POINT", "POINT25D", "MULTIPOINT", "MULTIPOINT25D":
		return "POINT"
	case "LINESTRING", "LINESTRING25D", "MULTILINESTRING", "MULTILINESTRING25D":
		return "LINESTRING"
	case "POLYGON", "POLYGON25D", "MULTIPOLYGON", "MULTIPOLYGON25D":
		return "MULTIPOLYGON"
	default:
		return "GEOMETRY"
	}
}

type SourceConfig struct {
	SourcePath      string               `json:"source_path"`
	SourceLayerName string               `json:"source_layer_name"`
	SourceLayerSRS  int                  `json:"source_layer_srs"` //EPSG代码
	KeyAttribute    string               `json:"key_attribute"`
	AttMap          []ProcessedFieldInfo `json:"att_map"`
}

// AddGDBDirectlyOptimized 优化后的GDB导入函数
func AddGDBDirectlyOptimized(DB *gorm.DB, gdbPath string, targetLayers []string, Main string, Color string, Opacity string, Userunits string, LineWidth string) []string {
	layers, err := Gogeo.GDBToPostGIS(gdbPath, targetLayers)
	if err != nil {
		log.Printf("读取GDB文件失败: %v", err)
		return nil
	}
	// 读取GDB元数据，获取图层别名和路径信息
	metadataCollection, err := Gogeo.ReadGDBLayerMetadata(gdbPath)
	if err != nil {
		log.Printf("读取GDB元数据失败: %v, 将使用默认值", err)
		metadataCollection = nil
	}
	// 获取GDB文件名（不含扩展名）
	gdbFileName := filepath.Base(gdbPath)
	gdbFileName = strings.TrimSuffix(gdbFileName, filepath.Ext(gdbFileName))
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
	for _, layer := range layers {
		// 处理表名，转换为合适的数据库表名
		var SC SourceConfig
		SC.SourcePath = gdbPath
		SC.SourceLayerName = layer.LayerName
		SC.KeyAttribute = "fid"
		tableName := sanitizeTableName(Main + "_" + layer.LayerName)
		// 获取图层元数据
		var layerMeta *Gogeo.GDBLayerMetaData
		if metadataCollection != nil {
			layerMeta = metadataCollection.GetLayerByName(layer.LayerName)
		}
		SC.SourceLayerSRS = layerMeta.EPSG
		// 确定CN（中文名）：优先使用AliasName，否则使用Name
		layerCN := layer.LayerName
		if layerMeta != nil && layerMeta.AliasName != "" && layerMeta.AliasName != layerMeta.Name {
			layerCN = layerMeta.AliasName
		}
		// 构建完整的Main路径：原始Main/GDB文件名/图层路径（去掉图层名）
		fullMain := buildFullMainPath(Main, gdbFileName, layerMeta)
		// 普通图层，检查重名
		var count int64
		DB.Model(&models.MySchema{}).Where("en = ? AND cn != ?", tableName, layerCN).Count(&count)
		if count > 0 {
			tableName = tableName + "_1"
		}
		var count2 int64
		DB.Model(&models.MySchema{}).Where("en = ? AND cn = ?", tableName, layerCN).Count(&count2)
		if count2 > 0 {
			// 清空现有数据
			DB.Exec(fmt.Sprintf("DELETE FROM %s", tableName))
			if isEndWithNumber(tableName) {
				DB.Exec(fmt.Sprintf("DELETE FROM %s", tableName+"_mvt"))
			} else {
				DB.Exec(fmt.Sprintf("DELETE FROM %s", tableName+"mvt"))
			}
		}
		// 直接转换并写入数据，传入图层元数据用于字段别名处理
		AttMap := ConvertGDBLayerToPGDirectWithMeta(layer, DB, tableName, layerMeta)
		SC.AttMap = AttMap
		// 处理schema记录，使用tableName作为EN，layerCN作为CN，fullMain作为Main
		geoType := mapGeoTypeToStandard(layer.GeoType)
		handleSchemaRecord(DB, tableName, layerCN, fullMain, Color, Opacity, geoType, replacer, Userunits, LineWidth, SC)
		processedTables = append(processedTables, tableName)
	}
	return processedTables
}

// buildFullMainPath 构建完整的Main路径
// 格式：原始Main/GDB文件名/图层路径（不含图层名本身）
// 例如：Main="道路", gdbFileName="城市道路", layerPath="/成都市/匝道/LCTL"
// 结果："道路/城市道路/成都市/匝道"
func buildFullMainPath(originalMain string, gdbFileName string, layerMeta *Gogeo.GDBLayerMetaData) string {
	// 基础路径：原始Main/GDB文件名
	basePath := originalMain
	if gdbFileName != "" {
		basePath = originalMain + "/" + gdbFileName
	}
	// 如果没有元数据或路径为空，直接返回基础路径
	if layerMeta == nil || layerMeta.Path == "" {
		return basePath
	}
	// 处理图层路径
	layerPath := layerMeta.Path
	// 统一路径分隔符
	layerPath = strings.ReplaceAll(layerPath, "\\", "/")
	// 移除开头的斜杠
	layerPath = strings.TrimPrefix(layerPath, "/")
	// 分割路径
	pathParts := strings.Split(layerPath, "/")
	// 如果路径有多个部分，移除最后一个（图层名本身）
	if len(pathParts) > 1 {
		// 取除最后一个元素外的所有部分
		parentPath := strings.Join(pathParts[:len(pathParts)-1], "/")
		if parentPath != "" {
			return basePath + "/" + parentPath
		}
	}
	return basePath
}

// ConvertGDBLayerToPGDirectWithMeta 带元数据的图层转换函数
func ConvertGDBLayerToPGDirectWithMeta(layer Gogeo.GDBLayerInfo, DB *gorm.DB, tableName string, layerMeta *Gogeo.GDBLayerMetaData) []ProcessedFieldInfo {
	if len(layer.FeatureData) == 0 {
		log.Printf("图层 %s 没有要素数据", layer.LayerName)
		return nil
	}
	// 处理字段信息，转换字段名，传入图层元数据用于获取字段别名
	processedFields := processFieldInfosWithMeta(layer.FieldInfos, tableName, layerMeta)
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
		createGDBTableDirect(DB, tableName, validFields, layer.GeoType)
		log.Printf("创建新表 %s，包含 %d 个字段", tableName, len(validFields))
	}
	// 直接写入数据
	writeGDBDataToDBDirect(layer.FeatureData, DB, tableName, validFields, processedFields)
	log.Printf("成功导入图层 %s 到表 %s，共 %d 条记录",
		layer.LayerName, tableName, len(layer.FeatureData))
	return processedFields
}

// processFieldInfosWithMeta 处理字段信息，支持从元数据获取字段别名
func processFieldInfosWithMeta(fieldInfos []Gogeo.FieldInfo, tableName string, layerMeta *Gogeo.GDBLayerMetaData) []ProcessedFieldInfo {
	var processedFields []ProcessedFieldInfo
	// 构建字段别名映射（从元数据）
	fieldAliasMap := make(map[string]string)
	if layerMeta != nil {
		for _, field := range layerMeta.Fields {
			fieldAliasMap[field.Name] = field.AliasName
		}
	}
	for _, field := range fieldInfos {
		if field.Name == "id" || field.Name == "ID" {
			continue
		}
		// 转换字段名
		processedName := methods.ConvertToInitials(field.Name)
		processedName = strings.ToLower(processedName)
		// 获取字段别名
		aliasName := ""
		if alias, exists := fieldAliasMap[field.Name]; exists {
			aliasName = alias
		}
		// 判断是否需要创建中文属性映射
		// 条件1：Name和AliasName不一致（有别名）
		// 条件2：字段名包含中文
		needChineseMapping := false
		chineseName := ""
		if aliasName != "" && aliasName != field.Name {
			// Name和AliasName不一致，使用AliasName作为CName
			needChineseMapping = true
			chineseName = aliasName
		} else if containsChinese(field.Name) {
			// Name和AliasName一致（或没有AliasName），但字段名包含中文
			needChineseMapping = true
			chineseName = field.Name
		}
		// 创建中文属性映射
		if needChineseMapping && chineseName != "" {
			DB := models.DB
			attColor := models.ChineseProperty{
				CName:     chineseName,
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

// 将gdb文件图层全部更新到数据库中
func UpdateGDBDirectly(DB *gorm.DB, gdbPath string, EN, CN, Main string, Color string, Opacity string, Userunits, AddType string, LineWidth string) []string {

	layers, err := Gogeo.GDBToPostGIS(gdbPath, nil)
	if err != nil {
		log.Printf("读取GDB文件失败: %v", err)
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

	for _, layer := range layers {
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
			ConvertGDBLayerToPGDirect(layer, DB, newEN)
			createSchemaIfNotExists(DB, newEN)
			processedTables = append(processedTables, newEN)
		} else {
			// 普通图层，检查重名
			var count int64
			DB.Model(&models.MySchema{}).Where("en = ? AND cn != ?", tableName, layer.LayerName).Count(&count)
			if count > 0 {
				tableName = tableName + "_1"
			}

			var count2 int64
			DB.Model(&models.MySchema{}).Where("en = ? AND cn = ?", tableName, layer.LayerName).Count(&count2)
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
			ConvertGDBLayerToPGDirect(layer, DB, tableName)

			// 处理schema记录
			geoType := mapGeoTypeToStandard(layer.GeoType)
			handleSchemaRecord(DB, tableName, CN, Main, Color, Opacity, geoType, replacer, Userunits, LineWidth)
			processedTables = append(processedTables, tableName)
		}
	}

	return processedTables
}

// 直接将GDB图层数据写入PostgreSQL
func ConvertGDBLayerToPGDirect(layer Gogeo.GDBLayerInfo, DB *gorm.DB, tableName string) []ProcessedFieldInfo {
	return ConvertGDBLayerToPGDirectWithMeta(layer, DB, tableName, nil)
}

// ProcessedFieldInfo 处理后的字段信息
type ProcessedFieldInfo struct {
	OriginalName  string
	ProcessedName string
	DBType        string
}

// 处理字段信息，转换字段名
func processFieldInfos(fieldInfos []Gogeo.FieldInfo, tableName string) []ProcessedFieldInfo {
	return processFieldInfosWithMeta(fieldInfos, tableName, nil)
}

// filterMatchingFieldsDirect 过滤匹配的字段（直接版本）
func filterMatchingFieldsDirect(processedFields []ProcessedFieldInfo, existingFields map[string]string) map[string]string {
	validFields := make(map[string]string)

	for _, field := range processedFields {
		if existingType, exists := existingFields[field.ProcessedName]; exists {
			validFields[field.ProcessedName] = existingType
		} else {
			log.Printf("字段 %s 在数据库表中不存在，将被跳过", field.ProcessedName)
		}
	}

	return validFields
}

// createGDBTableDirect 直接创建GDB表
func createGDBTableDirect(DB *gorm.DB, tableName string, fields map[string]string, geoType string) {
	var columns []string

	// 添加属性字段

	for fieldName, fieldType := range fields {
		// 跳过id字段(不区分大小写)
		if strings.EqualFold(fieldName, "id") {
			continue
		}
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

// writeGDBDataToDBDirect 直接将GDB数据写入数据库
func writeGDBDataToDBDirect(featureData []Gogeo.FeatureData, DB *gorm.DB, tableName string,
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
		recordCount := 0
		errorCount := 0
		skippedCount := 0

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
				recordCount++

				// 批量发送
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
				// 记录没有几何数据的要素
				skippedCount++
			}
		}

		// 发送剩余记录
		if len(batch) > 0 {
			recordChan <- batch
		}

		log.Printf("GDB data processed: %d records inserted, %d skipped (no geometry), %d errors for table %s",
			recordCount, skippedCount, errorCount, tableName)
	}()

	wg.Wait()

	// 报告最终统计
	if insertErrors > 0 {
		log.Printf("GDB import completed with %d insert errors for table %s", insertErrors, tableName)
	} else {
		log.Printf("GDB import completed successfully for table %s", tableName)
	}
}
