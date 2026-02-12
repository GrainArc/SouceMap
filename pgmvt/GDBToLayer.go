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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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

	// 提取基础类型（处理 VARCHAR(255) 这种带长度的类型）
	baseType := targetType
	if idx := strings.Index(targetType, "("); idx != -1 {
		baseType = targetType[:idx]
	}

	switch baseType {
	case "INTEGER", "INT", "INT4", "BIGINT", "INT8", "SMALLINT", "INT2":
		// 转换为整数
		switch v := value.(type) {
		case int:
			return v
		case int32:
			return int(v)
		case int64:
			return v
		case float32:
			return int(v)
		case float64:
			return int(v)
		case string:
			if v == "" {
				return nil
			}
			if intVal, err := strconv.Atoi(v); err == nil {
				return intVal
			}
			return nil
		default:
			return nil
		}

	case "DOUBLE PRECISION", "NUMERIC", "REAL", "FLOAT", "FLOAT4", "FLOAT8", "DECIMAL":
		// 转换为浮点数
		switch v := value.(type) {
		case float32:
			return float64(v)
		case float64:
			return v
		case int:
			return float64(v)
		case int32:
			return float64(v)
		case int64:
			return float64(v)
		case string:
			if v == "" {
				return nil
			}
			if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
				return floatVal
			}
			return nil
		default:
			return nil
		}

	case "CHARACTER VARYING", "VARCHAR", "TEXT", "CHARACTER", "CHAR":
		// 转换为字符串
		switch v := value.(type) {
		case string:
			return cleanString(v)
		case int:
			return strconv.Itoa(v)
		case int32:
			return strconv.Itoa(int(v))
		case int64:
			return strconv.FormatInt(v, 10)
		case float32:
			return strconv.FormatFloat(float64(v), 'f', -1, 32)
		case float64:
			return strconv.FormatFloat(v, 'f', -1, 64)
		default:
			return fmt.Sprintf("%v", v)
		}

	case "BOOLEAN", "BOOL":
		// 转换为布尔值
		switch v := value.(type) {
		case bool:
			return v
		case string:
			if v == "" {
				return nil
			}
			lower := strings.ToLower(v)
			return lower == "true" || v == "1" || lower == "yes" || lower == "t"
		case int:
			return v != 0
		case int32:
			return v != 0
		case int64:
			return v != 0
		default:
			return nil
		}

	case "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ":
		// 转换为时间戳
		return convertToTimestamp(value)

	case "DATE":
		// 转换为日期
		return convertToDate(value)

	case "TIME", "TIME WITHOUT TIME ZONE", "TIME WITH TIME ZONE", "TIMETZ":
		// 转换为时间
		return convertToTime(value)

	default:
		// 默认处理
		// 检查是否是带长度的类型，如 VARCHAR(255)
		if strings.HasPrefix(baseType, "VARCHAR") || strings.HasPrefix(baseType, "CHARACTER") {
			if strValue, ok := value.(string); ok {
				return cleanString(strValue)
			}
			return fmt.Sprintf("%v", value)
		}

		// 对于未知类型，如果是空字符串则返回 nil
		if strValue, ok := value.(string); ok {
			if strValue == "" {
				return nil
			}
			return cleanString(strValue)
		}
		return fmt.Sprintf("%v", value)
	}
}

// convertToTimestamp 转换为时间戳
func convertToTimestamp(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case string:
		if v == "" {
			return nil
		}
		// 尝试解析常见的日期时间格式
		formats := []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02T15:04:05Z",
			"2006-01-02T15:04:05Z07:00",
			"2006-01-02 15:04:05.000",
			"2006-01-02 15:04:05.000000",
			"2006/01/02 15:04:05",
			"01/02/2006 15:04:05",
			"02-Jan-2006 15:04:05",
			"2006-01-02",
			"2006/01/02",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t
			}
		}
		// 如果无法解析，记录警告并返回 nil
		log.Printf("警告: 无法解析时间戳值: %s", v)
		return nil
	case time.Time:
		if v.IsZero() {
			return nil
		}
		return v
	default:
		return nil
	}
}

// convertToDate 转换为日期
func convertToDate(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case string:
		if v == "" {
			return nil
		}
		// 尝试解析常见的日期格式
		formats := []string{
			"2006-01-02",
			"2006/01/02",
			"01/02/2006",
			"02-01-2006",
			"02-Jan-2006",
			"Jan 02, 2006",
			"2006-01-02 15:04:05", // 如果包含时间，只取日期部分
			"2006-01-02T15:04:05",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				// 返回 time.Time 类型，GORM 会正确处理
				return t
			}
		}
		log.Printf("警告: 无法解析日期值: %s", v)
		return nil
	case time.Time:
		if v.IsZero() {
			return nil
		}
		return v
	default:
		return nil
	}
}

// convertToTime 转换为时间
func convertToTime(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case string:
		if v == "" {
			return nil
		}
		// 尝试解析常见的时间格式
		formats := []string{
			"15:04:05",
			"15:04",
			"15:04:05.000",
			"15:04:05.000000",
			"3:04:05 PM",
			"3:04 PM",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t.Format("15:04:05")
			}
		}
		log.Printf("警告: 无法解析时间值: %s", v)
		return nil
	case time.Time:
		if v.IsZero() {
			return nil
		}
		return v.Format("15:04:05")
	default:
		return nil
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

func InitOriginMapping(db *gorm.DB, tableName string, keyField string) error {
	// keyField: "objectid" for shp, "fid" for gdb
	sql := fmt.Sprintf(`
        INSERT INTO origin_mappings (table_name, post_gisid, source_object_id, origin, session_id, is_deleted)
        SELECT '%s', id, "%s", 'original', 0, false
        FROM "%s"
    `, tableName, keyField, tableName)
	return db.Exec(sql).Error
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
		InitOriginMapping(DB, tableName, "fid")
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

// GeometryTypeCategory 几何类型分类
type GeometryTypeCategory int

const (
	GeomCategoryUnknown GeometryTypeCategory = iota
	GeomCategoryPoint
	GeomCategoryLine
	GeomCategoryPolygon
)

// GeometryConversionResult 几何转换结果
type GeometryConversionResult struct {
	ConvertedWKBHex string
	Success         bool
	ErrorMsg        string
	WasConverted    bool // 是否进行了转换
}

// getGeometryCategory 获取几何类型分类
func getGeometryCategory(geoType string) GeometryTypeCategory {
	geoType = strings.ToUpper(geoType)

	switch {
	case strings.Contains(geoType, "POINT"):
		return GeomCategoryPoint
	case strings.Contains(geoType, "LINE") ||
		strings.Contains(geoType, "CURVE") && !strings.Contains(geoType, "POLYGON"):
		return GeomCategoryLine
	case strings.Contains(geoType, "POLYGON") ||
		strings.Contains(geoType, "SURFACE"): // 关键：SURFACE 类型归类为 Polygon
		return GeomCategoryPolygon
	default:
		return GeomCategoryUnknown
	}
}

// getTargetGeometryType 根据分类获取目标几何类型
func getTargetGeometryType(category GeometryTypeCategory, isMulti bool) string {
	switch category {
	case GeomCategoryPoint:
		if isMulti {
			return "MultiPoint"
		}
		return "Point"
	case GeomCategoryLine:
		if isMulti {
			return "MultiLineString"
		}
		return "LineString"
	case GeomCategoryPolygon:
		if isMulti {
			return "MultiPolygon"
		}
		return "Polygon"
	default:
		return ""
	}
}

// buildGeometryConversionSQL 构建几何转换SQL表达式 - 简化可靠版
func buildGeometryConversionSQL(wkbHex string, targetGeoType string) clause.Expr {
	targetGeoType = strings.ToUpper(targetGeoType)

	var sql string

	switch targetGeoType {
	case "POINT":
		sql = `ST_PointOnSurface(ST_Force2D(ST_SetSRID(ST_GeomFromWKB(decode(?, 'hex')), 4326)))`

	case "MULTIPOINT":
		sql = `ST_Multi(ST_Force2D(ST_SetSRID(ST_GeomFromWKB(decode(?, 'hex')), 4326)))::geometry(MultiPoint, 4326)`

	case "LINESTRING":
		sql = `ST_GeometryN(ST_Multi(ST_CurveToLine(ST_Force2D(ST_SetSRID(ST_GeomFromWKB(decode(?, 'hex')), 4326)))), 1)`

	case "MULTILINESTRING":
		sql = `ST_Multi(ST_CurveToLine(ST_Force2D(ST_SetSRID(ST_GeomFromWKB(decode(?, 'hex')), 4326))))::geometry(MultiLineString, 4326)`

	case "POLYGON":
		sql = `ST_GeometryN(
			ST_Multi(
				ST_MakeValid(
					ST_CurveToLine(
						ST_Force2D(ST_SetSRID(ST_GeomFromWKB(decode(?, 'hex')), 4326))
					)
				)
			), 
			1
		)`

	case "MULTIPOLYGON":
		// 关键：使用 ST_MakeValid 确保几何有效，然后强制转换类型
		sql = `ST_Multi(
			ST_MakeValid(
				ST_CurveToLine(
					ST_Force2D(ST_SetSRID(ST_GeomFromWKB(decode(?, 'hex')), 4326))
				)
			)
		)::geometry(MultiPolygon, 4326)`

	default:
		sql = `ST_Force2D(ST_SetSRID(ST_GeomFromWKB(decode(?, 'hex')), 4326))`
	}

	return clause.Expr{
		SQL:  sql,
		Vars: []interface{}{wkbHex},
	}
}

// buildSimpleGeometrySQL 构建简单的几何SQL（用于已知类型匹配的情况）
func buildSimpleGeometrySQL(wkbHex string, targetGeoType string) clause.Expr {
	targetGeoType = strings.ToLower(targetGeoType)
	isMultiTarget := strings.Contains(targetGeoType, "multi")

	var sql string
	if isMultiTarget {
		// 对于Multi类型目标，确保结果是Multi
		sql = `ST_Multi(ST_Force2D(ST_SetSRID(ST_GeomFromWKB(decode(?, 'hex')), 4326)))`
	} else {
		sql = `ST_Force2D(ST_SetSRID(ST_GeomFromWKB(decode(?, 'hex')), 4326))`
	}

	return clause.Expr{
		SQL:  sql,
		Vars: []interface{}{wkbHex},
	}
}

// preCheckGeometryType 预检测几何类型（批量检测以提高性能）
func preCheckGeometryType(DB *gorm.DB, wkbHex string) (string, error) {
	var result struct {
		GeomType string
	}

	err := DB.Raw(`
		SELECT GeometryType(ST_GeomFromWKB(decode(?, 'hex'))) as geom_type
	`, wkbHex).Scan(&result).Error

	if err != nil {
		return "", err
	}

	return result.GeomType, nil
}

// batchPreCheckGeometryTypes 批量预检测几何类型
func batchPreCheckGeometryTypes(DB *gorm.DB, wkbHexList []string) (map[string]string, error) {
	result := make(map[string]string)

	if len(wkbHexList) == 0 {
		return result, nil
	}

	// 使用UNION ALL批量查询，每批最多100个
	const checkBatchSize = 100

	for i := 0; i < len(wkbHexList); i += checkBatchSize {
		end := i + checkBatchSize
		if end > len(wkbHexList) {
			end = len(wkbHexList)
		}

		batch := wkbHexList[i:end]

		// 构建批量查询
		var queryParts []string
		var args []interface{}

		for idx, wkb := range batch {
			queryParts = append(queryParts, fmt.Sprintf(
				"SELECT %d as idx, ? as wkb, GeometryType(ST_GeomFromWKB(decode(?, 'hex'))) as geom_type",
				i+idx,
			))
			args = append(args, wkb, wkb)
		}

		query := strings.Join(queryParts, " UNION ALL ")

		var rows []struct {
			Idx      int
			Wkb      string
			GeomType string
		}

		if err := DB.Raw(query, args...).Scan(&rows).Error; err != nil {
			// 如果批量查询失败，回退到单个查询
			for _, wkb := range batch {
				geomType, err := preCheckGeometryType(DB, wkb)
				if err == nil {
					result[wkb] = geomType
				}
			}
			continue
		}

		for _, row := range rows {
			result[row.Wkb] = row.GeomType
		}
	}

	return result, nil
}

// isGeometryTypeCompatible 检查几何类型是否兼容
func isGeometryTypeCompatible(sourceType, targetType string) bool {
	sourceCategory := getGeometryCategory(sourceType)
	targetCategory := getGeometryCategory(targetType)

	// 同类型兼容
	if sourceCategory == targetCategory {
		return true
	}

	// GeometryCollection可能包含任何类型
	if strings.ToUpper(sourceType) == "GEOMETRYCOLLECTION" {
		return true // 需要进一步检查是否包含目标类型
	}

	return false
}

// needsConversion 检查是否需要转换
func needsConversion(sourceType, targetType string) bool {
	sourceType = strings.ToUpper(sourceType)
	targetType = strings.ToUpper(targetType)

	// 完全匹配不需要转换
	if sourceType == targetType {
		return false
	}

	// Multi和非Multi之间需要转换
	// Surface类型需要转换为Polygon
	// Curve类型需要转换为Line

	return true
}

// FeatureWriteResult 要素写入结果
type FeatureWriteResult struct {
	TotalCount      int64
	SuccessCount    int64
	ConvertedCount  int64
	SkippedCount    int64
	ErrorCount      int64
	SkippedFeatures []SkippedFeatureInfo
}

// SkippedFeatureInfo 跳过的要素信息
type SkippedFeatureInfo struct {
	Index      int
	SourceType string
	TargetType string
	Reason     string
}

// FailedFeature 失败的要素信息
type FailedFeature struct {
	Record   map[string]interface{}
	WKBHex   string
	ErrorMsg string
	Index    int
}

func writeGDBDataToDBDirect(featureData []Gogeo.FeatureData, DB *gorm.DB, tableName string,
	validFields map[string]string, processedFields []ProcessedFieldInfo, targetGeoType string) *FeatureWriteResult {
	result := &FeatureWriteResult{}
	const batchSize = 500
	workerCount := runtime.NumCPU() / 2
	// 动态计算安全的批次大小
	fieldCount := len(validFields) + 1
	maxSafeBatchSize := calculateSafeBatchSize(fieldCount)
	actualBatchSize := min(batchSize, maxSafeBatchSize)
	// 创建字段映射表
	fieldMap := make(map[string]string)
	for _, field := range processedFields {
		fieldMap[field.OriginalName] = field.ProcessedName
	}
	// 标准化目标几何类型
	normalizedTargetGeoType := strings.ToUpper(targetGeoType)
	log.Printf("目标表 %s 几何类型: %s", tableName, normalizedTargetGeoType)
	// 收集失败的要素
	var failedFeaturesMu sync.Mutex
	var failedFeatures []FailedFeature
	// 创建通道
	type BatchItem struct {
		Records []map[string]interface{}
		WKBHexs []string // 保存原始WKB用于失败重试
		Indices []int    // 保存原始索引
	}
	recordChan := make(chan BatchItem, workerCount*2)
	var wg sync.WaitGroup
	var successCount, errorCount int64
	// 启动工作协程
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for batchItem := range recordChan {
				batch := batchItem.Records
				wkbHexs := batchItem.WKBHexs
				indices := batchItem.Indices
				// 直接批量插入，不使用事务
				err := DB.Table(tableName).CreateInBatches(batch, actualBatchSize/2).Error

				if err != nil {
					// 批量插入失败，尝试单条插入
					log.Printf("Worker %d - 批量插入失败，尝试单条插入: %v", workerID, err)
					for j, record := range batch {
						if err := DB.Table(tableName).Create(record).Error; err != nil {
							// 检查是否是几何类型不匹配错误
							if strings.Contains(err.Error(), "Geometry type") ||
								strings.Contains(err.Error(), "geometry") {
								// 收集失败的要素，稍后进行转换重试
								failedFeaturesMu.Lock()
								failedFeatures = append(failedFeatures, FailedFeature{
									Record:   record,
									WKBHex:   wkbHexs[j],
									ErrorMsg: err.Error(),
									Index:    indices[j],
								})
								failedFeaturesMu.Unlock()
							} else {
								// 其他错误直接计入错误数
								atomic.AddInt64(&errorCount, 1)
								log.Printf("Worker %d - 插入失败: %v", workerID, err)
							}
						} else {
							atomic.AddInt64(&successCount, 1)
						}
					}
				} else {
					atomic.AddInt64(&successCount, int64(len(batch)))
				}
			}
		}(i)
	}
	// 处理数据
	go func() {
		defer close(recordChan)
		var batch []map[string]interface{}
		var wkbHexs []string
		var indices []int
		skippedCount := 0
		var skippedFeatures []SkippedFeatureInfo
		for idx, feature := range featureData {
			if feature.WKBHex == "" {
				skippedCount++
				skippedFeatures = append(skippedFeatures, SkippedFeatureInfo{
					Index:  idx,
					Reason: "无几何数据",
				})
				continue
			}
			record := make(map[string]interface{})
			// 处理属性数据
			for originalName, value := range feature.Properties {
				if originalName == "id" || originalName == "ID" {
					continue
				}
				if processedName, exists := fieldMap[originalName]; exists {
					if targetType, valid := validFields[processedName]; valid {
						convertedValue := convertValueToTargetType(value, targetType)
						record[processedName] = convertedValue
					}
				}
			}
			// 使用简单的几何SQL（原有逻辑）
			record["geom"] = buildSimpleGeometrySQL(feature.WKBHex, targetGeoType)
			batch = append(batch, record)
			wkbHexs = append(wkbHexs, feature.WKBHex)
			indices = append(indices, idx)
			// 批量发送
			if len(batch) >= actualBatchSize {
				recordChan <- BatchItem{
					Records: batch,
					WKBHexs: wkbHexs,
					Indices: indices,
				}
				batch = make([]map[string]interface{}, 0, actualBatchSize)
				wkbHexs = make([]string, 0, actualBatchSize)
				indices = make([]int, 0, actualBatchSize)
			}
		}
		// 发送剩余记录
		if len(batch) > 0 {
			recordChan <- BatchItem{
				Records: batch,
				WKBHexs: wkbHexs,
				Indices: indices,
			}
		}
		result.SkippedCount = int64(skippedCount)
		result.SkippedFeatures = skippedFeatures
		log.Printf("数据处理完成: 总计 %d 条, 跳过 %d 条 (表: %s)",
			len(featureData), skippedCount, tableName)
	}()
	wg.Wait()

	// 第一阶段完成，统计结果
	firstPassSuccess := successCount
	firstPassErrors := len(failedFeatures)

	log.Printf("第一阶段完成 [%s]: 成功=%d, 需要转换=%d",
		tableName, firstPassSuccess, firstPassErrors)

	// 第二阶段：对失败的要素进行几何转换后重试
	var convertedCount int64
	if len(failedFeatures) > 0 {
		log.Printf("开始第二阶段：对 %d 个失败要素进行几何转换重试", len(failedFeatures))

		convertedSuccess, convertedErrors := retryFailedFeaturesWithConversion(
			DB, tableName, failedFeatures, normalizedTargetGeoType,
		)

		convertedCount = convertedSuccess
		atomic.AddInt64(&successCount, convertedSuccess)
		atomic.AddInt64(&errorCount, convertedErrors)

		log.Printf("第二阶段完成 [%s]: 转换成功=%d, 转换失败=%d",
			tableName, convertedSuccess, convertedErrors)
	}

	result.TotalCount = int64(len(featureData))
	result.SuccessCount = successCount
	result.ConvertedCount = convertedCount
	result.ErrorCount = errorCount

	// 输出详细统计
	log.Printf("导入完成 [%s]: 总计=%d, 成功=%d (直接=%d, 转换=%d), 跳过=%d, 错误=%d",
		tableName, result.TotalCount, result.SuccessCount,
		firstPassSuccess, convertedCount, result.SkippedCount, result.ErrorCount)

	if len(result.SkippedFeatures) > 0 && len(result.SkippedFeatures) <= 10 {
		for _, sf := range result.SkippedFeatures {
			log.Printf("  跳过要素 #%d: %s (%s -> %s)",
				sf.Index, sf.Reason, sf.SourceType, sf.TargetType)
		}
	} else if len(result.SkippedFeatures) > 10 {
		log.Printf("  跳过要素过多，仅显示前10条...")
		for i := 0; i < 10; i++ {
			sf := result.SkippedFeatures[i]
			log.Printf("  跳过要素 #%d: %s (%s -> %s)",
				sf.Index, sf.Reason, sf.SourceType, sf.TargetType)
		}
	}

	return result
}

// retryFailedFeaturesWithConversion 对失败的要素进行几何转换后重试插入
func retryFailedFeaturesWithConversion(DB *gorm.DB, tableName string, failedFeatures []FailedFeature, targetGeoType string) (successCount int64, errorCount int64) {
	if len(failedFeatures) == 0 {
		return 0, 0
	}

	const retryBatchSize = 100
	const retryWorkerCount = 2

	// 创建通道
	retryChan := make(chan []FailedFeature, retryWorkerCount*2)
	var wg sync.WaitGroup
	var retrySuccess, retryError int64

	// 启动重试工作协程
	for i := 0; i < retryWorkerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for batch := range retryChan {
				for _, failed := range batch {
					// 构建使用转换SQL的新记录
					newRecord := make(map[string]interface{})

					// 复制属性数据（排除geom字段）
					for k, v := range failed.Record {
						if k != "geom" {
							newRecord[k] = v
						}
					}

					// 使用转换SQL处理几何
					newRecord["geom"] = buildGeometryConversionSQL(failed.WKBHex, targetGeoType)

					// 尝试插入
					if err := DB.Table(tableName).Create(newRecord).Error; err != nil {
						atomic.AddInt64(&retryError, 1)
						log.Printf("Worker %d - 转换后插入仍失败 [要素#%d]: %v",
							workerID, failed.Index, err)
					} else {
						atomic.AddInt64(&retrySuccess, 1)
					}
				}
			}
		}(i)
	}

	// 分批发送失败的要素
	go func() {
		defer close(retryChan)

		var batch []FailedFeature
		for _, failed := range failedFeatures {
			batch = append(batch, failed)

			if len(batch) >= retryBatchSize {
				retryChan <- batch
				batch = make([]FailedFeature, 0, retryBatchSize)
			}
		}

		// 发送剩余的
		if len(batch) > 0 {
			retryChan <- batch
		}
	}()

	wg.Wait()

	return retrySuccess, retryError
}

// ConvertGDBLayerToPGDirectWithMeta 优化后的图层转换函数
func ConvertGDBLayerToPGDirectWithMeta(layer Gogeo.GDBLayerInfo, DB *gorm.DB, tableName string, layerMeta *Gogeo.GDBLayerMetaData) []ProcessedFieldInfo {
	if len(layer.FeatureData) == 0 {
		log.Printf("图层 %s 没有要素数据", layer.LayerName)
		return nil
	}

	// 处理字段信息
	processedFields := processFieldInfosWithMeta(layer.FieldInfos, tableName, layerMeta)

	// 检查表是否存在
	var validFields map[string]string
	var targetGeoType string

	if tableExists(DB, tableName) {
		existingFields := getTableColumns2(DB, tableName)
		validFields = filterMatchingFieldsDirect(processedFields, existingFields)

		// 获取现有表的几何类型
		targetGeoType = getTableGeometryType(DB, tableName)
		log.Printf("表 %s 已存在，几何类型: %s，匹配到 %d 个字段",
			tableName, targetGeoType, len(validFields))
	} else {
		validFields = make(map[string]string)
		for _, field := range processedFields {
			validFields[field.ProcessedName] = field.DBType
		}

		// 使用图层的几何类型
		targetGeoType = normalizeGeoType(layer.GeoType)
		createGDBTableDirect(DB, tableName, validFields, targetGeoType)
		log.Printf("创建新表 %s，几何类型: %s，包含 %d 个字段",
			tableName, targetGeoType, len(validFields))
	}

	// 使用优化后的写入函数
	result := writeGDBDataToDBDirect(layer.FeatureData, DB, tableName, validFields, processedFields, targetGeoType)

	log.Printf("图层 %s 导入完成: 成功 %d/%d 条记录",
		layer.LayerName, result.SuccessCount, result.TotalCount)

	return processedFields
}

// getTableGeometryType 获取表的几何类型
func getTableGeometryType(DB *gorm.DB, tableName string) string {
	var result struct {
		Type string
	}

	// 从geometry_columns视图获取几何类型
	err := DB.Raw(`
		SELECT type 
		FROM geometry_columns 
		WHERE f_table_name = ? AND f_geometry_column = 'geom'
		LIMIT 1
	`, tableName).Scan(&result).Error

	if err != nil || result.Type == "" {
		// 如果查询失败，尝试从实际数据推断
		err = DB.Raw(`
			SELECT GeometryType(geom) as type 
			FROM ` + tableName + ` 
			WHERE geom IS NOT NULL 
			LIMIT 1
		`).Scan(&result).Error

		if err != nil {
			return "GEOMETRY" // 默认返回通用几何类型
		}
	}

	return result.Type
}

// normalizeGeoType 标准化几何类型名称
func normalizeGeoType(geoType string) string {
	geoType = strings.ToUpper(geoType)

	// 处理常见的变体
	switch {
	case strings.Contains(geoType, "MULTISURFACE"):
		return "MULTIPOLYGON"
	case strings.Contains(geoType, "SURFACE"):
		return "POLYGON"
	case strings.Contains(geoType, "MULTICURVE"):
		return "MULTILINESTRING"
	case strings.Contains(geoType, "CURVE"):
		return "LINESTRING"
	case strings.Contains(geoType, "MULTIPOLYGON"):
		return "MULTIPOLYGON"
	case strings.Contains(geoType, "POLYGON"):
		return "POLYGON"
	case strings.Contains(geoType, "MULTILINESTRING"):
		return "MULTILINESTRING"
	case strings.Contains(geoType, "LINESTRING"):
		return "LINESTRING"
	case strings.Contains(geoType, "MULTIPOINT"):
		return "MULTIPOINT"
	case strings.Contains(geoType, "POINT"):
		return "POINT"
	default:
		return geoType
	}
}
