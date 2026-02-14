package views

import (
	"encoding/json"
	"fmt"
	"github.com/GrainArc/Gogeo"
	"github.com/GrainArc/SouceMap/Transformer"
	"github.com/GrainArc/SouceMap/config"
	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/models"
	"github.com/GrainArc/SouceMap/pgmvt"
	"github.com/gin-gonic/gin"
	"github.com/paulmach/orb/geojson"
	"gorm.io/gorm"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// 单要素图斑获取
type getData struct {
	TableName string `json:"TableName"`
	ID        int32
}

type getDatas struct {
	TableName string  `json:"TableName"`
	ID        []int32 `json:"ID"`
}
type outData struct {
	GeoJson    []byte `gorm:"column:geojson"`
	Properties []byte `gorm:"column:properties"`
}

func GetGeo(jsonData getData) geojson.FeatureCollection {
	DB := models.DB
	sql := fmt.Sprintf(`
    SELECT 
        ST_AsGeoJSON(geom) AS geojson,
        to_jsonb(record) - 'geom' AS properties
    FROM %s AS record
    WHERE id = %d;
`, jsonData.TableName, jsonData.ID)

	var data outData
	DB.Raw(sql).Scan(&data)
	var feature struct {
		Geometry   map[string]interface{} `json:"geometry"`
		Properties map[string]interface{} `json:"properties"`
		Type       string                 `json:"type"`
	}
	feature.Type = "Feature"
	json.Unmarshal(data.GeoJson, &feature.Geometry)
	json.Unmarshal(data.Properties, &feature.Properties)
	var NewGeo geojson.FeatureCollection
	data2, _ := json.Marshal(feature)
	var myfeature *geojson.Feature
	aa := json.Unmarshal(data2, &myfeature)
	if aa != nil {
		fmt.Println(aa.Error())
	}
	NewGeo.Features = append(NewGeo.Features, myfeature)
	return NewGeo
}

func GetGeos(jsonData getDatas) geojson.FeatureCollection {
	DB := models.DB
	var NewGeo geojson.FeatureCollection

	for _, id := range jsonData.ID {
		sql := fmt.Sprintf(`
			SELECT 
				ST_AsGeoJSON(geom) AS geojson,
				to_jsonb(record) - 'geom' AS properties
			FROM %s AS record
			WHERE id = %d;
		`, jsonData.TableName, id)

		var data outData
		if err := DB.Raw(sql).Scan(&data).Error; err != nil {
			// 处理错误
			fmt.Println(err.Error())
		}

		var feature struct {
			Geometry   map[string]interface{} `json:"geometry"`
			Properties map[string]interface{} `json:"properties"`
			Type       string                 `json:"type"`
		}
		feature.Type = "Feature"

		json.Unmarshal(data.GeoJson, &feature.Geometry)
		json.Unmarshal(data.Properties, &feature.Properties)

		data2, _ := json.Marshal(feature)
		var myfeature *geojson.Feature
		aa := json.Unmarshal(data2, &myfeature)
		if aa != nil {
			fmt.Println(aa.Error())
		}
		NewGeo.Features = append(NewGeo.Features, myfeature)
	}

	return NewGeo
}

func (uc *UserController) GetGeoFromSchema(c *gin.Context) {
	var jsonData getData
	if err := c.ShouldBindJSON(&jsonData); err != nil { // 使用ShouldBindJSON代替BindJSON，避免自动返回400错误
		// 记录绑定失败的错误日志
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid JSON format", // 返回用户友好的错误信息
			"details": err.Error(),           // 提供详细错误信息用于调试
		})
		return // 提前返回，避免继续处理无效数据
	}
	NewGeo := GetGeo(jsonData)

	c.JSON(http.StatusOK, NewGeo)
}

// 图层要素添加
type geoData struct {
	TableName string                    `json:"TableName"`
	GeoJson   geojson.FeatureCollection `json:"geojson"`
	Username  string
	ID        int32
	BZ        string
}

func (uc *UserController) AddGeoToSchema(c *gin.Context) {
	var jsonData geoData
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	DB := models.DB
	// 获取最大ID
	sql := fmt.Sprintf(`SELECT COALESCE(MAX(id), 0) AS max_id FROM "%s";`, jsonData.TableName)
	var maxid int
	DB.Raw(sql).Scan(&maxid)
	newID := int32(maxid + 1)
	jsonData.GeoJson.Features[0].Properties["id"] = newID
	methods.SavaGeojsonToTable(DB, jsonData.GeoJson, jsonData.TableName)
	// 创建会话
	session := GetOrCreateSession(DB, jsonData.TableName, jsonData.Username)
	// 维护映射表：新增要素，源文件中无对应
	CreateDerivedMappings(DB, jsonData.TableName, []int32{newID}, 0, session.ID)
	// 记录操作
	NewGeojson, _ := json.Marshal(jsonData.GeoJson)
	outputIDs := MarshalIDs([]int32{newID})
	result := models.GeoRecord{
		TableName:  jsonData.TableName,
		GeoID:      newID,
		Username:   jsonData.Username,
		Type:       "要素添加",
		Date:       timeNowStr(),
		NewGeojson: NewGeojson,
		BZ:         jsonData.BZ,
		SessionID:  session.ID,
		SeqNo:      GetNextSeqNo(DB, session.ID),
		InputIDs:   MarshalIDs([]int32{}),
		OutputIDs:  outputIDs,
	}
	DB.Create(&result)
	// 删除MVT
	geom := jsonData.GeoJson.Features[0].Geometry
	pgmvt.DelMVT(DB, jsonData.TableName, geom)
	c.JSON(http.StatusOK, jsonData.GeoJson)
}

// 图层要素删除\

type delData struct {
	TableName string `json:"TableName"`
	ID        int32  `json:"ID"`
	Username  string
	BZ        string
}

// 提取并转换 ObjectID 的辅助函数
func extractObjectID(properties map[string]interface{}) (int64, error) {
	// 尝试不同的字段名（不区分大小写）
	possibleKeys := []string{"fid", "Fid", "FID", "objectid", "Objectid", "ObjectId", "OBJECTID"}

	var value interface{}
	var found bool

	for _, key := range possibleKeys {
		if val, ok := properties[key]; ok {
			value = val
			found = true
			break
		}
	}

	if !found {
		return 0, fmt.Errorf("objectid field not found")
	}

	// 转换为 int32
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case string:
		// 尝试将字符串转换为整数
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse objectid string '%s': %w", v, err)
		}
		return int64(parsed), nil
	default:
		return 0, fmt.Errorf("unsupported objectid type: %T", v)
	}
}

func (uc *UserController) DelGeoToSchema(c *gin.Context) {
	var jsonData delData
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	DB := models.DB
	getData := getData{ID: jsonData.ID, TableName: jsonData.TableName}
	geo := GetGeo(getData)
	OldGeojson, _ := json.Marshal(geo)
	sql := fmt.Sprintf(`DELETE FROM "%s" WHERE id = %d;`, jsonData.TableName, jsonData.ID)
	aa := DB.Exec(sql)
	if err := aa.Error; err != nil {
		log.Printf("Failed to delete record: %v", err)
	}
	// 创建会话
	session := GetOrCreateSession(DB, jsonData.TableName, jsonData.Username)
	// 维护映射表：标记为已删除
	MarkMappingDeleted(DB, jsonData.TableName, []int32{jsonData.ID})
	delObjJSON := DelIDGen(geo)
	inputIDs := MarshalIDs([]int32{jsonData.ID})
	result := &models.GeoRecord{
		TableName:    jsonData.TableName,
		GeoID:        jsonData.ID,
		Username:     jsonData.Username,
		Type:         "要素删除",
		Date:         timeNowStr(),
		DelObjectIDs: delObjJSON,
		OldGeojson:   OldGeojson,
		BZ:           jsonData.BZ,
		SessionID:    session.ID,
		SeqNo:        GetNextSeqNo(DB, session.ID),
		InputIDs:     inputIDs,
		OutputIDs:    MarshalIDs([]int32{}),
	}
	if err := DB.Create(&result).Error; err != nil {
		log.Printf("Failed to create geo record: %v", err)
	}
	geom := geo.Features[0].Geometry
	pgmvt.DelMVT(DB, jsonData.TableName, geom)
	c.JSON(http.StatusOK, "ok")
}

func DelIDGen(geom geojson.FeatureCollection) []byte {
	var delObjectIDs []int64
	for _, feature := range geom.Features {
		objID, _ := extractObjectID(feature.Properties)
		delObjectIDs = append(delObjectIDs, objID)
	}
	// 序列化 ObjectIDs
	delObjJSON, _ := json.Marshal(delObjectIDs)
	return delObjJSON
}

// 图层要素修改

// ChangeGeoToSchema 图层要素修改
func (uc *UserController) ChangeGeoToSchema(c *gin.Context) {
	var jsonData geoData
	c.BindJSON(&jsonData)
	DB := models.DB
	getData := getData{ID: jsonData.ID, TableName: jsonData.TableName}
	geo := GetGeo(getData)
	OldGeojson, _ := json.MarshalIndent(geo, "", "  ")
	methods.UpdateGeojsonToTable(DB, jsonData.GeoJson, jsonData.TableName, jsonData.ID)
	NewGeojson, _ := json.MarshalIndent(jsonData.GeoJson, "", "  ")
	delObjJSON := DelIDGen(geo)

	session := GetOrCreateSession(DB, jsonData.TableName, jsonData.Username)
	inputIDs := MarshalIDs([]int32{jsonData.ID})
	outputIDs := MarshalIDs([]int32{jsonData.ID})

	result := models.GeoRecord{
		TableName:    jsonData.TableName,
		GeoID:        jsonData.ID,
		Username:     jsonData.Username,
		Type:         "要素修改",
		Date:         timeNowStr(),
		OldGeojson:   OldGeojson,
		NewGeojson:   NewGeojson,
		DelObjectIDs: delObjJSON,
		BZ:           jsonData.BZ,
		SessionID:    session.ID,
		SeqNo:        GetNextSeqNo(DB, session.ID),
		InputIDs:     inputIDs,
		OutputIDs:    outputIDs,
	}
	err := DB.Create(&result).Error
	if err != nil {
		log.Printf("Failed to create geo record: %v", err)
	}
	geom := geo.Features[0].Geometry
	geom2 := jsonData.GeoJson.Features[0].Geometry
	pgmvt.DelMVT(DB, jsonData.TableName, geom)
	pgmvt.DelMVT(DB, jsonData.TableName, geom2)
	c.JSON(http.StatusOK, jsonData.GeoJson)
}

// 图层要素查询

type searchData struct {
	Rule          interface{} `json:"Rule"` // 改为 interface{} 以支持多种格式
	TableName     string      `json:"TableName"`
	Page          int         `json:"page"`
	PageSize      int         `json:"pagesize"`
	SortAttribute string      `json:"SortAttribute"` // 排序字段
	SortType      string      `json:"SortType"`      // 排序方式：DESC/ASC
}

type SearchCondition struct {
	Logic    string      `json:"logic"`    // AND 或 OR
	Field    string      `json:"field"`    // 字段名
	Operator string      `json:"operator"` // 操作符：=, !=, >, <, >=, <=, LIKE, NOT LIKE, IS NULL, IS NOT NULL, IN, NOT IN
	Value    interface{} `json:"value"`    // 值
}

type PaginatedResult struct {
	Data       []map[string]interface{} `json:"data"`
	Total      int64                    `json:"total"`
	Page       int                      `json:"page"`
	PageSize   int                      `json:"pageSize"`
	TotalPages int                      `json:"totalPages"`
}

func queryTable(db *gorm.DB, data searchData) (*PaginatedResult, error) {
	var results []map[string]interface{}
	var total int64

	// 创建基础查询
	baseQuery := db.Table(data.TableName)

	// 处理查询条件
	baseQuery = buildQueryConditions(baseQuery, data.Rule, data.TableName)

	// 获取总数 - 使用 Count 前需要先克隆查询
	countQuery := baseQuery.Session(&gorm.Session{})
	if err := countQuery.Count(&total).Error; err != nil {
		fmt.Println("获取总数出错:", err.Error())
		return nil, err
	}

	// 添加排序条件
	if data.SortAttribute != "" {
		sortType := strings.ToUpper(data.SortType)
		if sortType != "DESC" && sortType != "ASC" {
			sortType = "ASC"
		}
		quotedField := fmt.Sprintf(`"%s"`, data.SortAttribute)
		orderClause := fmt.Sprintf("%s %s", quotedField, sortType)
		baseQuery = baseQuery.Order(orderClause)
	}

	// 检查是否需要分页
	if data.Page > 0 && data.PageSize > 0 {
		offset := (data.Page - 1) * data.PageSize
		baseQuery = baseQuery.Offset(offset).Limit(data.PageSize)
	}

	// 执行查询
	if err := baseQuery.Find(&results).Error; err != nil {
		fmt.Println("查询数据出错:", err.Error())
		return nil, err
	}

	// 计算总页数
	totalPages := 0
	if data.Page > 0 && data.PageSize > 0 {
		totalPages = int((total + int64(data.PageSize) - 1) / int64(data.PageSize))
	}

	return &PaginatedResult{
		Data:       results,
		Total:      total,
		Page:       data.Page,
		PageSize:   data.PageSize,
		TotalPages: totalPages,
	}, nil
}

// buildQueryConditions 构建查询条件
func buildQueryConditions(query *gorm.DB, rule interface{}, tableName string) *gorm.DB {
	if rule == nil {
		return query
	}

	switch v := rule.(type) {
	case []interface{}:
		// 新格式：复杂条件数组
		return buildComplexConditions(query, v, tableName)
	case map[string]interface{}:
		// 旧格式：简单键值对
		return buildSimpleConditions(query, v, tableName)
	default:
		return query
	}
}

// buildComplexConditions 构建复杂查询条件（支持 AND/OR 和多种操作符）
func buildComplexConditions(query *gorm.DB, conditions []interface{}, tableName string) *gorm.DB {
	if len(conditions) == 0 {
		return query
	}

	for i, condItem := range conditions {
		condMap, ok := condItem.(map[string]interface{})
		if !ok {
			continue
		}

		field, _ := condMap["field"].(string)
		operator, _ := condMap["operator"].(string)
		value := condMap["value"]
		logic, _ := condMap["logic"].(string)

		if field == "" || operator == "" {
			continue
		}

		// 构建单个条件
		condition := buildSingleCondition(field, operator, value)

		// 第一个条件使用 Where，后续根据 logic 决定
		if i == 0 {
			query = query.Where(condition.sql, condition.args...)
		} else if strings.ToUpper(logic) == "OR" {
			query = query.Or(condition.sql, condition.args...)
		} else {
			// 默认使用 AND
			query = query.Where(condition.sql, condition.args...)
		}
	}

	return query
}

// ConditionResult 条件构建结果
type ConditionResult struct {
	sql  string
	args []interface{}
}

// buildSingleCondition 构建单个查询条件
func buildSingleCondition(field, operator string, value interface{}) ConditionResult {
	operator = strings.ToUpper(operator)
	// 用双引号包裹字段名,防止与SQL关键字冲突
	quotedField := fmt.Sprintf(`"%s"`, field)

	switch operator {
	case "=", "!=", ">", "<", ">=", "<=":
		// 基本比较操作符
		sql := fmt.Sprintf("CAST(%s AS TEXT) %s ?", quotedField, operator)
		return ConditionResult{sql: sql, args: []interface{}{value}}

	case "LIKE":
		// 模糊查询
		sql := fmt.Sprintf("CAST(%s AS TEXT) ILIKE ?", quotedField)
		return ConditionResult{sql: sql, args: []interface{}{value}}

	case "NOT LIKE":
		// 不包含
		sql := fmt.Sprintf("CAST(%s AS TEXT) NOT ILIKE ?", quotedField)
		return ConditionResult{sql: sql, args: []interface{}{value}}

	case "IS NULL":
		// 为空
		sql := fmt.Sprintf("%s IS NULL", quotedField)
		return ConditionResult{sql: sql, args: []interface{}{}}

	case "IS NOT NULL":
		// 不为空
		sql := fmt.Sprintf("%s IS NOT NULL", quotedField)
		return ConditionResult{sql: sql, args: []interface{}{}}

	case "IN":
		// 在范围内
		values, ok := value.([]interface{})
		if !ok {
			// 尝试转换字符串为数组
			if strVal, ok := value.(string); ok {
				values = []interface{}{strVal}
			} else {
				values = []interface{}{value}
			}
		}
		sql := fmt.Sprintf("CAST(%s AS TEXT) IN (?)", quotedField)
		return ConditionResult{sql: sql, args: []interface{}{values}}

	case "NOT IN":
		// 不在范围内
		values, ok := value.([]interface{})
		if !ok {
			if strVal, ok := value.(string); ok {
				values = []interface{}{strVal}
			} else {
				values = []interface{}{value}
			}
		}
		sql := fmt.Sprintf("CAST(%s AS TEXT) NOT IN (?)", quotedField)
		return ConditionResult{sql: sql, args: []interface{}{values}}

	default:
		// 默认使用等于
		sql := fmt.Sprintf("CAST(%s AS TEXT) = ?", quotedField)
		return ConditionResult{sql: sql, args: []interface{}{value}}
	}
}

// buildSimpleConditions 构建简单查询条件（兼容旧格式）
func buildSimpleConditions(query *gorm.DB, rule map[string]interface{}, tableName string) *gorm.DB {
	for key, value := range rule {
		searchValue := fmt.Sprintf("%%%v%%", value)
		// 用双引号包裹字段名
		quotedKey := fmt.Sprintf(`"%s"`, key)

		if key == "all_data_search" {
			// 获取表中所有字段
			atts := GetAtt(tableName, "")
			for _, field := range atts {
				quotedField := fmt.Sprintf(`"%s"`, field)
				condition := fmt.Sprintf("CAST(%s AS TEXT) ILIKE ?", quotedField)
				query = query.Or(condition, searchValue)
			}
		} else {
			condition := fmt.Sprintf("CAST(%s AS TEXT) ILIKE ?", quotedKey)
			query = query.Where(condition, searchValue)
		}
	}
	return query
}
func (uc *UserController) SearchGeoFromSchema(c *gin.Context) {
	var jsonData searchData
	c.BindJSON(&jsonData) //将前端geojson转换为geo对象
	DB := models.DB
	result, err := queryTable(DB, jsonData)
	if err != nil {
		fmt.Println(err.Error())
	}
	data := methods.MakeGeoJSON2(result.Data)
	type outdata struct {
		Data       interface{} `json:"data"`
		Total      int64       `json:"total"`
		Page       int         `json:"page"`
		PageSize   int         `json:"pageSize"`
		TotalPages int         `json:"totalPages"`
		TableName  string      `json:"TableName"`
		Code       int         `json:"code"`
	}
	response := outdata{
		Data:       data,              // 设置地理数据
		Page:       result.Page,       // 设置当前页码
		Total:      result.Total,      // 设置总记录数
		TotalPages: result.TotalPages, // 设置总页数
		PageSize:   result.PageSize,   // 设置每页大小
		TableName:  jsonData.TableName,
		Code:       200,
	}
	c.JSON(http.StatusOK, response)
}

func DownloadSearchGeo(SD searchData) string {
	DB := models.DB
	var Schema models.MySchema
	DB.Where("en = ?", SD.TableName).First(&Schema)
	taskid := Schema.EN
	homeDir, _ := os.UserHomeDir()
	OutFilePath := filepath.Join(homeDir, "BoundlessMap", "OutFile")

	result, _ := queryTable(DB, SD)

	outdir := filepath.Join(OutFilePath, taskid)
	os.MkdirAll(outdir, os.ModePerm)
	outshp := outdir + "/" + Schema.CN + ".shp"

	// 直接从查询结果转换为Shapefile
	Gogeo.ConvertPostGISToShapefileWithStructure(DB, result.Data, outshp, SD.TableName)

	methods.ZipFolder(outdir, SD.TableName)
	copyFile(OutFilePath+"/"+taskid+"/"+SD.TableName+".zip", config.MainConfig.Download)

	return "OutFile/" + taskid + "/" + SD.TableName + ".zip"
}

func (uc *UserController) DownloadSearchGeoFromSchema(c *gin.Context) {
	var jsonData searchData
	c.BindJSON(&jsonData) //将前端geojson转换为geo对象
	path := DownloadSearchGeo(jsonData)
	host := c.Request.Host
	url := &url.URL{
		Scheme: "http",
		Host:   host,
		Path:   "/geo/" + path,
	}
	c.String(http.StatusOK, url.String())
}

// 获取修改记录
type GeoRecordResponse struct {
	TableName string
	Username  string
	Type      string
	Date      string
	BZ        string
	ID        int64
	GeoID     int32
}

func (uc *UserController) GetChangeRecord(c *gin.Context) {
	username := c.Query("Username")
	DB := models.DB
	var aa []models.GeoRecord
	DB.Where("username = ?", username).Find(&aa)

	// 转换为响应结构体
	response := make([]GeoRecordResponse, 0) // Initialize as empty slice

	for _, record := range aa {
		response = append(response, GeoRecordResponse{
			TableName: record.TableName,
			Username:  record.Username,
			Type:      record.Type,
			Date:      record.Date,
			BZ:        record.BZ,
			ID:        record.ID,
			GeoID:     record.GeoID,
		})
	}
	c.JSON(http.StatusOK, response)
}

// DelChangeRecord 删除指定用户的所有地理记录
func (uc *UserController) DelChangeRecord(c *gin.Context) {
	// 从URL查询参数中获取用户名
	username := c.Query("Username")

	// 获取数据库连接实例
	DB := models.DB

	// 执行删除操作
	// 使用 Model 指定表，Where 指定条件，Delete 执行删除
	// result 包含操作结果信息
	result := DB.Where("username = ?", username).Delete(&models.GeoRecord{})

	// 检查数据库操作是否发生错误
	if result.Error != nil {
		// 数据库操作失败，返回500内部错误
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,                  // 状态码：服务器内部错误
			"message": "删除记录失败",       // 错误提示信息
			"error":   result.Error.Error(), // 具体错误信息（生产环境可考虑隐藏）
		})
		return // 提前返回
	}

	// 检查是否有记录被删除
	if result.RowsAffected == 0 {
		// 没有找到匹配的记录
		c.JSON(http.StatusOK, gin.H{
			"code":    200,                    // 状态码：请求成功
			"message": "没有找到该用户的记录", // 提示信息
			"count":   0,                      // 删除的记录数量
		})
		return // 提前返回
	}

	// 删除成功，返回成功响应
	c.JSON(http.StatusOK, gin.H{
		"code":    200,                 // 状态码：请求成功
		"message": "清空记录成功",      // 成功提示信息
		"count":   result.RowsAffected, // 返回实际删除的记录数量
	})
}

// 图层要素分割
type SplitData struct {
	Line      geojson.FeatureCollection `json:"Line"`
	LayerName string                    `json:"LayerName"`
	Username  string                    `json:"Username"`
	ID        int32                     `json:"ID"`
}

func (uc *UserController) SplitFeature(c *gin.Context) {
	var jsonData SplitData
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": err.Error(), "data": ""})
		return
	}
	DB := models.DB
	line := Transformer.GetGeometryString(jsonData.Line.Features[0])
	LayerName := jsonData.LayerName
	var schema models.MySchema
	result := DB.Where("en = ?", LayerName).First(&schema)
	if result.Error != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": result.Error.Error(), "data": ""})
		return
	}
	if schema.Type != "polygon" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": "只有面数据才能分割", "data": ""})
		return
	}
	fileExt := GetFileExt(LayerName)
	var mappingField string
	switch fileExt {
	case ".shp":
		mappingField = "objectid"
	case ".gdb":
		mappingField = "fid"
	default:
		mappingField = ""
	}
	tx := DB.Begin()
	if tx.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "开启事务失败: " + tx.Error.Error(), "data": ""})
		return
	}
	lockSQL := fmt.Sprintf(`SELECT pg_advisory_xact_lock(hashtext('%s_id_lock'))`, LayerName)
	if err := tx.Exec(lockSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "获取锁失败: " + err.Error(), "data": ""})
		return
	}
	var originalFeature map[string]interface{}
	getOriginalSQL := fmt.Sprintf(`SELECT * FROM "%s" WHERE id = %d`, LayerName, jsonData.ID)
	if err := tx.Raw(getOriginalSQL).Scan(&originalFeature).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "获取原要素失败: " + err.Error(), "data": ""})
		return
	}
	if len(originalFeature) == 0 {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{"code": 404, "message": "未找到指定ID的几何数据", "data": ""})
		return
	}
	var additionalColumns []string
	for key := range originalFeature {
		if key != "id" && key != "geom" {
			additionalColumns = append(additionalColumns, key)
		}
	}
	var maxID int32
	getMaxIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX(id), 0) as max_id FROM "%s"`, LayerName)
	if err := tx.Raw(getMaxIDSQL).Scan(&maxID).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "查询最大ID失败: " + err.Error(), "data": ""})
		return
	}
	var maxMappingID int32
	if mappingField != "" {
		getMaxMappingIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX("%s"), 0) as max_id FROM "%s"`, mappingField, LayerName)
		if err := tx.Raw(getMaxMappingIDSQL).Scan(&maxMappingID).Error; err != nil {
			maxMappingID = 0
		}
	}
	splitSelectCols := `(ST_Dump(ST_Split(o.geom, ST_GeomFromGeoJSON('%s')))).geom AS geom`
	for _, col := range additionalColumns {
		splitSelectCols += fmt.Sprintf(`, o."%s"`, col)
	}
	insertCols := "id, geom"
	for _, col := range additionalColumns {
		insertCols += fmt.Sprintf(`, "%s"`, col)
	}
	selectCols := fmt.Sprintf(`%d + ROW_NUMBER() OVER () AS id, geom`, maxID)
	for _, col := range additionalColumns {
		if mappingField != "" && col == mappingField {
			selectCols += fmt.Sprintf(`, %d + ROW_NUMBER() OVER () AS "%s"`, maxMappingID, col)
		} else {
			selectCols += fmt.Sprintf(`, "%s"`, col)
		}
	}
	splitAndInsertSQL := fmt.Sprintf(`
		WITH original AS (
			SELECT * FROM "%s" WHERE id = %d
		),
		split_geom AS (
			SELECT `+splitSelectCols+`
			FROM original o)
		INSERT INTO "%s" (`+insertCols+`)
		SELECT `+selectCols+`
		FROM split_geom
		RETURNING id
	`, LayerName, jsonData.ID, line, LayerName)
	type InsertedID struct {
		ID int32
	}
	var insertedIDs []InsertedID
	if err := tx.Raw(splitAndInsertSQL).Scan(&insertedIDs).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "切割并插入新要素失败: " + err.Error(), "data": ""})
		return
	}
	if len(insertedIDs) == 0 {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "切割失败，未生成新要素", "data": ""})
		return
	}
	// 获取原要素GeoJSON（事务提交前，原要素还在）
	GetPdata := getData{TableName: LayerName, ID: jsonData.ID}
	geom := GetGeo(GetPdata)
	pgmvt.DelMVT(DB, jsonData.LayerName, geom.Features[0].Geometry)
	// 删除原要素
	deleteSQL := fmt.Sprintf(`DELETE FROM "%s" WHERE id = %d`, LayerName, jsonData.ID)
	if err := tx.Exec(deleteSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "删除原要素失败: " + err.Error(), "data": ""})
		return
	}
	// 收集新ID列表
	var newIDList []int32
	for _, id := range insertedIDs {
		newIDList = append(newIDList, id.ID)
	}
	getdata2 := getDatas{TableName: LayerName, ID: newIDList}
	// 提交事务
	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "提交事务失败: " + err.Error(), "data": ""})
		return
	}
	splitGeojson := GetGeos(getdata2)
	// ========== 维护映射表 ==========
	session := GetOrCreateSession(DB, LayerName, "") // Username可从jsonData中取，SplitData需加Username字段
	MarkMappingDeleted(DB, LayerName, []int32{jsonData.ID})
	CreateDerivedMappings(DB, LayerName, newIDList, jsonData.ID, session.ID)
	// ========== 记录操作（带InputIDs/OutputIDs） ==========
	delObjJSON := DelIDGen(geom)
	OldGeojson, _ := json.Marshal(geom)
	NewGeojson, _ := json.Marshal(splitGeojson)
	inputIDs := MarshalIDs([]int32{jsonData.ID})
	outputIDs := MarshalIDs(newIDList)
	RecordResult := models.GeoRecord{
		TableName:    jsonData.LayerName,
		GeoID:        jsonData.ID,
		Type:         "要素分割",
		Date:         timeNowStr(),
		OldGeojson:   OldGeojson,
		NewGeojson:   NewGeojson,
		DelObjectIDs: delObjJSON,
		SessionID:    session.ID,
		SeqNo:        GetNextSeqNo(DB, session.ID),
		InputIDs:     inputIDs,
		OutputIDs:    outputIDs,
	}
	DB.Create(&RecordResult)
	c.JSON(http.StatusOK, gin.H{"code": 200, "message": "切割成功，已生成多个新要素", "data": splitGeojson})
}

// 图层要素合并
type DissolveData struct {
	LayerName string  `json:"LayerName"`
	IDs       []int32 `json:"ids"`
	MainID    int32   `json:"mainId"`
}

func (uc *UserController) DissolveFeature(c *gin.Context) {
	var jsonData DissolveData
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": err.Error(), "data": ""})
		return
	}
	if len(jsonData.IDs) < 2 {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "至少需要选择2个要素进行合并", "data": ""})
		return
	}
	mainIDExists := false
	for _, id := range jsonData.IDs {
		if id == jsonData.MainID {
			mainIDExists = true
			break
		}
	}
	if !mainIDExists {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "MainID必须在选中的要素列表中", "data": ""})
		return
	}
	DB := models.DB
	LayerName := jsonData.LayerName
	var schema models.MySchema
	result := DB.Where("en = ?", LayerName).First(&schema)
	if result.Error != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": result.Error.Error(), "data": ""})
		return
	}
	if schema.Type != "polygon" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": "只有面数据才能合并", "data": ""})
		return
	}
	fileExt := GetFileExt(LayerName)
	var mappingField string
	switch fileExt {
	case ".shp":
		mappingField = "objectid"
	case ".gdb":
		mappingField = "fid"
	default:
		mappingField = ""
	}
	tx := DB.Begin()
	if tx.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "开启事务失败: " + tx.Error.Error(), "data": ""})
		return
	}
	idList := make([]string, len(jsonData.IDs))
	for i, id := range jsonData.IDs {
		idList[i] = fmt.Sprintf("%d", id)
	}
	idsStr := strings.Join(idList, ",")
	var count int64
	checkSQL := fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE id IN (%s)`, LayerName, idsStr)
	if err := tx.Raw(checkSQL).Count(&count).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "验证要素失败: " + err.Error(), "data": ""})
		return
	}
	if int(count) != len(jsonData.IDs) {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{"code": 404, "message": fmt.Sprintf("部分ID不存在，期望%d个，实际找到%d个", len(jsonData.IDs), count), "data": ""})
		return
	}
	var originalFeature map[string]interface{}
	getOriginalSQL := fmt.Sprintf(`SELECT * FROM "%s" WHERE id = %d`, LayerName, jsonData.MainID)
	if err := tx.Raw(getOriginalSQL).Scan(&originalFeature).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "获取主要素属性失败: " + err.Error(), "data": ""})
		return
	}
	if len(originalFeature) == 0 {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{"code": 404, "message": fmt.Sprintf("MainID(%d)对应的要素不存在", jsonData.MainID), "data": ""})
		return
	}
	var maxID int32
	getMaxIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX(id), 0) FROM "%s"`, LayerName)
	if err := tx.Raw(getMaxIDSQL).Scan(&maxID).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "获取最大ID失败: " + err.Error(), "data": ""})
		return
	}
	var maxMappingID int32
	if mappingField != "" {
		getMaxMappingIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX("%s"), 0) FROM "%s"`, mappingField, LayerName)
		if err := tx.Raw(getMaxMappingIDSQL).Scan(&maxMappingID).Error; err != nil {
			maxMappingID = 0
		}
	}
	// 构建属性字段列表
	var columnNames []string
	var columnValues []string
	for key, value := range originalFeature {
		if key != "id" && key != "geom" {
			columnNames = append(columnNames, fmt.Sprintf(`"%s"`, key))
			if mappingField != "" && key == mappingField {
				columnValues = append(columnValues, fmt.Sprintf("%d", maxMappingID+1))
				continue
			}
			switch v := value.(type) {
			case nil:
				columnValues = append(columnValues, "NULL")
			case string:
				escapedValue := strings.ReplaceAll(v, "'", "''")
				columnValues = append(columnValues, fmt.Sprintf("'%s'", escapedValue))
			case time.Time:
				if v.IsZero() {
					columnValues = append(columnValues, "NULL")
				} else {
					columnValues = append(columnValues, fmt.Sprintf("'%s'", v.Format("2006-01-02")))
				}
			case int, int32, int64, float32, float64:
				columnValues = append(columnValues, fmt.Sprintf("%v", v))
			case bool:
				columnValues = append(columnValues, fmt.Sprintf("%t", v))
			default:
				if t, ok := value.(*time.Time); ok {
					if t == nil || t.IsZero() {
						columnValues = append(columnValues, "NULL")
					} else {
						columnValues = append(columnValues, fmt.Sprintf("'%s'", t.Format("2006-01-02")))
					}
				} else {
					columnValues = append(columnValues, fmt.Sprintf("'%v'", v))
				}
			}
		}
	}
	columnsStr := ""
	valuesStr := ""
	if len(columnNames) > 0 {
		columnsStr = ", " + strings.Join(columnNames, ", ")
		valuesStr = ", " + strings.Join(columnValues, ", ")
	}
	newID := maxID + 1
	dissolveAndInsertSQL := fmt.Sprintf(`
		WITH dissolved AS (
			SELECT 
				ST_Multi(ST_Union(ST_SnapToGrid(geom, 0.0000001))) AS merged_geom
			FROM "%s"
			WHERE id IN (%s)
		)
		INSERT INTO "%s" (id, geom%s)
		SELECT 
			%d AS id,
			merged_geom
			%s
		FROM dissolved
		RETURNING ST_AsGeoJSON(geom) AS geojson, id
	`, LayerName, idsStr, LayerName, columnsStr, newID, valuesStr)
	type DissolveResult struct {
		Geojson string
		ID      int32
	}
	var dissolveResult DissolveResult
	if err := tx.Raw(dissolveAndInsertSQL).Scan(&dissolveResult).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "合并并插入新要素失败: " + err.Error(), "data": ""})
		return
	}
	if dissolveResult.Geojson == "" {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "合并失败，未生成有效几何", "data": ""})
		return
	}
	// 获取原要素GeoJSON（事务提交前）
	getdata2 := getDatas{TableName: LayerName, ID: jsonData.IDs}
	oldGeo := GetGeos(getdata2)
	oldGeojson, _ := json.Marshal(oldGeo)
	for _, feature := range oldGeo.Features {
		pgmvt.DelMVT(DB, jsonData.LayerName, feature.Geometry)
	}
	delObjJSON := DelIDGen(oldGeo)
	// 删除原要素
	deleteSQL := fmt.Sprintf(`DELETE FROM "%s" WHERE id IN (%s)`, LayerName, idsStr)
	if err := tx.Exec(deleteSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "删除原要素失败: " + err.Error(), "data": ""})
		return
	}
	// 提交事务
	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "提交事务失败: " + err.Error(), "data": ""})
		return
	}
	GetPdata := getData{TableName: LayerName, ID: dissolveResult.ID}
	newGeo := GetGeo(GetPdata)
	newGeoJson, _ := json.Marshal(newGeo)
	// ========== 维护映射表 ==========
	session := GetOrCreateSession(DB, LayerName, "")
	MarkMappingDeleted(DB, LayerName, jsonData.IDs)
	CreateDerivedMappingsMultiParent(DB, LayerName, []int32{dissolveResult.ID}, jsonData.IDs, session.ID)
	// ========== 记录操作 ==========
	inputIDs := MarshalIDs(jsonData.IDs)
	outputIDs := MarshalIDs([]int32{dissolveResult.ID})
	RecordResult := models.GeoRecord{
		TableName:    jsonData.LayerName,
		Type:         "要素合并",
		Date:         timeNowStr(),
		OldGeojson:   oldGeojson,
		NewGeojson:   newGeoJson,
		DelObjectIDs: delObjJSON,
		SessionID:    session.ID,
		SeqNo:        GetNextSeqNo(DB, session.ID),
		InputIDs:     inputIDs,
		OutputIDs:    outputIDs,
	}
	DB.Create(&RecordResult)
	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": fmt.Sprintf("成功合并%d个要素为1个新要素，使用ID(%d)的属性", len(jsonData.IDs), jsonData.MainID),
		"data": gin.H{
			"geojson": dissolveResult.Geojson,
			"new_id":  dissolveResult.ID,
			"main_id": jsonData.MainID,
		},
	})
}

// 要素聚合体打散

type ExplodeData struct {
	LayerName string  `json:"LayerName"`
	IDs       []int32 `json:"ids"`
}

func (uc *UserController) ExplodeFeature(c *gin.Context) {
	var jsonData ExplodeData
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": err.Error(), "data": ""})
		return
	}
	if len(jsonData.IDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "IDs 不能为空", "data": ""})
		return
	}
	DB := models.DB
	LayerName := jsonData.LayerName
	var schema models.MySchema
	result := DB.Where("en = ?", LayerName).First(&schema)
	if result.Error != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": result.Error.Error(), "data": ""})
		return
	}
	fileExt := GetFileExt(LayerName)
	var mappingField string
	switch fileExt {
	case ".shp":
		mappingField = "objectid"
	case ".gdb":
		mappingField = "fid"
	default:
		mappingField = ""
	}
	tx := DB.Begin()
	if tx.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "开启事务失败: " + tx.Error.Error(), "data": ""})
		return
	}
	lockSQL := fmt.Sprintf(`SELECT pg_advisory_xact_lock(hashtext('%s_id_lock'))`, LayerName)
	if err := tx.Exec(lockSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "获取锁失败: " + err.Error(), "data": ""})
		return
	}
	idPlaceholders := make([]string, len(jsonData.IDs))
	idInterfaces := make([]interface{}, len(jsonData.IDs))
	for i, id := range jsonData.IDs {
		idPlaceholders[i] = "?"
		idInterfaces[i] = id
	}
	idCondition := fmt.Sprintf("id IN (%s)", strings.Join(idPlaceholders, ","))
	// 检查几何类型
	type GeomInfo struct {
		ID       int32
		GeomType string
	}
	var geomInfos []GeomInfo
	checkTypeSQL := fmt.Sprintf(`SELECT id, ST_GeometryType(geom) FROM "%s" WHERE %s`, LayerName, idCondition)
	if err := tx.Raw(checkTypeSQL, idInterfaces...).Scan(&geomInfos).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "获取几何类型失败: " + err.Error(), "data": ""})
		return
	}
	if len(geomInfos) != len(jsonData.IDs) {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{"code": 404, "message": fmt.Sprintf("部分ID未找到，期望 %d 个，实际找到 %d 个", len(jsonData.IDs), len(geomInfos)), "data": ""})
		return
	}
	// 检查每个聚合体包含多少个几何体
	type GeomCount struct {
		ID    int32
		Count int
	}
	var geomCounts []GeomCount
	countGeomsSQL := fmt.Sprintf(`SELECT id, ST_NumGeometries(geom) as count FROM "%s" WHERE %s`, LayerName, idCondition)
	if err := tx.Raw(countGeomsSQL, idInterfaces...).Scan(&geomCounts).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "获取几何数量失败: " + err.Error(), "data": ""})
		return
	}
	singleGeoms := []int32{}
	for _, count := range geomCounts {
		if count.Count <= 1 {
			singleGeoms = append(singleGeoms, count.ID)
		}
	}
	if len(singleGeoms) > 0 {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": fmt.Sprintf("以下ID的聚合体只包含一个几何体，无需打散: %v", singleGeoms), "data": ""})
		return
	}
	// 获取原要素属性
	var originalFeatures []map[string]interface{}
	getOriginalSQL := fmt.Sprintf(`SELECT * FROM "%s" WHERE %s`, LayerName, idCondition)
	if err := tx.Raw(getOriginalSQL, idInterfaces...).Scan(&originalFeatures).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "获取原要素失败: " + err.Error(), "data": ""})
		return
	}
	var additionalColumns []string
	if len(originalFeatures) > 0 {
		for key := range originalFeatures[0] {
			if key != "id" && key != "geom" {
				additionalColumns = append(additionalColumns, key)
			}
		}
	}
	var maxID int32
	getMaxIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX(id), 0) as max_id FROM "%s"`, LayerName)
	if err := tx.Raw(getMaxIDSQL).Scan(&maxID).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "查询最大ID失败: " + err.Error(), "data": ""})
		return
	}
	var maxMappingID int32
	if mappingField != "" {
		getMaxMappingIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX("%s"), 0) as max_id FROM "%s"`, mappingField, LayerName)
		if err := tx.Raw(getMaxMappingIDSQL).Scan(&maxMappingID).Error; err != nil {
			maxMappingID = 0
		}
	}
	// 构建打散SQL
	explodeSelectCols := `(ST_Dump(o.geom)).geom AS geom`
	for _, col := range additionalColumns {
		explodeSelectCols += fmt.Sprintf(`, o."%s"`, col)
	}
	insertCols := "id, geom"
	for _, col := range additionalColumns {
		insertCols += fmt.Sprintf(`, "%s"`, col)
	}
	selectCols := fmt.Sprintf(`%d + ROW_NUMBER() OVER () AS id, geom`, maxID)
	for _, col := range additionalColumns {
		if mappingField != "" && col == mappingField {
			selectCols += fmt.Sprintf(`, %d + ROW_NUMBER() OVER () AS "%s"`, maxMappingID, col)
		} else {
			selectCols += fmt.Sprintf(`, "%s"`, col)
		}
	}
	explodeAndInsertSQL := fmt.Sprintf(`
		WITH original AS (
			SELECT * FROM "%s" WHERE %s
		),
		exploded_geom AS (
			SELECT %s
			FROM original o)
		INSERT INTO "%s" (%s)
		SELECT %s
		FROM exploded_geom
		RETURNING id
	`, LayerName, idCondition, explodeSelectCols, LayerName, insertCols, selectCols)
	type InsertedID struct {
		ID int32
	}
	var insertedIDs []InsertedID
	if err := tx.Raw(explodeAndInsertSQL, idInterfaces...).Scan(&insertedIDs).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "打散并插入新要素失败: " + err.Error(), "data": ""})
		return
	}
	if len(insertedIDs) == 0 {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "打散失败，未生成新要素", "data": ""})
		return
	}
	// 删除原要素
	deleteSQL := fmt.Sprintf(`DELETE FROM "%s" WHERE %s`, LayerName, idCondition)
	if err := tx.Exec(deleteSQL, idInterfaces...).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "删除原要素失败: " + err.Error(), "data": ""})
		return
	}
	var newIDList []int32
	for _, id := range insertedIDs {
		newIDList = append(newIDList, id.ID)
	}
	getdata2 := getDatas{TableName: LayerName, ID: newIDList}
	// 提交事务
	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "提交事务失败: " + err.Error(), "data": ""})
		return
	}
	explodedGeojson := GetGeos(getdata2)
	// ========== 维护映射表 ==========
	session := GetOrCreateSession(DB, LayerName, "")
	MarkMappingDeleted(DB, LayerName, jsonData.IDs)
	// 为每个新要素创建映射，关联到对应的原父要素
	// 由于打散可能涉及多个原要素，需要建立正确的父子关系
	// 这里简化处理：所有新要素都关联到第一个父要素，完整关系通过InputIDs追溯
	for _, nid := range newIDList {
		mapping := models.OriginMapping{
			TableName:       LayerName,
			PostGISID:       nid,
			SourceObjectID:  -1,
			Origin:          "derived",
			ParentPostGISID: jsonData.IDs[0], // 主父要素
			SessionID:       session.ID,
			IsDeleted:       false,
		}
		DB.Create(&mapping)
	}
	// ========== 记录操作 ==========
	// 构建原要素GeoJSON（事务已提交，原要素已删除，从缓存的originalFeatures构建）

	// 使用事务提交前已获取的数据
	// 这里改为在事务提交前获取
	// 实际上需要在删除前获取，下面用一个更安全的方式
	// 重新从explodedGeojson和原始数据构建记录
	OldGeojson, _ := json.Marshal(buildFeatureCollectionFromMaps(originalFeatures, LayerName))
	NewGeojson, _ := json.Marshal(explodedGeojson)
	delObjJSON := DelIDGen(*buildFeatureCollectionFromMaps(originalFeatures, LayerName))
	inputIDs := MarshalIDs(jsonData.IDs)
	outputIDs := MarshalIDs(newIDList)
	RecordResult := models.GeoRecord{
		TableName:    jsonData.LayerName,
		GeoID:        jsonData.IDs[0],
		Type:         "要素打散",
		Date:         timeNowStr(),
		OldGeojson:   OldGeojson,
		NewGeojson:   NewGeojson,
		DelObjectIDs: delObjJSON,
		SessionID:    session.ID,
		SeqNo:        GetNextSeqNo(DB, session.ID),
		InputIDs:     inputIDs,
		OutputIDs:    outputIDs,
	}
	DB.Create(&RecordResult)
	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": fmt.Sprintf("打散成功，已将 %d 个聚合体拆分为 %d 个独立要素", len(jsonData.IDs), len(insertedIDs)),
		"data":    explodedGeojson,
	})
}

// 环岛构造
type DonutData struct {
	LayerName string                    `json:"LayerName"`
	ID        int32                     `json:"ID"`
	Donut     geojson.FeatureCollection `json:"Donut"`
}

// 要素环岛构造
func (uc *UserController) DonutBuilder(c *gin.Context) {
	var jsonData DonutData
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": err.Error(), "data": ""})
		return
	}
	DB := models.DB
	donut := Transformer.GetGeometryString(jsonData.Donut.Features[0])
	LayerName := jsonData.LayerName
	var schema models.MySchema
	result := DB.Where("en = ?", LayerName).First(&schema)
	if result.Error != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": result.Error.Error(), "data": ""})
		return
	}
	if schema.Type != "polygon" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": "只有面数据才能构造环岛", "data": ""})
		return
	}
	fileExt := GetFileExt(LayerName)
	var mappingField string
	switch fileExt {
	case ".shp":
		mappingField = "objectid"
	case ".gdb":
		mappingField = "fid"
	default:
		mappingField = ""
	}
	tx := DB.Begin()
	if tx.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "开启事务失败: " + tx.Error.Error(), "data": ""})
		return
	}
	lockSQL := fmt.Sprintf(`SELECT pg_advisory_xact_lock(hashtext('%s_donut_lock'))`, LayerName)
	if err := tx.Exec(lockSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "获取锁失败: " + err.Error(), "data": ""})
		return
	}
	var originalFeature map[string]interface{}
	getOriginalSQL := fmt.Sprintf(`SELECT * FROM "%s" WHERE id = %d`, LayerName, jsonData.ID)
	if err := tx.Raw(getOriginalSQL).Scan(&originalFeature).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "获取原要素失败: " + err.Error(), "data": ""})
		return
	}
	if len(originalFeature) == 0 {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{"code": 404, "message": "未找到指定ID的几何数据", "data": ""})
		return
	}
	var additionalColumns []string
	for key := range originalFeature {
		if key != "id" && key != "geom" {
			additionalColumns = append(additionalColumns, key)
		}
	}
	var maxID int32
	getMaxIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX(id), 0) as max_id FROM "%s"`, LayerName)
	if err := tx.Raw(getMaxIDSQL).Scan(&maxID).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "查询最大ID失败: " + err.Error(), "data": ""})
		return
	}
	var maxMappingID int32
	if mappingField != "" {
		getMaxMappingIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX("%s"), 0) as max_id FROM "%s"`, mappingField, LayerName)
		if err := tx.Raw(getMaxMappingIDSQL).Scan(&maxMappingID).Error; err != nil {
			maxMappingID = 0
		}
	}
	// 验证环岛位置
	validateDonutSQL := fmt.Sprintf(`
		SELECT ST_Contains(
			(SELECT geom FROM "%s" WHERE id = %d),
			ST_GeomFromGeoJSON('%s')
		)
	`, LayerName, jsonData.ID, donut)
	var isContained bool
	if err := tx.Raw(validateDonutSQL).Scan(&isContained).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "验证环岛位置失败: " + err.Error(), "data": ""})
		return
	}
	if !isContained {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "环岛多边形必须完全位于原多边形内部", "data": ""})
		return
	}
	insertCols := "id, geom"
	for _, col := range additionalColumns {
		insertCols += fmt.Sprintf(`, "%s"`, col)
	}
	selectCols := fmt.Sprintf(`%d + 1 AS id, geom`, maxID)
	for _, col := range additionalColumns {
		if col == mappingField && mappingField != "" {
			selectCols += fmt.Sprintf(`, %d + 1 AS "%s"`, maxMappingID, col)
		} else {
			selectCols += fmt.Sprintf(`, "%s"`, col)
		}
	}
	additionalColsRef := ""
	for _, col := range additionalColumns {
		additionalColsRef += fmt.Sprintf(`, o."%s"`, col)
	}
	donutConstructSQL := fmt.Sprintf(`
		WITH original AS (
			SELECT * FROM "%s" WHERE id = %d
		),
		donut_geom AS (
			SELECT 
				ST_Difference(o.geom, ST_GeomFromGeoJSON('%s')) AS geom
				%s
			FROM original o
		)
		INSERT INTO "%s" (%s)
		SELECT %s
		FROM donut_geom
		RETURNING id
	`, LayerName, jsonData.ID, donut, additionalColsRef, LayerName, insertCols, selectCols)
	type InsertedID struct {
		ID int32
	}
	var insertedIDs []InsertedID
	if err := tx.Raw(donutConstructSQL).Scan(&insertedIDs).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "构造环岛失败: " + err.Error(), "data": ""})
		return
	}
	if len(insertedIDs) == 0 {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "环岛构造失败，未生成新要素", "data": ""})
		return
	}
	// 获取原要素GeoJSON
	GetPdata := getData{TableName: LayerName, ID: jsonData.ID}
	geom := GetGeo(GetPdata)
	pgmvt.DelMVT(DB, jsonData.LayerName, geom.Features[0].Geometry)
	// 删除原要素
	deleteSQL := fmt.Sprintf(`DELETE FROM "%s" WHERE id = %d`, LayerName, jsonData.ID)
	if err := tx.Exec(deleteSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "删除原要素失败: " + err.Error(), "data": ""})
		return
	}
	var newIDList []int32
	for _, id := range insertedIDs {
		newIDList = append(newIDList, id.ID)
	}
	getdata2 := getDatas{TableName: LayerName, ID: newIDList}
	// 提交事务
	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "提交事务失败: " + err.Error(), "data": ""})
		return
	}
	donutGeojson := GetGeos(getdata2)
	// ========== 维护映射表 ==========
	session := GetOrCreateSession(DB, LayerName, "")
	MarkMappingDeleted(DB, LayerName, []int32{jsonData.ID})
	CreateDerivedMappings(DB, LayerName, newIDList, jsonData.ID, session.ID)
	// ========== 记录操作 ==========
	delObjJSON := DelIDGen(geom)
	OldGeojson, _ := json.Marshal(geom)
	NewGeojson, _ := json.Marshal(donutGeojson)
	inputIDs := MarshalIDs([]int32{jsonData.ID})
	outputIDs := MarshalIDs(newIDList)
	RecordResult := models.GeoRecord{
		TableName:    jsonData.LayerName,
		GeoID:        jsonData.ID,
		Type:         "要素环岛构造",
		Date:         timeNowStr(),
		OldGeojson:   OldGeojson,
		NewGeojson:   NewGeojson,
		DelObjectIDs: delObjJSON,
		SessionID:    session.ID,
		SeqNo:        GetNextSeqNo(DB, session.ID),
		InputIDs:     inputIDs,
		OutputIDs:    outputIDs,
	}
	DB.Create(&RecordResult)
	c.JSON(http.StatusOK, gin.H{"code": 200, "message": "环岛构造成功", "data": donutGeojson})
}

// 要素聚合
type AggregatorData struct {
	LayerName string  `json:"LayerName"`
	IDs       []int32 `json:"ids"`
	MainID    int32   `json:"mainId"`
}

func (uc *UserController) AggregatorFeature(c *gin.Context) {
	var jsonData AggregatorData
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": err.Error(), "data": ""})
		return
	}
	if len(jsonData.IDs) < 2 {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "至少需要选择2个要素进行聚合", "data": ""})
		return
	}
	mainIDExists := false
	for _, id := range jsonData.IDs {
		if id == jsonData.MainID {
			mainIDExists = true
			break
		}
	}
	if !mainIDExists {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "MainID必须在选中的要素列表中", "data": ""})
		return
	}
	DB := models.DB
	LayerName := jsonData.LayerName
	var schema models.MySchema
	result := DB.Where("en = ?", LayerName).First(&schema)
	if result.Error != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": result.Error.Error(), "data": ""})
		return
	}
	fileExt := GetFileExt(LayerName)
	var mappingField string
	switch fileExt {
	case ".shp":
		mappingField = "objectid"
	case ".gdb":
		mappingField = "fid"
	default:
		mappingField = ""
	}
	tx := DB.Begin()
	if tx.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "开启事务失败: " + tx.Error.Error(), "data": ""})
		return
	}
	idList := make([]string, len(jsonData.IDs))
	for i, id := range jsonData.IDs {
		idList[i] = fmt.Sprintf("%d", id)
	}
	idsStr := strings.Join(idList, ",")
	// 验证所有ID是否存在
	var count int64
	checkSQL := fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE id IN (%s)`, LayerName, idsStr)
	if err := tx.Raw(checkSQL).Count(&count).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "验证要素失败: " + err.Error(), "data": ""})
		return
	}
	if int(count) != len(jsonData.IDs) {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{"code": 404, "message": fmt.Sprintf("部分ID不存在，期望%d个，实际找到%d个", len(jsonData.IDs), count), "data": ""})
		return
	}
	// 获取MainID对应要素的所有属性
	var originalFeature map[string]interface{}
	getOriginalSQL := fmt.Sprintf(`SELECT * FROM "%s" WHERE id = %d`, LayerName, jsonData.MainID)
	if err := tx.Raw(getOriginalSQL).Scan(&originalFeature).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "获取主要素属性失败: " + err.Error(), "data": ""})
		return
	}
	if len(originalFeature) == 0 {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{"code": 404, "message": fmt.Sprintf("MainID(%d)对应的要素不存在", jsonData.MainID), "data": ""})
		return
	}
	var maxID int32
	getMaxIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX(id), 0) FROM "%s"`, LayerName)
	if err := tx.Raw(getMaxIDSQL).Scan(&maxID).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "获取最大ID失败: " + err.Error(), "data": ""})
		return
	}
	var maxMappingID int32
	if mappingField != "" {
		getMaxMappingIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX("%s"), 0) FROM "%s"`, mappingField, LayerName)
		if err := tx.Raw(getMaxMappingIDSQL).Scan(&maxMappingID).Error; err != nil {
			maxMappingID = 0
		}
	}
	// 构建属性字段列表
	var columnNames []string
	var columnValues []string
	for key, value := range originalFeature {
		if key != "id" && key != "geom" {
			columnNames = append(columnNames, fmt.Sprintf(`"%s"`, key))
			if mappingField != "" && key == mappingField {
				columnValues = append(columnValues, fmt.Sprintf("%d", maxMappingID+1))
				continue
			}
			switch v := value.(type) {
			case nil:
				columnValues = append(columnValues, "NULL")
			case string:
				escapedValue := strings.ReplaceAll(v, "'", "''")
				columnValues = append(columnValues, fmt.Sprintf("'%s'", escapedValue))
			case time.Time:
				if v.IsZero() {
					columnValues = append(columnValues, "NULL")
				} else {
					columnValues = append(columnValues, fmt.Sprintf("'%s'", v.Format("2006-01-02")))
				}
			case int, int32, int64, float32, float64:
				columnValues = append(columnValues, fmt.Sprintf("%v", v))
			case bool:
				columnValues = append(columnValues, fmt.Sprintf("%t", v))
			default:
				if t, ok := value.(*time.Time); ok {
					if t == nil || t.IsZero() {
						columnValues = append(columnValues, "NULL")
					} else {
						columnValues = append(columnValues, fmt.Sprintf("'%s'", t.Format("2006-01-02")))
					}
				} else {
					columnValues = append(columnValues, fmt.Sprintf("'%v'", v))
				}
			}
		}
	}
	columnsStr := ""
	valuesStr := ""
	if len(columnNames) > 0 {
		columnsStr = ", " + strings.Join(columnNames, ", ")
		valuesStr = ", " + strings.Join(columnValues, ", ")
	}
	newID := maxID + 1
	// 聚合SQL：使用ST_Collect（不做union，保留Multi结构）
	aggregateSQL := fmt.Sprintf(`
		WITH dumped AS (
			SELECT 
				(ST_Dump(geom)).geom AS single_geom
			FROM "%s"
			WHERE id IN (%s)
		),
		aggregated AS (
			SELECT 
				ST_Collect(single_geom) AS aggregated_geom
			FROM dumped
		)
		INSERT INTO "%s" (id, geom%s)
		SELECT 
			%d AS id,
			aggregated_geom
			%s
		FROM aggregated
		RETURNING ST_AsGeoJSON(geom) AS geojson, id
	`, LayerName, idsStr, LayerName, columnsStr, newID, valuesStr)
	type AggregatorResult struct {
		Geojson string
		ID      int32
	}
	var aggregatorResult AggregatorResult
	if err := tx.Raw(aggregateSQL).Scan(&aggregatorResult).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "聚合并插入新要素失败: " + err.Error(), "data": ""})
		return
	}
	if aggregatorResult.Geojson == "" {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "聚合失败，未生成有效几何", "data": ""})
		return
	}
	// 获取原要素GeoJSON（事务提交前）
	getdata2 := getDatas{TableName: LayerName, ID: jsonData.IDs}
	oldGeo := GetGeos(getdata2)
	oldGeojson, _ := json.Marshal(oldGeo)
	delObjJSON := DelIDGen(oldGeo)
	// 删除原要素
	deleteSQL := fmt.Sprintf(`DELETE FROM "%s" WHERE id IN (%s)`, LayerName, idsStr)
	if err := tx.Exec(deleteSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "删除原要素失败: " + err.Error(), "data": ""})
		return
	}
	// 提交事务
	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "提交事务失败: " + err.Error(), "data": ""})
		return
	}
	GetPdata := getData{TableName: LayerName, ID: aggregatorResult.ID}
	newGeo := GetGeo(GetPdata)
	newGeoJson, _ := json.Marshal(newGeo)
	// ========== 维护映射表 ==========
	session := GetOrCreateSession(DB, LayerName, "")
	MarkMappingDeleted(DB, LayerName, jsonData.IDs)
	CreateDerivedMappingsMultiParent(DB, LayerName, []int32{aggregatorResult.ID}, jsonData.IDs, session.ID)
	// ========== 记录操作 ==========
	inputIDs := MarshalIDs(jsonData.IDs)
	outputIDs := MarshalIDs([]int32{aggregatorResult.ID})
	RecordResult := models.GeoRecord{
		TableName:    jsonData.LayerName,
		Type:         "要素聚合",
		Date:         timeNowStr(),
		OldGeojson:   oldGeojson,
		NewGeojson:   newGeoJson,
		DelObjectIDs: delObjJSON,
		SessionID:    session.ID,
		SeqNo:        GetNextSeqNo(DB, session.ID),
		InputIDs:     inputIDs,
		OutputIDs:    outputIDs,
	}
	DB.Create(&RecordResult)
	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": fmt.Sprintf("成功聚合%d个要素为1个Multi类型要素，使用ID(%d)的属性", len(jsonData.IDs), jsonData.MainID),
		"data": gin.H{
			"geojson": aggregatorResult.Geojson,
			"new_id":  aggregatorResult.ID,
			"main_id": jsonData.MainID,
		},
	})
}

// 要素平移
type OffsetData struct {
	LayerName string  `json:"LayerName"`
	IDs       []int32 `json:"ids"`
	XOffset   float64 `json:"xOffset"` // 米
	YOffset   float64 `json:"yOffset"` // 米
}

const (
	EarthRadiusMeters = 6378137.0           // WGS84椭球体长半轴，更精确
	EarthFlattening   = 1.0 / 298.257223563 // WGS84扁率
	DegreesToRadians  = math.Pi / 180.0
	RadiansToDegrees  = 180.0 / math.Pi
)

// 计算给定纬度处的地球半径（考虑椭球体形状）
func getEarthRadiusAtLatitude(latitude float64) float64 {
	latRad := latitude * DegreesToRadians
	sinLat := math.Sin(latRad)
	cosLat := math.Cos(latRad)

	// WGS84椭球体参数
	a := EarthRadiusMeters         // 长半轴
	b := a * (1 - EarthFlattening) // 短半轴

	// 计算该纬度处的地球半径
	numerator := math.Pow(a*a*cosLat, 2) + math.Pow(b*b*sinLat, 2)
	denominator := math.Pow(a*cosLat, 2) + math.Pow(b*sinLat, 2)

	return math.Sqrt(numerator / denominator)
}

// 计算纬度方向的子午线曲率半径
func getMeridionalRadiusOfCurvature(latitude float64) float64 {
	latRad := latitude * DegreesToRadians
	sinLat := math.Sin(latRad)

	a := EarthRadiusMeters
	e2 := 2*EarthFlattening - EarthFlattening*EarthFlattening // 第一偏心率的平方

	return a * (1 - e2) / math.Pow(1-e2*sinLat*sinLat, 1.5)
}

// 计算纬度方向的卯酉圈曲率半径
func getPrimeVerticalRadiusOfCurvature(latitude float64) float64 {
	latRad := latitude * DegreesToRadians
	sinLat := math.Sin(latRad)

	a := EarthRadiusMeters
	e2 := 2*EarthFlattening - EarthFlattening*EarthFlattening

	return a / math.Sqrt(1-e2*sinLat*sinLat)
}

// 高精度米转纬度度数
func metersToDegreesLat(meters float64, latitude float64) float64 {
	// 使用子午线曲率半径
	M := getMeridionalRadiusOfCurvature(latitude)
	return meters * RadiansToDegrees / M
}

// 高精度米转经度度数
func metersToDegreeeLon(meters float64, latitude float64) float64 {
	// 使用卯酉圈曲率半径
	N := getPrimeVerticalRadiusOfCurvature(latitude)
	latRad := latitude * DegreesToRadians
	return meters * RadiansToDegrees / (N * math.Cos(latRad))
}

// 更精确的度数转米（用于验证）
func degreesToMetersLat(degrees float64, latitude float64) float64 {
	M := getMeridionalRadiusOfCurvature(latitude)
	return degrees * DegreesToRadians * M
}

func degreesToMetersLon(degrees float64, latitude float64) float64 {
	N := getPrimeVerticalRadiusOfCurvature(latitude)
	latRad := latitude * DegreesToRadians
	return degrees * DegreesToRadians * N * math.Cos(latRad)
}

// OffsetFeature 要素平移
func (uc *UserController) OffsetFeature(c *gin.Context) {
	var jsonData OffsetData
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    500,
			"message": err.Error(),
			"data":    "",
		})
		return
	}

	if len(jsonData.IDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "至少需要选择1个要素进行平移",
			"data":    "",
		})
		return
	}

	if jsonData.XOffset == 0 && jsonData.YOffset == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "X偏移量和Y偏移量不能同时为0",
			"data":    "",
		})
		return
	}

	DB := models.DB
	LayerName := jsonData.LayerName

	var schema models.MySchema
	result := DB.Where("en = ?", LayerName).First(&schema)
	if result.Error != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    500,
			"message": result.Error.Error(),
			"data":    "",
		})
		return
	}

	idFieldName := "id"

	tx := DB.Begin()
	if tx.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "开启事务失败: " + tx.Error.Error(),
			"data":    "",
		})
		return
	}

	idList := make([]string, len(jsonData.IDs))
	for i, id := range jsonData.IDs {
		idList[i] = fmt.Sprintf("%d", id)
	}
	idsStr := strings.Join(idList, ",")

	var count int64
	checkSQL := fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE "%s" IN (%s)`, LayerName, "id", idsStr)
	if err := tx.Raw(checkSQL).Count(&count).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "验证要素失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	if int(count) != len(jsonData.IDs) {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    404,
			"message": fmt.Sprintf("部分ID不存在，期望%d个，实际找到%d个", len(jsonData.IDs), count),
			"data":    "",
		})
		return
	}

	getdata2 := getDatas{
		TableName: LayerName,
		ID:        jsonData.IDs,
	}
	oldGeo := GetGeos(getdata2)
	oldGeojson, _ := json.Marshal(oldGeo)

	var centerLat float64
	latSQL := fmt.Sprintf(`
		SELECT AVG(ST_Y(ST_Centroid(geom))) FROM "%s" WHERE "%s" IN (%s)
	`, LayerName, idFieldName, idsStr)

	if err := tx.Raw(latSQL).Scan(&centerLat).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "获取纬度信息失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	offsetLat := metersToDegreesLat(jsonData.YOffset, centerLat)
	offsetLon := metersToDegreeeLon(jsonData.XOffset, centerLat)

	offsetSQL := fmt.Sprintf(`
		UPDATE "%s"
		SET geom = ST_Translate(geom, %f, %f)
		WHERE "%s" IN (%s)RETURNING "%s", ST_AsGeoJSON(geom) AS geojson
	`, LayerName, offsetLon, offsetLat, idFieldName, idsStr, idFieldName)

	type OffsetResult struct {
		ID      int32
		Geojson string
	}
	var offsetResults []OffsetResult

	if err := tx.Raw(offsetSQL).Scan(&offsetResults).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "平移要素失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	if len(offsetResults) == 0 {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "平移失败，未更新任何要素",
			"data":    "",
		})
		return
	}

	pgmvt.DelMVTALL(DB, jsonData.LayerName)

	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "提交事务失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	getdata3 := getDatas{
		TableName: LayerName,
		ID:        jsonData.IDs,
	}
	newGeo := GetGeos(getdata3)
	newGeojson, _ := json.Marshal(newGeo)

	delObjectIDs := DelIDGen(oldGeo)

	session := GetOrCreateSession(DB, LayerName, "")
	inputIDs := MarshalIDs(jsonData.IDs)
	outputIDs := MarshalIDs(jsonData.IDs)

	RecordResult := models.GeoRecord{
		TableName:    jsonData.LayerName,
		Type:         "要素平移",
		Date:         timeNowStr(),
		OldGeojson:   oldGeojson,
		NewGeojson:   newGeojson,
		DelObjectIDs: delObjectIDs,
		SessionID:    session.ID,
		SeqNo:        GetNextSeqNo(DB, session.ID),
		InputIDs:     inputIDs,
		OutputIDs:    outputIDs,
	}
	DB.Create(&RecordResult)

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": fmt.Sprintf("成功平移%d个要素，X偏移: %f米, Y偏移: %f米", len(jsonData.IDs), jsonData.XOffset, jsonData.YOffset),
		"data": gin.H{
			"count":      len(offsetResults),
			"offset_x_m": jsonData.XOffset,
			"offset_y_m": jsonData.YOffset,
			"offset_lon": offsetLon,
			"offset_lat": offsetLat, "id_field": idFieldName,
		},
	})
}

// 面要素去重叠
type AreaOnAreaData struct {
	LayerName string  `json:"LayerName"`
	IDs       []int32 `json:"ids"`
}

func (uc *UserController) AreaOnAreaAnalysis(c *gin.Context) {
	var jsonData AreaOnAreaData
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": err.Error(), "data": ""})
		return
	}
	if len(jsonData.IDs) < 2 {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "至少需要选择2个要素进行叠加分析", "data": ""})
		return
	}
	DB := models.DB
	LayerName := jsonData.LayerName
	EXT := GetFileExt(LayerName)
	var schema models.MySchema
	result := DB.Where("en = ?", LayerName).First(&schema)
	if result.Error != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": result.Error.Error(), "data": ""})
		return
	}
	tx := DB.Begin()
	if tx.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "开启事务失败: " + tx.Error.Error(), "data": ""})
		return
	}
	lockSQL := fmt.Sprintf(`SELECT pg_advisory_xact_lock(hashtext('%s_id_lock'))`, LayerName)
	if err := tx.Exec(lockSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "获取锁失败: " + err.Error(), "data": ""})
		return
	}
	idList := make([]string, len(jsonData.IDs))
	for i, id := range jsonData.IDs {
		idList[i] = fmt.Sprintf("%d", id)
	}
	idsStr := strings.Join(idList, ",")
	// 验证所有ID
	var count int64
	checkSQL := fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE id IN (%s)`, LayerName, idsStr)
	if err := tx.Raw(checkSQL).Count(&count).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "验证要素失败: " + err.Error(), "data": ""})
		return
	}
	if int(count) != len(jsonData.IDs) {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{"code": 404, "message": fmt.Sprintf("部分ID不存在，期望%d个，实际找到%d个", len(jsonData.IDs), count), "data": ""})
		return
	}
	// 检查几何类型
	type GeometryTypeData struct {
		ID   int32
		Type string
	}
	var geomTypes []GeometryTypeData
	checkGeomSQL := fmt.Sprintf(`SELECT id, ST_GeometryType(geom) AS type FROM "%s" WHERE id IN (%s)`, LayerName, idsStr)
	if err := tx.Raw(checkGeomSQL).Scan(&geomTypes).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "获取几何类型失败: " + err.Error(), "data": ""})
		return
	}
	nonPolygons := []int32{}
	for _, geom := range geomTypes {
		if geom.Type != "ST_Polygon" && geom.Type != "ST_MultiPolygon" {
			nonPolygons = append(nonPolygons, geom.ID)
		}
	}
	if len(nonPolygons) > 0 {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": fmt.Sprintf("以下ID不是面类型: %v", nonPolygons), "data": ""})
		return
	}
	// 获取表结构
	var sampleFeature map[string]interface{}
	getSampleSQL := fmt.Sprintf(`SELECT * FROM "%s" LIMIT 1`, LayerName)
	if err := tx.Raw(getSampleSQL).Scan(&sampleFeature).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "获取表结构失败: " + err.Error(), "data": ""})
		return
	}
	var additionalColumns []string
	for key := range sampleFeature {
		if key != "id" && key != "geom" {
			additionalColumns = append(additionalColumns, key)
		}
	}
	// 获取原始要素GeoJSON
	getdata := getDatas{TableName: LayerName, ID: jsonData.IDs}
	oldGeo := GetGeos(getdata)
	oldGeojson, _ := json.Marshal(oldGeo)
	// 删除MVT缓存
	if len(oldGeo.Features) >= 10 {
		pgmvt.DelMVTALL(DB, jsonData.LayerName)
	} else {
		for _, feature := range oldGeo.Features {
			pgmvt.DelMVT(DB, jsonData.LayerName, feature.Geometry)
		}
	}
	// 获取最大ID和映射字段最大值
	var maxID int32
	getMaxIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX(id), 0) FROM "%s"`, LayerName)
	if err := tx.Raw(getMaxIDSQL).Scan(&maxID).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "获取最大ID失败: " + err.Error(), "data": ""})
		return
	}
	var mappingField string
	var maxMappingValue int32
	if EXT == ".shp" {
		mappingField = "objectid"
	} else if EXT == ".gdb" {
		mappingField = "fid"
	}
	delObjectIDs := DelIDGen(oldGeo)
	if mappingField != "" {
		getMaxMappingSQL := fmt.Sprintf(`SELECT COALESCE(MAX("%s"), 0) FROM "%s"`, mappingField, LayerName)
		if tx.Raw(getMaxMappingSQL).Scan(&maxMappingValue).Error != nil {
			maxMappingValue = 0
		}
	}
	// GDAL叠加分析
	gdallayer, err := Gogeo.ConvertGeoJSONToGDALLayer(&oldGeo, LayerName)
	if err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "转换GeoJSON失败: " + err.Error(), "data": ""})
		return
	}
	analysisGDAL, err := Gogeo.AreaOnAreaAnalysis(gdallayer, 0.00000001)
	if err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "叠加分析失败: " + err.Error(), "data": ""})
		return
	}
	analysisGeoJSON, err := Gogeo.LayerToGeoJSON(analysisGDAL)
	if err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "转换分析结果失败: " + err.Error(), "data": ""})
		return
	}
	if len(analysisGeoJSON.Features) == 0 {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "叠加分析失败，未生成任何结果几何", "data": ""})
		return
	}
	// 插入新要素
	newFeatureIDs := make([]int32, 0)
	insertCols := "id, geom"
	for _, col := range additionalColumns {
		insertCols += fmt.Sprintf(`, "%s"`, col)
	}
	insertSQL := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES `, LayerName, insertCols)
	valueStrings := make([]string, 0)
	for i, feature := range analysisGeoJSON.Features {
		newID := maxID + int32(i) + 1
		newFeatureIDs = append(newFeatureIDs, newID)
		geojsonGeom := geojson.NewGeometry(feature.Geometry)
		geomBytes, err := json.Marshal(geojsonGeom)
		if err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": fmt.Sprintf("序列化几何对象失败: %v", err), "data": ""})
			return
		}
		geomStr := string(geomBytes)
		geomStr = strings.ReplaceAll(geomStr, "'", "''")
		valueList := fmt.Sprintf(`%d, ST_GeomFromGeoJSON('%s')`, newID, geomStr)
		featureProps := feature.Properties
		for _, col := range additionalColumns {
			if mappingField != "" && col == mappingField {
				newMappingValue := maxMappingValue + int32(i) + 1
				valueList += fmt.Sprintf(`, %d`, newMappingValue)
			} else {
				if val, exists := featureProps[col]; exists && val != nil {
					valueList += formatFieldValue(val)
				} else {
					valueList += `, NULL`
				}
			}
		}
		valueStrings = append(valueStrings, fmt.Sprintf(`(%s)`, valueList))
	}
	if len(valueStrings) == 0 {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "无法生成有效的插入语句", "data": ""})
		return
	}
	insertSQL += strings.Join(valueStrings, ",")
	if err := tx.Exec(insertSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "插入新要素失败: " + err.Error(), "data": ""})
		return
	}
	// 删除原要素
	deleteSQL := fmt.Sprintf(`DELETE FROM "%s" WHERE id IN (%s)`, LayerName, idsStr)
	if err := tx.Exec(deleteSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "删除原要素失败: " + err.Error(), "data": ""})
		return
	}
	// 提交事务
	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "提交事务失败: " + err.Error(), "data": ""})
		return
	}
	// 获取新要素信息
	getdata2 := getDatas{TableName: LayerName, ID: newFeatureIDs}
	newGeo := GetGeos(getdata2)
	newGeojson, _ := json.Marshal(newGeo)
	// ========== 维护映射表 ==========
	session := GetOrCreateSession(DB, LayerName, "")
	MarkMappingDeleted(DB, LayerName, jsonData.IDs)
	// 为每个新要素创建派生映射
	for _, nid := range newFeatureIDs {
		mapping := models.OriginMapping{
			TableName:       LayerName,
			PostGISID:       nid,
			SourceObjectID:  -1,
			Origin:          "derived",
			ParentPostGISID: jsonData.IDs[0],
			SessionID:       session.ID,
			IsDeleted:       false,
		}
		DB.Create(&mapping)
	}
	// ========== 记录操作 ==========
	inputIDs := MarshalIDs(jsonData.IDs)
	outputIDs := MarshalIDs(newFeatureIDs)
	// 统计重叠和非重叠
	overlapCount := 0
	nonOverlapCount := 0
	for _, feature := range analysisGeoJSON.Features {
		if featureType, ok := feature.Properties["type"].(string); ok {
			if featureType == "overlap" {
				overlapCount++
			} else if featureType == "non_overlap" {
				nonOverlapCount++
			}
		}
	}

	RecordResult := models.GeoRecord{
		TableName:    jsonData.LayerName,
		Type:         "面要素去重叠",
		Date:         timeNowStr(),
		OldGeojson:   oldGeojson,
		NewGeojson:   newGeojson,
		DelObjectIDs: delObjectIDs,
		SessionID:    session.ID,
		SeqNo:        GetNextSeqNo(DB, session.ID),
		InputIDs:     inputIDs,
		OutputIDs:    outputIDs,
	}
	DB.Create(&RecordResult)

	returnData := make([]gin.H, len(newGeo.Features))
	for i, feature := range newGeo.Features {
		returnData[i] = gin.H{
			"id":         newFeatureIDs[i],
			"type":       analysisGeoJSON.Features[i].Properties["type"],
			"geojson":    feature.Geometry,
			"properties": feature.Properties,
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"message": fmt.Sprintf("成功完成叠加分析,生成%d个新要素（重叠区域%d个，非重叠区域%d个）",
			len(analysisGeoJSON.Features), overlapCount, nonOverlapCount),
		"data": gin.H{
			"original_ids":      jsonData.IDs,
			"total_results":     len(analysisGeoJSON.Features),
			"overlap_count":     overlapCount,
			"non_overlap_count": nonOverlapCount,
			"new_ids":           newFeatureIDs,
			"features":          returnData,
		},
	})
}

// formatFieldValue 格式化字段值为SQL字符串
func formatFieldValue(val interface{}) string {
	if val == nil {
		return `, NULL`
	}

	switch v := val.(type) {
	case string:
		escapedVal := strings.ReplaceAll(v, "'", "''")
		return fmt.Sprintf(`, '%s'`, escapedVal)
	case int:
		return fmt.Sprintf(`, %d`, v)
	case int32:
		return fmt.Sprintf(`, %d`, v)
	case int64:
		return fmt.Sprintf(`, %d`, v)
	case float32:
		return fmt.Sprintf(`, %v`, v)
	case float64:
		return fmt.Sprintf(`, %v`, v)
	case bool:
		return fmt.Sprintf(`, %t`, v)
	case json.Number:
		return fmt.Sprintf(`, %s`, v.String())
	default:
		// 尝试转换为字符串
		strVal := fmt.Sprintf("%v", v)
		escapedVal := strings.ReplaceAll(strVal, "'", "''")
		return fmt.Sprintf(`, '%s'`, escapedVal)
	}
}

func GetFileExt(TableName string) string {
	DB := models.DB
	var Schema models.MySchema
	if err := DB.Where("en = ?", TableName).First(&Schema).Error; err != nil {
		return ""
	}
	var sourceConfigs []pgmvt.SourceConfig

	if err := json.Unmarshal(Schema.Source, &sourceConfigs); err != nil {
		return ""
	}
	sourceConfig := sourceConfigs[0]
	// 判断源头文件是gdb还是shp
	Soucepath := sourceConfig.SourcePath

	// 获取文件扩展名
	ext := strings.ToLower(filepath.Ext(Soucepath))

	// 判断是否为GDB(GDB通常是文件夹,扩展名为.gdb)
	isGDB := ext == ".gdb" || strings.HasSuffix(strings.ToLower(Soucepath), ".gdb")
	isSHP := ext == ".shp"

	if !isGDB && !isSHP {

		return ""
	}
	return ext
}

// buildWhereClause 构建WHERE子句
// keyAttribute: 关键字段名(如 OBJECTID, FID等)
// objectIDs: 要删除的ID列表
func buildWhereClause(keyAttribute string, objectIDs []int32) string {
	if len(objectIDs) == 0 {
		return ""
	}

	// 如果没有指定关键字段,默认使用OBJECTID
	if keyAttribute == "" {
		keyAttribute = "OBJECTID"
	}

	if len(objectIDs) == 1 {
		return fmt.Sprintf("%s = %d", keyAttribute, objectIDs[0])
	}

	// 构建 IN 子句
	ids := make([]string, len(objectIDs))
	for i, id := range objectIDs {
		ids[i] = fmt.Sprintf("%d", id)
	}

	return fmt.Sprintf("%s IN (%s)", keyAttribute, strings.Join(ids, ", "))
}

// mapFieldsToSource 将GeoJSON中的处理后字段名映射回源文件的原始字段名
// fc: 原始FeatureCollection (字段名是ProcessedName)
// attMap: 字段映射关系
// 返回: 字段名已替换为OriginalName的新FeatureCollection
func mapFieldsToSource(fc *geojson.FeatureCollection, attMap []pgmvt.ProcessedFieldInfo) (*geojson.FeatureCollection, error) {
	if fc == nil {
		return nil, fmt.Errorf("FeatureCollection为空")
	}

	// 创建映射表: ProcessedName -> OriginalName
	fieldMapping := make(map[string]string)
	for _, field := range attMap {
		fieldMapping[field.ProcessedName] = field.OriginalName
	}

	// 创建新的FeatureCollection
	mappedFC := &geojson.FeatureCollection{
		Type:     fc.Type,
		Features: make([]*geojson.Feature, len(fc.Features)),
	}

	// 遍历每个Feature
	for i, feature := range fc.Features {
		if feature == nil {
			continue
		}

		// 创建新Feature
		mappedFeature := &geojson.Feature{
			Type:       feature.Type,
			Geometry:   feature.Geometry, // 几何保持不变
			Properties: make(map[string]interface{}),
			ID:         feature.ID,
		}

		// 映射属性字段
		for processedName, value := range feature.Properties {
			// 查找原始字段名
			if originalName, exists := fieldMapping[processedName]; exists {
				mappedFeature.Properties[originalName] = value
			} else {
				// 如果映射表中没有,保留原字段名(可能是未处理的字段)
				mappedFeature.Properties[processedName] = value
			}
		}

		mappedFC.Features[i] = mappedFeature
	}

	return mappedFC, nil
}

func mapField(fc string, attMap []pgmvt.ProcessedFieldInfo) string {
	for _, field := range attMap {
		if field.ProcessedName == fc {
			return field.OriginalName
		}
	}

	// 如果没有找到映射，返回原字段名
	return fc

}

// controllers/sync_controller.go (更新 mapPostGISTypeToGDB 调用部分)

// mapPostGISTypeToGDB 扩展的类型映射函数
func mapPostGISTypeToGDB(pgType string) (Gogeo.FieldType, int, int) {
	// 转换为小写便于匹配
	pgType = strings.ToLower(strings.TrimSpace(pgType))

	// 解析类型定义
	baseType, params := parseTypeParams(pgType)

	// 类型映射
	switch baseType {
	// 整数类型
	case "smallint", "int2":
		return Gogeo.FieldTypeInteger, 0, 0

	case "integer", "int", "int4", "serial":
		return Gogeo.FieldTypeInteger, 0, 0

	case "bigint", "int8", "bigserial":
		return Gogeo.FieldTypeInteger64, 0, 0

	// 浮点数类型
	case "real", "float4":
		return Gogeo.FieldTypeReal, 0, 0

	case "double precision", "float8", "float", "double":
		return Gogeo.FieldTypeReal, 0, 0

	case "numeric", "decimal":
		width := 18
		precision := 6
		if len(params) >= 1 {
			width = params[0]
		}
		if len(params) >= 2 {
			precision = params[1]
		}
		return Gogeo.FieldTypeReal, width, precision

	// 字符串类型
	case "character varying", "varchar":
		width := 254
		if len(params) >= 1 {
			width = params[0]
		}
		if width > 2147483647 {
			width = 2147483647
		}
		return Gogeo.FieldTypeString, width, 0

	case "character", "char":
		width := 1
		if len(params) >= 1 {
			width = params[0]
		}
		return Gogeo.FieldTypeString, width, 0

	case "text":
		return Gogeo.FieldTypeString, 2147483647, 0

	// 日期时间类型
	case "date":
		return Gogeo.FieldTypeDate, 0, 0

	case "time", "time without time zone", "time with time zone", "timetz":
		return Gogeo.FieldTypeTime, 0, 0

	case "timestamp", "timestamp without time zone", "timestamp with time zone", "timestamptz":
		return Gogeo.FieldTypeDateTime, 0, 0

	// 二进制类型
	case "bytea", "bytes":
		return Gogeo.FieldTypeBinary, 0, 0

	// 布尔类型
	case "boolean", "bool":
		return Gogeo.FieldTypeInteger, 0, 0

	// UUID类型
	case "uuid":
		return Gogeo.FieldTypeString, 36, 0

	default:
		// 默认作为字符串处理
		width := 254
		if len(params) >= 1 {
			width = params[0]
		}
		return Gogeo.FieldTypeString, width, 0
	}
}

// parseTypeParams 解析类型参数
func parseTypeParams(pgType string) (baseType string, params []int) {
	re := regexp.MustCompile(`^([a-z\s]+)(?:\(([^)]+)\))?$`)
	matches := re.FindStringSubmatch(pgType)

	if len(matches) < 2 {
		return pgType, nil
	}

	baseType = strings.TrimSpace(matches[1])

	if len(matches) >= 3 && matches[2] != "" {
		paramStrs := strings.Split(matches[2], ",")
		for _, p := range paramStrs {
			if val, err := strconv.Atoi(strings.TrimSpace(p)); err == nil {
				params = append(params, val)
			}
		}
	}

	return baseType, params
}

// 线面叠加分析
type LineOnPolygon struct {
	Polygon geojson.Feature
	Line    geojson.Feature
}

func (uc *UserController) LineOnPolygonOverlay(c *gin.Context) {
	// 声明数据结构体变量
	var data LineOnPolygon

	// 绑定 JSON 请求数据并检查错误
	if err := c.BindJSON(&data); err != nil {
		// 记录请求绑定错误日志

		// 返回 400 Bad Request 状态码和错误信息
		c.JSON(400, gin.H{
			"code":    400,
			"message": "请求数据格式错误",
			"error":   err.Error(),
		})
		return
	}

	// 调用地理数据处理函数，传入线、多边形和精度参数
	resultFeature, err := Gogeo.RemoveLinePolygonBoundaryOverlapFromGeoJSON(
		&data.Line,
		&data.Polygon,
		0.00000001,
	)
	// 检查处理过程中是否发生错误
	if err != nil {
		// 记录处理失败的错误日志
		// 返回 500 Internal Server Error 状态码
		c.JSON(500, gin.H{
			"code":    500,
			"message": "地理数据处理失败",
			"error":   err.Error(),
		})
		return
	}
	// 处理成功，返回 200 OK 状态码和结果数据
	c.JSON(200, gin.H{
		"code":    200,
		"data":    resultFeature,
		"message": "处理成功",
	})
}

// 批量删除
type delDatas struct {
	TableName string  `json:"TableName"`
	IDs       []int32 `json:"IDs"`
	Username  string  `json:"Username"`
	BZ        string  `json:"BZ"`
}

func (uc *UserController) DelGeosToSchema(c *gin.Context) {
	var jsonData delDatas
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "请求参数错误", "error": err.Error()})
		return
	}
	if len(jsonData.IDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "至少需要选择1个要素进行删除"})
		return
	}
	DB := models.DB
	getdata := getDatas{TableName: jsonData.TableName, ID: jsonData.IDs}
	geos := GetGeos(getdata)
	if len(geos.Features) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"code": 404, "message": "未找到指定的要素"})
		return
	}
	idList := make([]string, len(jsonData.IDs))
	for i, id := range jsonData.IDs {
		idList[i] = fmt.Sprintf("%d", id)
	}
	idsStr := strings.Join(idList, ",")
	sql := fmt.Sprintf(`DELETE FROM "%s" WHERE id IN (%s)`, jsonData.TableName, idsStr)
	result := DB.Exec(sql)
	if err := result.Error; err != nil {
		log.Printf("Failed to delete records: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "删除要素失败", "error": err.Error()})
		return
	}
	rowsAffected := result.RowsAffected
	if rowsAffected == 0 {
		c.JSON(http.StatusOK, gin.H{"code": 200, "message": "没有要素被删除", "count": 0})
		return
	}
	pgmvt.DelMVTALL(DB, jsonData.TableName)
	// 创建会话 + 维护映射
	session := GetOrCreateSession(DB, jsonData.TableName, jsonData.Username)
	MarkMappingDeleted(DB, jsonData.TableName, jsonData.IDs)
	delObjJSON := DelIDGen(geos)
	OldGeojson, _ := json.Marshal(geos)
	recordResult := models.GeoRecord{
		TableName:    jsonData.TableName,
		Username:     jsonData.Username,
		Type:         "批量要素删除",
		Date:         timeNowStr(),
		DelObjectIDs: delObjJSON,
		OldGeojson:   OldGeojson,
		BZ:           jsonData.BZ,
		SessionID:    session.ID,
		SeqNo:        GetNextSeqNo(DB, session.ID),
		InputIDs:     MarshalIDs(jsonData.IDs),
		OutputIDs:    MarshalIDs([]int32{}),
	}
	if err := DB.Create(&recordResult).Error; err != nil {
		log.Printf("Failed to create geo record: %v", err)
	}
	c.JSON(http.StatusOK, gin.H{"code": 200, "message": fmt.Sprintf("成功删除%d个要素", rowsAffected), "count": rowsAffected})
}
