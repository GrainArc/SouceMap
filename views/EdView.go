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
	c.BindJSON(&jsonData) //将前端geojson转换为geo对象
	DB := models.DB
	sql := fmt.Sprintf(`SELECT MAX(id) AS max_id FROM %s;`, jsonData.TableName)
	var maxid int
	DB.Raw(sql).Scan(&maxid)
	jsonData.GeoJson.Features[0].Properties["id"] = maxid + 1
	methods.SavaGeojsonToTable(DB, jsonData.GeoJson, jsonData.TableName)
	NewGeojson, _ := json.Marshal(jsonData.GeoJson)
	result := models.GeoRecord{TableName: jsonData.TableName, GeoID: int32(maxid + 1), Username: jsonData.Username, Type: "要素添加", Date: time.Now().Format("2006-01-02 15:04:05"), NewGeojson: NewGeojson, BZ: jsonData.BZ}
	DB.Create(&result)
	//删除MVT
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
	c.BindJSON(&jsonData) //将前端geojson转换为geo对象
	DB := models.DB
	getData := getData{ID: jsonData.ID, TableName: jsonData.TableName}
	geo := GetGeo(getData)
	OldGeojson, _ := json.Marshal(geo)
	sql := fmt.Sprintf(`DELETE FROM %s WHERE id = %d;`, jsonData.TableName, jsonData.ID)

	aa := DB.Exec(sql)
	if err := aa.Error; err != nil {
		log.Printf("Failed to delete record: %v", err)
	} else {
		rowsAffected := aa.RowsAffected
		if rowsAffected == 0 {
			log.Println("No records deleted.")
		} else {
			log.Printf("Deleted %d record(s).", rowsAffected)
		}
	}
	var delObjectIDs []int64
	// 提取所有 ObjectID
	for _, feature := range geo.Features {
		objID, _ := extractObjectID(feature.Properties)
		delObjectIDs = append(delObjectIDs, objID)
	}
	// 序列化 ObjectIDs
	delObjJSON, _ := json.Marshal(delObjectIDs)

	// 构建记录
	result := &models.GeoRecord{
		TableName:    jsonData.TableName,
		GeoID:        jsonData.ID,
		Username:     jsonData.Username,
		Type:         "要素删除",
		Date:         time.Now().Format("2006-01-02 15:04:05"),
		DelObjectIDs: delObjJSON,
		OldGeojson:   OldGeojson,
		BZ:           jsonData.BZ,
	}
	fmt.Println(result)
	if err := DB.Create(&result).Error; err != nil {
		// 记录创建失败的错误信息
		log.Printf("Failed to create geo record: %v", err)
		// 返回错误响应

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

func (uc *UserController) ChangeGeoToSchema(c *gin.Context) {
	var jsonData geoData
	c.BindJSON(&jsonData) //将前端geojson转换为geo对象s
	DB := models.DB
	getData := getData{ID: jsonData.ID, TableName: jsonData.TableName}
	geo := GetGeo(getData)
	OldGeojson, _ := json.MarshalIndent(geo, "", "  ")
	methods.UpdateGeojsonToTable(DB, jsonData.GeoJson, jsonData.TableName, jsonData.ID)
	NewGeojson, _ := json.MarshalIndent(jsonData.GeoJson, "", "  ")
	delObjJSON := DelIDGen(geo)
	result := models.GeoRecord{TableName: jsonData.TableName,
		GeoID:        jsonData.ID,
		Username:     jsonData.Username,
		Type:         "要素修改",
		Date:         time.Now().Format("2006-01-02 15:04:05"),
		OldGeojson:   OldGeojson,
		NewGeojson:   NewGeojson,
		DelObjectIDs: delObjJSON,
		BZ:           jsonData.BZ}
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
	var response []GeoRecordResponse
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

// 还原图形
func (uc *UserController) BackUpRecord(c *gin.Context) {
	ID := c.Query("ID")
	DB := models.DB
	var aa models.GeoRecord
	DB.Where("id = ?", ID).Find(&aa)

	switch aa.Type {
	case "要素添加":
		DB.Table(aa.TableName).Where("id = ?", aa.GeoID).Delete(nil)
		var featureCollection struct {
			Features []*geojson.Feature `json:"features"`
		}
		json.Unmarshal(aa.NewGeojson, &featureCollection)
		pgmvt.DelMVT(DB, aa.TableName, featureCollection.Features[0].Geometry)
	case "要素删除":
		var featureCollection geojson.FeatureCollection
		json.Unmarshal(aa.OldGeojson, &featureCollection)
		methods.SavaGeojsonToTable(DB, featureCollection, aa.TableName)
		pgmvt.DelMVT(DB, aa.TableName, featureCollection.Features[0].Geometry)
	case "要素修改":
		var featureCollection geojson.FeatureCollection
		json.Unmarshal(aa.OldGeojson, &featureCollection)
		var featureCollection2 geojson.FeatureCollection
		json.Unmarshal(aa.NewGeojson, &featureCollection2)
		methods.UpdateGeojsonToTable(DB, featureCollection, aa.TableName, aa.GeoID)
		pgmvt.DelMVT(DB, aa.TableName, featureCollection2.Features[0].Geometry)
		pgmvt.DelMVT(DB, aa.TableName, featureCollection.Features[0].Geometry)
	case "要素分割":
		var featureCollection geojson.FeatureCollection
		json.Unmarshal(aa.OldGeojson, &featureCollection)
		var featureCollection2 geojson.FeatureCollection
		json.Unmarshal(aa.NewGeojson, &featureCollection2)
		pgmvt.DelMVT(DB, aa.TableName, featureCollection.Features[0].Geometry)
		for _, feature := range featureCollection2.Features {
			var id int32
			switch v := feature.Properties["id"].(type) {
			case float64:
				id = int32(v)
			case int:
				id = int32(v)
			case int32:
				id = v
			default:
				log.Printf("unexpected type for id: %T", v)
				return
			}
			DB.Table(aa.TableName).Where("id = ?", id).Delete(nil)
		}
		methods.SavaGeojsonToTable(DB, featureCollection, aa.TableName)
	case "要素合并":
		var featureCollection geojson.FeatureCollection
		json.Unmarshal(aa.OldGeojson, &featureCollection)
		var featureCollection2 geojson.FeatureCollection
		json.Unmarshal(aa.NewGeojson, &featureCollection2)
		pgmvt.DelMVT(DB, aa.TableName, featureCollection.Features[0].Geometry)
		for _, feature := range featureCollection2.Features {
			var id int32
			switch v := feature.Properties["id"].(type) {
			case float64:
				id = int32(v)
			case int:
				id = int32(v)
			case int32:
				id = v
			default:
				log.Printf("unexpected type for id: %T", v)
				return
			}
			DB.Table(aa.TableName).Where("id = ?", id).Delete(nil)
		}
		methods.SavaGeojsonToTable(DB, featureCollection, aa.TableName)
	case "要素打散":
		var featureCollection geojson.FeatureCollection
		json.Unmarshal(aa.OldGeojson, &featureCollection)
		var featureCollection2 geojson.FeatureCollection
		json.Unmarshal(aa.NewGeojson, &featureCollection2)
		pgmvt.DelMVT(DB, aa.TableName, featureCollection.Features[0].Geometry)
		for _, feature := range featureCollection2.Features {
			var id int32
			switch v := feature.Properties["id"].(type) {
			case float64:
				id = int32(v)
			case int:
				id = int32(v)
			case int32:
				id = v
			default:
				log.Printf("unexpected type for id: %T", v)
				return
			}
			DB.Table(aa.TableName).Where("id = ?", id).Delete(nil)
		}
		methods.SavaGeojsonToTable(DB, featureCollection, aa.TableName)
	case "要素环岛构造":
		var featureCollection geojson.FeatureCollection
		json.Unmarshal(aa.OldGeojson, &featureCollection)
		var featureCollection2 geojson.FeatureCollection
		json.Unmarshal(aa.NewGeojson, &featureCollection2)
		methods.UpdateGeojsonToTable(DB, featureCollection, aa.TableName, aa.GeoID)
		pgmvt.DelMVT(DB, aa.TableName, featureCollection2.Features[0].Geometry)
		pgmvt.DelMVT(DB, aa.TableName, featureCollection.Features[0].Geometry)
	case "要素聚合":
		var featureCollection geojson.FeatureCollection
		json.Unmarshal(aa.OldGeojson, &featureCollection)
		var featureCollection2 geojson.FeatureCollection
		json.Unmarshal(aa.NewGeojson, &featureCollection2)
		pgmvt.DelMVT(DB, aa.TableName, featureCollection.Features[0].Geometry)
		for _, feature := range featureCollection2.Features {
			var id int32
			switch v := feature.Properties["id"].(type) {
			case float64:
				id = int32(v)
			case int:
				id = int32(v)
			case int32:
				id = v
			default:
				log.Printf("unexpected type for id: %T", v)
				return
			}
			DB.Table(aa.TableName).Where("id = ?", id).Delete(nil)
		}
		methods.SavaGeojsonToTable(DB, featureCollection, aa.TableName)
	case "要素平移":
		var featureCollection geojson.FeatureCollection
		json.Unmarshal(aa.OldGeojson, &featureCollection)
		var featureCollection2 geojson.FeatureCollection
		json.Unmarshal(aa.NewGeojson, &featureCollection2)
		for _, feature := range featureCollection2.Features {
			pgmvt.DelMVT(DB, aa.TableName, feature.Geometry)
		}
		for _, feature := range featureCollection.Features {
			pgmvt.DelMVT(DB, aa.TableName, feature.Geometry)
		}
		for _, feature := range featureCollection2.Features {
			var id int32
			switch v := feature.Properties["id"].(type) {
			case float64:
				id = int32(v)
			case int:
				id = int32(v)
			case int32:
				id = v
			default:
				log.Printf("unexpected type for id: %T", v)
				return
			}
			DB.Table(aa.TableName).Where("id = ?", id).Delete(nil)
		}
		methods.SavaGeojsonToTable(DB, featureCollection, aa.TableName)
	case "面要素去重叠":
		var featureCollection geojson.FeatureCollection
		json.Unmarshal(aa.OldGeojson, &featureCollection)
		var featureCollection2 geojson.FeatureCollection
		json.Unmarshal(aa.NewGeojson, &featureCollection2)
		for _, feature := range featureCollection.Features {
			pgmvt.DelMVT(DB, aa.TableName, feature.Geometry)
		}
		for _, feature := range featureCollection2.Features {
			var id int32
			switch v := feature.Properties["id"].(type) {
			case float64:
				id = int32(v)
			case int:
				id = int32(v)
			case int32:
				id = v
			default:
				log.Printf("unexpected type for id: %T", v)
				return
			}
			DB.Table(aa.TableName).Where("id = ?", id).Delete(nil)
		}
		methods.SavaGeojsonToTable(DB, featureCollection, aa.TableName)
	}

	DB.Delete(&aa)
	c.JSON(http.StatusOK, "ok")
}

// 图层要素分割
type SplitData struct {
	Line      geojson.FeatureCollection `json:"Line"`
	LayerName string                    `json:"LayerName"`
	ID        int32                     `json:"ID"`
}

func (uc *UserController) SplitFeature(c *gin.Context) {
	var jsonData SplitData
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    500,
			"message": err.Error(),
			"data":    "",
		})
		return
	}

	DB := models.DB
	line := Transformer.GetGeometryString(jsonData.Line.Features[0])
	LayerName := jsonData.LayerName

	// 查询schema信息
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

	// 验证是否为面数据
	if schema.Type != "polygon" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    500,
			"message": "只有面数据才能分割",
			"data":    "",
		})
		return
	}

	// ========== 获取文件类型以确定映射字段 ==========
	fileExt := GetFileExt(LayerName)
	var mappingField string // 映射字段名称
	switch fileExt {
	case ".shp":
		mappingField = "objectid"
	case ".gdb":
		mappingField = "fid"
	default:
		mappingField = "" // 空字符串表示没有映射字段
	}

	// 开启事务
	tx := DB.Begin()
	if tx.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "开启事务失败: " + tx.Error.Error(),
			"data":    "",
		})
		return
	}

	// ========== 新增：使用 advisory lock 防止并发冲突 ==========
	lockSQL := fmt.Sprintf(`SELECT pg_advisory_xact_lock(hashtext('%s_id_lock'))`, LayerName)
	if err := tx.Exec(lockSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "获取锁失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 1. 先获取原要素的所有属性（除了id和geom）
	var originalFeature map[string]interface{}
	getOriginalSQL := fmt.Sprintf(`SELECT * FROM "%s" WHERE id = %d`, LayerName, jsonData.ID)
	if err := tx.Raw(getOriginalSQL).Scan(&originalFeature).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "获取原要素失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	if len(originalFeature) == 0 {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    404,
			"message": "未找到指定ID的几何数据",
			"data":    "",
		})
		return
	}

	// 2. 获取所有附加列（排除id和geom）
	var additionalColumns []string
	for key := range originalFeature {
		if key != "id" && key != "geom" {
			additionalColumns = append(additionalColumns, key)
		}
	}

	// ========== 查询 id 的最大值（三种情况都需要） ==========
	var maxID int32
	getMaxIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX(id), 0) as max_id FROM "%s"`, LayerName)
	if err := tx.Raw(getMaxIDSQL).Scan(&maxID).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "查询最大ID失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// ========== 查询映射字段的最大值（如果存在） ==========
	var maxMappingID int32
	if mappingField != "" {
		getMaxMappingIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX("%s"), 0) as max_id FROM "%s"`, mappingField, LayerName)
		if err := tx.Raw(getMaxMappingIDSQL).Scan(&maxMappingID).Error; err != nil {
			// 映射字段可能不存在，忽略错误
			maxMappingID = 0
		}
	}

	// 构建 split_geom CTE 的选择列
	splitSelectCols := `(ST_Dump(ST_Split(o.geom, ST_GeomFromGeoJSON('%s')))).geom AS geom`
	for _, col := range additionalColumns {
		splitSelectCols += fmt.Sprintf(`, o."%s"`, col)
	}

	// 构建 INSERT 的列名（包括id）
	insertCols := "id, geom"
	for _, col := range additionalColumns {
		insertCols += fmt.Sprintf(`, "%s"`, col)
	}

	// 构建 SELECT 的列名（包括id的自增逻辑和映射字段的自增逻辑）
	selectCols := fmt.Sprintf(`%d + ROW_NUMBER() OVER () AS id, geom`, maxID)
	for _, col := range additionalColumns {
		if mappingField != "" && col == mappingField {
			// 如果是映射字段，使用 ROW_NUMBER() 生成递增的值
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
			FROM original o
		)
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
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "切割并插入新要素失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 检查是否有插入的记录
	if len(insertedIDs) == 0 {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "切割失败，未生成新要素",
			"data":    "",
		})
		return
	}

	// 更新缓存库
	GetPdata := getData{
		TableName: LayerName,
		ID:        jsonData.ID,
	}

	geom := GetGeo(GetPdata)
	pgmvt.DelMVT(DB, jsonData.LayerName, geom.Features[0].Geometry)

	// 3. 删除原要素
	deleteSQL := fmt.Sprintf(`DELETE FROM "%s" WHERE id = %d`, LayerName, jsonData.ID)
	if err := tx.Exec(deleteSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "删除原要素失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 4. 查询新插入的所有几何，生成GeoJSON
	var idList []int32
	for _, id := range insertedIDs {
		idList = append(idList, id.ID)
	}
	getdata2 := getDatas{
		TableName: LayerName,
		ID:        idList,
	}

	// 5. 提交事务
	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "提交事务失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	splitGeojson := GetGeos(getdata2)
	delObjJSON := DelIDGen(geom)
	OldGeojson, _ := json.Marshal(geom)
	NewGeojson, _ := json.Marshal(splitGeojson)
	RecordResult := models.GeoRecord{
		TableName:    jsonData.LayerName,
		GeoID:        jsonData.ID,
		Type:         "要素分割",
		Date:         time.Now().Format("2006-01-02 15:04:05"),
		OldGeojson:   OldGeojson,
		NewGeojson:   NewGeojson,
		DelObjectIDs: delObjJSON,
	}

	DB.Create(&RecordResult)

	// 返回成功结果
	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "切割成功，已生成多个新要素",
		"data":    splitGeojson,
	})
}

// 图层要素合并
type DissolveData struct {
	LayerName string  `json:"LayerName"`
	IDs       []int32 `json:"ids"`
	MainID    int32   `json:"mainId"`
}

func (uc *UserController) DissolveFeature(c *gin.Context) {
	var jsonData DissolveData
	// 绑定JSON请求体到结构体,并检查绑定是否成功
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    500,
			"message": err.Error(),
			"data":    "",
		})
		return
	}

	// 验证参数
	if len(jsonData.IDs) < 2 {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "至少需要选择2个要素进行合并",
			"data":    "",
		})
		return
	}

	// 验证 MainID 是否在 IDs 列表中
	mainIDExists := false
	for _, id := range jsonData.IDs {
		if id == jsonData.MainID {
			mainIDExists = true
			break
		}
	}
	if !mainIDExists {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "MainID必须在选中的要素列表中",
			"data":    "",
		})
		return
	}

	DB := models.DB
	LayerName := jsonData.LayerName

	// 查询schema信息
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

	// 验证是否为面数据
	if schema.Type != "polygon" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    500,
			"message": "只有面数据才能合并",
			"data":    "",
		})
		return
	}

	// 判断数据源类型，确定映射字段
	fileExt := GetFileExt(LayerName)
	var mappingField string
	switch fileExt {
	case ".shp":
		mappingField = "objectid"
	case ".gdb":
		mappingField = "fid"
	default:
		mappingField = "" // 空字符串表示不需要额外的映射字段
	}

	// 开启事务
	tx := DB.Begin()
	if tx.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "开启事务失败: " + tx.Error.Error(),
			"data":    "",
		})
		return
	}

	// 构建ID列表字符串
	idList := make([]string, len(jsonData.IDs))
	for i, id := range jsonData.IDs {
		idList[i] = fmt.Sprintf("%d", id)
	}
	idsStr := strings.Join(idList, ",")

	// 1. 验证所有ID是否存在
	var count int64
	checkSQL := fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE id IN (%s)`, LayerName, idsStr)
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

	// 2. 获取 MainID 对应要素的所有属性（作为合并后要素的属性）
	var originalFeature map[string]interface{}
	getOriginalSQL := fmt.Sprintf(`SELECT * FROM "%s" WHERE id = %d`, LayerName, jsonData.MainID)
	if err := tx.Raw(getOriginalSQL).Scan(&originalFeature).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "获取主要素属性失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 验证 MainID 对应的要素是否存在
	if len(originalFeature) == 0 {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    404,
			"message": fmt.Sprintf("MainID(%d)对应的要素不存在", jsonData.MainID),
			"data":    "",
		})
		return
	}

	// 3. 获取当前表的最大ID，用于生成新ID（所有情况都需要）
	var maxID int32
	getMaxIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX(id), 0) FROM "%s"`, LayerName)
	if err := tx.Raw(getMaxIDSQL).Scan(&maxID).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "获取最大ID失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 查询映射字段的最大值（如果需要）
	var maxMappingID int32
	if mappingField != "" {
		getMaxMappingIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX("%s"), 0) FROM "%s"`, mappingField, LayerName)
		if err := tx.Raw(getMaxMappingIDSQL).Scan(&maxMappingID).Error; err != nil {
			// 如果字段不存在，忽略错误
			maxMappingID = 0
		}
	}

	// 4. 构建属性字段列表（排除id和geom）
	var columnNames []string
	var columnValues []string
	for key, value := range originalFeature {
		if key != "id" && key != "geom" {
			columnNames = append(columnNames, fmt.Sprintf(`"%s"`, key))

			// 判断是否为映射字段，如果是则使用自增值
			if mappingField != "" && key == mappingField {
				columnValues = append(columnValues, fmt.Sprintf("%d", maxMappingID+1))
				continue
			}

			// 根据值类型构建SQL值
			switch v := value.(type) {
			case nil:
				columnValues = append(columnValues, "NULL")
			case string:
				// 转义单引号
				escapedValue := strings.ReplaceAll(v, "'", "''")
				columnValues = append(columnValues, fmt.Sprintf("'%s'", escapedValue))
			case time.Time:
				// 处理时间类型
				if v.IsZero() {
					columnValues = append(columnValues, "NULL")
				} else {
					// 对于date类型，只需要日期部分
					// 如果是timestamp类型，使用: v.Format("2006-01-02 15:04:05")
					columnValues = append(columnValues, fmt.Sprintf("'%s'", v.Format("2006-01-02")))
				}
			case int, int32, int64, float32, float64:
				columnValues = append(columnValues, fmt.Sprintf("%v", v))
			case bool:
				columnValues = append(columnValues, fmt.Sprintf("%t", v))
			default:
				// 检查是否是时间指针类型
				if t, ok := value.(*time.Time); ok {
					if t == nil || t.IsZero() {
						columnValues = append(columnValues, "NULL")
					} else {
						columnValues = append(columnValues, fmt.Sprintf("'%s'", t.Format("2006-01-02")))
					}
				} else {
					// 其他类型尝试转换为字符串
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

	// 5. 执行合并并插入新要素
	dissolveAndInsertSQL := fmt.Sprintf(`
		WITH dissolved AS (
			-- 合并所有选中的几何
			SELECT 
				ST_Multi(ST_Union(ST_SnapToGrid(geom, 0.0000001))) AS merged_geom
			FROM "%s"
			WHERE id IN (%s)
		)
		-- 插入合并后的新要素
		INSERT INTO "%s" (id, geom%s)
		SELECT 
			%d AS id,
			merged_geom
			%s
		FROM dissolved
		RETURNING ST_AsGeoJSON(geom) AS geojson, id
	`, LayerName, idsStr, LayerName, columnsStr, maxID+1, valuesStr)

	type DissolveResult struct {
		Geojson string
		ID      int32
	}
	var dissolveResult DissolveResult

	if err := tx.Raw(dissolveAndInsertSQL).Scan(&dissolveResult).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "合并并插入新要素失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 检查是否成功插入
	if dissolveResult.Geojson == "" {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "合并失败，未生成有效几何",
			"data":    "",
		})
		return
	}

	// 删除MVT缓存
	getdata2 := getDatas{
		TableName: LayerName,
		ID:        jsonData.IDs,
	}
	oldGeo := GetGeos(getdata2)
	oldGeojson, _ := json.Marshal(oldGeo)

	for _, feature := range oldGeo.Features {
		pgmvt.DelMVT(DB, jsonData.LayerName, feature.Geometry)
	}

	delObjJSON := DelIDGen(oldGeo)

	// 6. 删除原要素
	deleteSQL := fmt.Sprintf(`DELETE FROM "%s" WHERE id IN (%s)`, LayerName, idsStr)
	if err := tx.Exec(deleteSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "删除原要素失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 7. 提交事务
	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "提交事务失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	GetPdata := getData{
		TableName: LayerName,
		ID:        dissolveResult.ID,
	}
	newGeo := GetGeo(GetPdata)
	newGeoJson, _ := json.Marshal(newGeo)
	RecordResult := models.GeoRecord{
		TableName:    jsonData.LayerName,
		Type:         "要素合并",
		Date:         time.Now().Format("2006-01-02 15:04:05"),
		OldGeojson:   oldGeojson,
		NewGeojson:   newGeoJson,
		DelObjectIDs: delObjJSON,
	}

	DB.Create(&RecordResult)

	// 返回成功结果
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
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    500,
			"message": err.Error(),
			"data":    "",
		})
		return
	}

	// 验证输入
	if len(jsonData.IDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "IDs 不能为空",
			"data":    "",
		})
		return
	}

	DB := models.DB
	LayerName := jsonData.LayerName

	// 查询schema信息
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

	// 判断数据源类型，确定映射字段
	fileExt := GetFileExt(LayerName)
	var mappingField string
	switch fileExt {
	case ".shp":
		mappingField = "objectid"
	case ".gdb":
		mappingField = "fid"
	default:
		mappingField = "" // 空字符串表示不需要额外的映射字段
	}

	// 开启事务
	tx := DB.Begin()
	if tx.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "开启事务失败: " + tx.Error.Error(),
			"data":    "",
		})
		return
	}

	// 使用 advisory lock 防止并发冲突
	lockSQL := fmt.Sprintf(`SELECT pg_advisory_xact_lock(hashtext('%s_id_lock'))`, LayerName)
	if err := tx.Exec(lockSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "获取锁失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 构建 ID 列表的 SQL 占位符
	idPlaceholders := make([]string, len(jsonData.IDs))
	idInterfaces := make([]interface{}, len(jsonData.IDs))
	for i, id := range jsonData.IDs {
		idPlaceholders[i] = "?"
		idInterfaces[i] = id
	}
	idCondition := fmt.Sprintf("id IN (%s)", strings.Join(idPlaceholders, ","))

	// 1. 检查所有几何体是否为Multi类型（可打散）
	type GeomInfo struct {
		ID       int32
		GeomType string
	}
	var geomInfos []GeomInfo
	checkTypeSQL := fmt.Sprintf(`SELECT id, ST_GeometryType(geom) FROM "%s" WHERE %s`, LayerName, idCondition)
	if err := tx.Raw(checkTypeSQL, idInterfaces...).Scan(&geomInfos).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "获取几何类型失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 验证返回的几何体数量
	if len(geomInfos) != len(jsonData.IDs) {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    404,
			"message": fmt.Sprintf("部分ID未找到对应的几何数据，期望 %d 个，实际找到 %d 个", len(jsonData.IDs), len(geomInfos)),
			"data":    "",
		})
		return
	}

	// 2. 检查每个聚合体包含多少个几何体
	type GeomCount struct {
		ID    int32
		Count int
	}
	var geomCounts []GeomCount
	countGeomsSQL := fmt.Sprintf(`SELECT id, ST_NumGeometries(geom) as count FROM "%s" WHERE %s`, LayerName, idCondition)
	if err := tx.Raw(countGeomsSQL, idInterfaces...).Scan(&geomCounts).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "获取几何数量失败: " + err.Error(),
			"data":    "",
		})
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
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": fmt.Sprintf("以下ID的聚合体只包含一个几何体，无需打散: %v", singleGeoms),
			"data":    "",
		})
		return
	}

	// 3. 获取原要素的所有属性（除了id和geom）
	var originalFeatures []map[string]interface{}
	getOriginalSQL := fmt.Sprintf(`SELECT * FROM "%s" WHERE %s`, LayerName, idCondition)
	if err := tx.Raw(getOriginalSQL, idInterfaces...).Scan(&originalFeatures).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "获取原要素失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 4. 获取所有附加列（排除id和geom）
	var additionalColumns []string
	if len(originalFeatures) > 0 {
		for key := range originalFeatures[0] {
			if key != "id" && key != "geom" {
				additionalColumns = append(additionalColumns, key)
			}
		}
	}

	// 5. 查询 id 的最大值（所有情况都需要）
	var maxID int32
	getMaxIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX(id), 0) as max_id FROM "%s"`, LayerName)
	if err := tx.Raw(getMaxIDSQL).Scan(&maxID).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "查询最大ID失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 查询映射字段的最大值（如果需要）
	var maxMappingID int32
	if mappingField != "" {
		getMaxMappingIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX("%s"), 0) as max_id FROM "%s"`, mappingField, LayerName)
		if err := tx.Raw(getMaxMappingIDSQL).Scan(&maxMappingID).Error; err != nil {
			// 如果字段不存在，忽略错误
			maxMappingID = 0
		}
	}

	// 6. 构建 explode CTE 的选择列
	explodeSelectCols := `(ST_Dump(o.geom)).geom AS geom`
	for _, col := range additionalColumns {
		explodeSelectCols += fmt.Sprintf(`, o."%s"`, col)
	}

	// 构建 INSERT 的列名（包括id和映射字段）
	insertCols := "id, geom"
	for _, col := range additionalColumns {
		insertCols += fmt.Sprintf(`, "%s"`, col)
	}

	// 构建 SELECT 的列名（包括id的自增逻辑和映射字段的自增逻辑）
	selectCols := fmt.Sprintf(`%d + ROW_NUMBER() OVER () AS id, geom`, maxID)
	for _, col := range additionalColumns {
		if mappingField != "" && col == mappingField {
			// 使用 ROW_NUMBER() 生成递增的映射字段值
			selectCols += fmt.Sprintf(`, %d + ROW_NUMBER() OVER () AS "%s"`, maxMappingID, col)
		} else {
			selectCols += fmt.Sprintf(`, "%s"`, col)
		}
	}

	// 7. 执行打散并插入新要素
	explodeAndInsertSQL := fmt.Sprintf(`
		WITH original AS (
			SELECT * FROM "%s" WHERE %s
		),
		exploded_geom AS (
			SELECT %s
			FROM original o
		)
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
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "打散并插入新要素失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 检查是否有插入的记录
	if len(insertedIDs) == 0 {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "打散失败，未生成新要素",
			"data":    "",
		})
		return
	}

	// 9. 删除原要素
	deleteSQL := fmt.Sprintf(`DELETE FROM "%s" WHERE %s`, LayerName, idCondition)
	if err := tx.Exec(deleteSQL, idInterfaces...).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "删除原要素失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 10. 查询新插入的所有几何，生成GeoJSON
	var idList []int32
	for _, id := range insertedIDs {
		idList = append(idList, id.ID)
	}
	getdata2 := getDatas{
		TableName: LayerName,
		ID:        idList,
	}

	// 11. 提交事务
	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "提交事务失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 12. 记录操作日志
	explodedGeojson := GetGeos(getdata2)

	// 构建原要素的 GeoJSON（用于记录）
	allOriginalGeoms := geojson.FeatureCollection{}

	// 获取所有原要素的 GeoJSON
	for _, id := range jsonData.IDs {
		GetPdata := getData{
			TableName: LayerName,
			ID:        id,
		}
		geom := GetGeo(GetPdata)
		if len(geom.Features) > 0 {
			allOriginalGeoms.Features = append(allOriginalGeoms.Features, geom.Features[0])
		}
	}

	OldGeojson, _ := json.Marshal(allOriginalGeoms)
	NewGeojson, _ := json.Marshal(explodedGeojson)

	// 生成删除的对象IDs（可选，根据你的需求）
	delObjJSON := DelIDGen(allOriginalGeoms)

	RecordResult := models.GeoRecord{
		TableName:    jsonData.LayerName,
		GeoID:        jsonData.IDs[0], // 记录第一个ID（可选）
		Type:         "要素批量打散",
		Date:         time.Now().Format("2006-01-02 15:04:05"),
		OldGeojson:   OldGeojson,
		NewGeojson:   NewGeojson,
		DelObjectIDs: delObjJSON,
	}

	DB.Create(&RecordResult)

	// 返回成功结果
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
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    500,
			"message": err.Error(),
			"data":    "",
		})
		return
	}

	DB := models.DB
	donut := Transformer.GetGeometryString(jsonData.Donut.Features[0])
	LayerName := jsonData.LayerName

	// 查询schema信息
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

	// 验证是否为面数据
	if schema.Type != "polygon" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    500,
			"message": "只有面数据才能构造环岛",
			"data":    "",
		})
		return
	}

	// 获取文件类型，确定映射字段
	fileExt := GetFileExt(LayerName)
	var mappingField string
	switch fileExt {
	case ".shp":
		mappingField = "objectid"
	case ".gdb":
		mappingField = "fid"
	default:
		mappingField = "" // 无映射字段
	}

	// 开启事务
	tx := DB.Begin()
	if tx.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "开启事务失败: " + tx.Error.Error(),
			"data":    "",
		})
		return
	}

	// ========== 使用 advisory lock 防止并发冲突 ==========
	lockSQL := fmt.Sprintf(`SELECT pg_advisory_xact_lock(hashtext('%s_donut_lock'))`, LayerName)
	if err := tx.Exec(lockSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "获取锁失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 1. 获取原要素的所有属性（除了id和geom）
	var originalFeature map[string]interface{}
	getOriginalSQL := fmt.Sprintf(`SELECT * FROM "%s" WHERE id = %d`, LayerName, jsonData.ID)
	if err := tx.Raw(getOriginalSQL).Scan(&originalFeature).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "获取原要素失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	if len(originalFeature) == 0 {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    404,
			"message": "未找到指定ID的几何数据",
			"data":    "",
		})
		return
	}

	// 2. 获取所有附加列（排除id和geom）
	var additionalColumns []string
	for key := range originalFeature {
		if key != "id" && key != "geom" {
			additionalColumns = append(additionalColumns, key)
		}
	}

	// ========== 查询 id 和映射字段的最大值 ==========
	var maxID int32
	getMaxIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX(id), 0) as max_id FROM "%s"`, LayerName)
	if err := tx.Raw(getMaxIDSQL).Scan(&maxID).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "查询最大ID失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	var maxMappingID int32
	if mappingField != "" {
		// 查询映射字段的最大值
		getMaxMappingIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX("%s"), 0) as max_id FROM "%s"`, mappingField, LayerName)
		if err := tx.Raw(getMaxMappingIDSQL).Scan(&maxMappingID).Error; err != nil {
			// 映射字段可能不存在，忽略错误
			maxMappingID = 0
		}
	}

	// ========== 构造环岛（使用 ST_Difference）==========
	// 环岛 = 原多边形 - 内部多边形
	// 需要验证内部多边形确实在原多边形内部
	validateDonutSQL := fmt.Sprintf(`
		SELECT ST_Contains(
			(SELECT geom FROM "%s" WHERE id = %d),
			ST_GeomFromGeoJSON('%s')
		)
	`, LayerName, jsonData.ID, donut)

	var isContained bool
	if err := tx.Raw(validateDonutSQL).Scan(&isContained).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "验证环岛位置失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	if !isContained {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "环岛多边形必须完全位于原多边形内部",
			"data":    "",
		})
		return
	}

	// 构建插入列名
	insertCols := "id, geom"
	for _, col := range additionalColumns {
		insertCols += fmt.Sprintf(`, "%s"`, col)
	}

	// 构建SELECT的列名
	selectCols := fmt.Sprintf(`%d + 1 AS id, geom`, maxID)
	for _, col := range additionalColumns {
		if col == mappingField && mappingField != "" {
			// 如果是映射字段，使用自增后的值
			selectCols += fmt.Sprintf(`, %d + 1 AS "%s"`, maxMappingID, col)
		} else {
			// 其他字段直接复制
			selectCols += fmt.Sprintf(`, "%s"`, col)
		}
	}

	// 构建附加列的引用（用于WITH子句）
	additionalColsRef := ""
	if len(additionalColumns) > 0 {
		for _, col := range additionalColumns {
			additionalColsRef += fmt.Sprintf(`, o."%s"`, col)
		}
	}

	// 构造环岛的SQL：使用ST_Difference创建带孔的多边形
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
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "构造环岛失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 检查是否有插入的记录
	if len(insertedIDs) == 0 {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "环岛构造失败，未生成新要素",
			"data":    "",
		})
		return
	}

	// 更新缓存库
	GetPdata := getData{
		TableName: LayerName,
		ID:        jsonData.ID,
	}

	geom := GetGeo(GetPdata)
	pgmvt.DelMVT(DB, jsonData.LayerName, geom.Features[0].Geometry)

	// 删除原要素
	deleteSQL := fmt.Sprintf(`DELETE FROM "%s" WHERE id = %d`, LayerName, jsonData.ID)
	if err := tx.Exec(deleteSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "删除原要素失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 查询新插入的所有几何，生成GeoJSON
	var idList []int32
	for _, id := range insertedIDs {
		idList = append(idList, id.ID)
	}
	getdata2 := getDatas{
		TableName: LayerName,
		ID:        idList,
	}

	// 提交事务
	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "提交事务失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	donutGeojson := GetGeos(getdata2)
	delObjJSON := DelIDGen(geom)
	OldGeojson, _ := json.Marshal(geom)
	NewGeojson, _ := json.Marshal(donutGeojson)
	RecordResult := models.GeoRecord{
		TableName:    jsonData.LayerName,
		GeoID:        jsonData.ID,
		Type:         "要素环岛构造",
		Date:         time.Now().Format("2006-01-02 15:04:05"),
		OldGeojson:   OldGeojson,
		NewGeojson:   NewGeojson,
		DelObjectIDs: delObjJSON,
	}

	DB.Create(&RecordResult)

	// 返回成功结果
	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "环岛构造成功",
		"data":    donutGeojson,
	})
}

// 要素聚合
type AggregatorData struct {
	LayerName string  `json:"LayerName"`
	IDs       []int32 `json:"ids"`
	MainID    int32   `json:"mainId"`
}

func (uc *UserController) AggregatorFeature(c *gin.Context) {
	var jsonData AggregatorData
	// 绑定JSON请求体到结构体，并检查绑定是否成功
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    500,
			"message": err.Error(),
			"data":    "",
		})
		return
	}

	// 验证参数
	if len(jsonData.IDs) < 2 {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "至少需要选择2个要素进行聚合",
			"data":    "",
		})
		return
	}

	// 验证 MainID 是否在 IDs 列表中
	mainIDExists := false
	for _, id := range jsonData.IDs {
		if id == jsonData.MainID {
			mainIDExists = true
			break
		}
	}
	if !mainIDExists {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "MainID必须在选中的要素列表中",
			"data":    "",
		})
		return
	}

	DB := models.DB
	LayerName := jsonData.LayerName

	// 查询schema信息
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

	// 获取文件扩展名，判断数据源类型
	fileExt := GetFileExt(LayerName)
	var mappingField string
	switch fileExt {
	case ".shp":
		mappingField = "objectid"
	case ".gdb":
		mappingField = "fid"
	default:
		mappingField = "" // 空字符串表示不需要映射字段
	}

	// 开启事务
	tx := DB.Begin()
	if tx.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "开启事务失败: " + tx.Error.Error(),
			"data":    "",
		})
		return
	}

	// 构建ID列表字符串
	idList := make([]string, len(jsonData.IDs))
	for i, id := range jsonData.IDs {
		idList[i] = fmt.Sprintf("%d", id)
	}
	idsStr := strings.Join(idList, ",")

	// 1. 验证所有ID是否存在
	var count int64
	checkSQL := fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE id IN (%s)`, LayerName, idsStr)
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

	// 2. 获取 MainID 对应要素的所有属性（作为聚合后要素的属性）
	var originalFeature map[string]interface{}
	getOriginalSQL := fmt.Sprintf(`SELECT * FROM "%s" WHERE id = %d`, LayerName, jsonData.MainID)
	if err := tx.Raw(getOriginalSQL).Scan(&originalFeature).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "获取主要素属性失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 验证 MainID 对应的要素是否存在
	if len(originalFeature) == 0 {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    404,
			"message": fmt.Sprintf("MainID(%d)对应的要素不存在", jsonData.MainID),
			"data":    "",
		})
		return
	}

	// 3. 获取当前表的最大ID，用于生成新ID
	var maxID int32
	getMaxIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX(id), 0) FROM "%s"`, LayerName)
	if err := tx.Raw(getMaxIDSQL).Scan(&maxID).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "获取最大ID失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 查询映射字段的最大值（如果有映射字段）
	var maxMappingID int32
	if mappingField != "" {
		getMaxMappingIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX(%s), 0) FROM "%s"`, mappingField, LayerName)
		if err := tx.Raw(getMaxMappingIDSQL).Scan(&maxMappingID).Error; err != nil {
			// 如果查询失败，继续执行（某些表可能没有该字段）
			maxMappingID = 0
		}
	}

	// 4. 构建属性字段列表（排除id和geom）
	var columnNames []string
	var columnValues []string
	for key, value := range originalFeature {
		if key != "id" && key != "geom" {
			columnNames = append(columnNames, fmt.Sprintf(`"%s"`, key))

			// 如果是映射字段，使用自增值
			if mappingField != "" && key == mappingField {
				columnValues = append(columnValues, fmt.Sprintf("%d", maxMappingID+1))
				continue
			}

			// 根据值类型构建SQL值
			switch v := value.(type) {
			case nil:
				columnValues = append(columnValues, "NULL")
			case string:
				// 转义单引号
				escapedValue := strings.ReplaceAll(v, "'", "''")
				columnValues = append(columnValues, fmt.Sprintf("'%s'", escapedValue))
			case time.Time:
				// 处理时间类型
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
				// 检查是否是时间指针类型
				if t, ok := value.(*time.Time); ok {
					if t == nil || t.IsZero() {
						columnValues = append(columnValues, "NULL")
					} else {
						columnValues = append(columnValues, fmt.Sprintf("'%s'", t.Format("2006-01-02")))
					}
				} else {
					// 其他类型尝试转换为字符串
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

	// 5. 执行聚合并插入新要素
	// 聚合方式：使用ST_Collect收集所有几何并打散Multi类型，然后重新组合成Multi类型
	var aggregateSQL string
	if schema.Type == "point" {
		// 点数据聚合：打散Multi类型后重新组合成MultiPoint
		aggregateSQL = fmt.Sprintf(`
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
		`, LayerName, idsStr, LayerName, columnsStr, maxID+1, valuesStr)
	} else if schema.Type == "line" {
		// 线数据聚合：打散Multi类型后重新组合成MultiLineString
		aggregateSQL = fmt.Sprintf(`
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
		`, LayerName, idsStr, LayerName, columnsStr, maxID+1, valuesStr)
	} else if schema.Type == "polygon" {
		// 面数据聚合：打散Multi类型后重新组合成MultiPolygon
		aggregateSQL = fmt.Sprintf(`
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
		`, LayerName, idsStr, LayerName, columnsStr, maxID+1, valuesStr)
	} else {
		// 默认方式：打散后使用ST_Collect
		aggregateSQL = fmt.Sprintf(`
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
		`, LayerName, idsStr, LayerName, columnsStr, maxID+1, valuesStr)
	}

	type AggregatorResult struct {
		Geojson string
		ID      int32
	}
	var aggregatorResult AggregatorResult

	if err := tx.Raw(aggregateSQL).Scan(&aggregatorResult).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "聚合并插入新要素失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 检查是否成功插入
	if aggregatorResult.Geojson == "" {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "聚合失败，未生成有效几何",
			"data":    "",
		})
		return
	}

	// 删除MVT缓存
	getdata2 := getDatas{
		TableName: LayerName,
		ID:        jsonData.IDs,
	}
	oldGeo := GetGeos(getdata2)
	oldGeojson, _ := json.Marshal(oldGeo)

	delObjJSON := DelIDGen(oldGeo)

	// 6. 删除原要素
	deleteSQL := fmt.Sprintf(`DELETE FROM "%s" WHERE id IN (%s)`, LayerName, idsStr)
	if err := tx.Exec(deleteSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "删除原要素失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 7. 提交事务
	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "提交事务失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	GetPdata := getData{
		TableName: LayerName,
		ID:        aggregatorResult.ID,
	}
	newGeo := GetGeo(GetPdata)
	newGeoJson, _ := json.Marshal(newGeo)
	RecordResult := models.GeoRecord{
		TableName:    jsonData.LayerName,
		Type:         "要素聚合",
		Date:         time.Now().Format("2006-01-02 15:04:05"),
		OldGeojson:   oldGeojson,
		NewGeojson:   newGeoJson,
		DelObjectIDs: delObjJSON,
	}

	DB.Create(&RecordResult)

	// 返回成功结果
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

	// 验证参数
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

	// 查询schema信息
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

	// ========== 新增：获取文件扩展名并确定映射字段 ==========
	fileExt := GetFileExt(LayerName)
	var idFieldName string

	switch fileExt {
	case ".shp":
		idFieldName = "objectid"
	case ".gdb":
		idFieldName = "fid"
	default:
		idFieldName = "id"
	}

	// 开启事务
	tx := DB.Begin()
	if tx.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "开启事务失败: " + tx.Error.Error(),
			"data":    "",
		})
		return
	}

	// 构建ID列表字符串
	idList := make([]string, len(jsonData.IDs))
	for i, id := range jsonData.IDs {
		idList[i] = fmt.Sprintf("%d", id)
	}
	idsStr := strings.Join(idList, ",")

	// 1. 验证所有ID是否存在
	var count int64
	checkSQL := fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE "%s" IN (%s)`, LayerName, idFieldName, idsStr)
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

	// 2. 获取原始要素的GeoJSON（用于记录）
	getdata2 := getDatas{
		TableName: LayerName,
		ID:        jsonData.IDs,
	}
	oldGeo := GetGeos(getdata2)
	oldGeojson, _ := json.Marshal(oldGeo)

	// ========== 关键改动：米转度数 ==========
	// 获取要素的中心纬度，用于计算经度偏移
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

	// 转换单位：米 -> 度数
	offsetLat := metersToDegreesLat(jsonData.YOffset, centerLat)
	offsetLon := metersToDegreeeLon(jsonData.XOffset, centerLat)

	// 3. 执行平移操作（使用转换后的度数）
	offsetSQL := fmt.Sprintf(`
		UPDATE "%s"
		SET geom = ST_Translate(geom, %f, %f)
		WHERE "%s" IN (%s)
		RETURNING "%s", ST_AsGeoJSON(geom) AS geojson
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

	// 检查是否成功更新
	if len(offsetResults) == 0 {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "平移失败，未更新任何要素",
			"data":    "",
		})
		return
	}

	// 4. 删除MVT缓存
	pgmvt.DelMVTALL(DB, jsonData.LayerName)

	// 5. 提交事务
	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "提交事务失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 6. 获取更新后的要素信息
	getdata3 := getDatas{
		TableName: LayerName,
		ID:        jsonData.IDs,
	}
	newGeo := GetGeos(getdata3)
	newGeojson, _ := json.Marshal(newGeo)

	delObjectIDs := DelIDGen(oldGeo)
	// 8. 记录操作
	RecordResult := models.GeoRecord{
		TableName:    jsonData.LayerName,
		Type:         "要素平移",
		Date:         time.Now().Format("2006-01-02 15:04:05"),
		OldGeojson:   oldGeojson,
		NewGeojson:   newGeojson,
		DelObjectIDs: delObjectIDs,
	}

	DB.Create(&RecordResult)

	// 返回成功结果
	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": fmt.Sprintf("成功平移%d个要素，X偏移: %f米, Y偏移: %f米", len(jsonData.IDs), jsonData.XOffset, jsonData.YOffset),
		"data": gin.H{
			"count":      len(offsetResults),
			"offset_x_m": jsonData.XOffset,
			"offset_y_m": jsonData.YOffset,
			"offset_lon": offsetLon,
			"offset_lat": offsetLat,
			"id_field":   idFieldName,
		},
	})
}

// 面要素去重叠
type AreaOnAreaData struct {
	LayerName string  `json:"LayerName"`
	IDs       []int32 `json:"ids"`
}

// 面要素去重叠
func (uc *UserController) AreaOnAreaAnalysis(c *gin.Context) {
	var jsonData AreaOnAreaData
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    500,
			"message": err.Error(),
			"data":    "",
		})
		return
	}

	if len(jsonData.IDs) < 2 {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "至少需要选择2个要素进行叠加分析",
			"data":    "",
		})
		return
	}

	DB := models.DB
	LayerName := jsonData.LayerName
	EXT := GetFileExt(LayerName)

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

	tx := DB.Begin()
	if tx.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "开启事务失败: " + tx.Error.Error(),
			"data":    "",
		})
		return
	}

	// ========== 使用 advisory lock 防止并发冲突 ==========
	lockSQL := fmt.Sprintf(`SELECT pg_advisory_xact_lock(hashtext('%s_id_lock'))`, LayerName)
	if err := tx.Exec(lockSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "获取锁失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	idList := make([]string, len(jsonData.IDs))
	for i, id := range jsonData.IDs {
		idList[i] = fmt.Sprintf("%d", id)
	}
	idsStr := strings.Join(idList, ",")

	// 1. 验证所有ID是否存在
	var count int64
	checkSQL := fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE id IN (%s)`, LayerName, idsStr)
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

	// 2. 检查几何类型是否都是面
	type GeometryTypeData struct {
		ID   int32
		Type string
	}
	var geomTypes []GeometryTypeData
	checkGeomSQL := fmt.Sprintf(`
		SELECT id, ST_GeometryType(geom) AS type FROM "%s" WHERE id IN (%s)
	`, LayerName, idsStr)
	if err := tx.Raw(checkGeomSQL).Scan(&geomTypes).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "获取几何类型失败: " + err.Error(),
			"data":    "",
		})
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
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": fmt.Sprintf("以下ID对应的要素不是面类型，无法进行叠加分析: %v", nonPolygons),
			"data":    "",
		})
		return
	}

	// ========== 3. 获取原要素的所有属性（用于继承） ==========
	var originalFeatures []map[string]interface{}
	getOriginalSQL := fmt.Sprintf(`SELECT * FROM "%s" WHERE id IN (%s)`, LayerName, idsStr)
	if err := tx.Raw(getOriginalSQL).Scan(&originalFeatures).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "获取原要素属性失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 获取所有附加列（排除id和geom）
	var additionalColumns []string
	if len(originalFeatures) > 0 {
		for key := range originalFeatures[0] {
			if key != "id" && key != "geom" {
				additionalColumns = append(additionalColumns, key)
			}
		}
	}

	// 使用第一个要素的属性作为模板（你也可以根据业务需求调整）
	var templateAttributes map[string]interface{}
	if len(originalFeatures) > 0 {
		templateAttributes = originalFeatures[0]
	}

	// 4. 获取原始要素的GeoJSON（用于记录）
	getdata := getDatas{
		TableName: LayerName,
		ID:        jsonData.IDs,
	}
	oldGeo := GetGeos(getdata)
	oldGeojson, _ := json.Marshal(oldGeo)

	// 删除MVT缓存（原要素）
	if len(oldGeo.Features) >= 10 {
		pgmvt.DelMVTALL(DB, jsonData.LayerName)
	} else {
		for _, feature := range oldGeo.Features {
			pgmvt.DelMVT(DB, jsonData.LayerName, feature.Geometry)
		}
	}

	// ========== 5. 获取当前表的最大ID和对应的映射字段最大值 ==========
	var maxID int32
	getMaxIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX(id), 0) FROM "%s"`, LayerName)
	if err := tx.Raw(getMaxIDSQL).Scan(&maxID).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "获取最大ID失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 根据文件扩展名确定映射字段
	var mappingField string
	var maxMappingValue int32

	if EXT == ".shp" {
		mappingField = "objectid"
	} else if EXT == ".gdb" {
		mappingField = "fid"
	}

	delObjectIDs := DelIDGen(oldGeo)

	// 如果有映射字段，获取其最大值
	if mappingField != "" {
		getMaxMappingSQL := fmt.Sprintf(`SELECT COALESCE(MAX("%s"), 0) FROM "%s"`, mappingField, LayerName)
		if tx.Raw(getMaxMappingSQL).Scan(&maxMappingValue).Error != nil {
			maxMappingValue = 0
		}
	}

	gdallayer, err := Gogeo.ConvertGeoJSONToGDALLayer(&oldGeo, LayerName)
	if err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "转换GeoJSON失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	analysisGDAL, err := Gogeo.AreaOnAreaAnalysis(gdallayer, 0.00000001)
	if err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "叠加分析失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 将GDAL结果转换为GeoJSON
	analysisGeoJSON, err := Gogeo.LayerToGeoJSON(analysisGDAL)
	if err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "转换分析结果失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	if len(analysisGeoJSON.Features) == 0 {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "叠加分析失败，未生成任何结果几何",
			"data":    "",
		})
		return
	}

	// ========== 7. 插入新要素（包含所有属性） ==========
	newFeatureIDs := make([]int32, 0)

	// 构建列名（包括id, geom和所有附加列）
	insertCols := "id, geom"
	for _, col := range additionalColumns {
		insertCols += fmt.Sprintf(`, "%s"`, col)
	}

	// 构建INSERT语句
	insertSQL := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES `, LayerName, insertCols)
	valueStrings := make([]string, 0)

	for i, feature := range analysisGeoJSON.Features {
		newID := maxID + int32(i) + 1
		newFeatureIDs = append(newFeatureIDs, newID)

		// 使用 geojson.NewGeometry 包装 orb.Geometry
		geojsonGeom := geojson.NewGeometry(feature.Geometry)
		geomBytes, err := json.Marshal(geojsonGeom)
		if err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{
				"code":    500,
				"message": fmt.Sprintf("序列化几何对象失败: %v", err),
				"data":    "",
			})
			return
		}

		geomStr := string(geomBytes)
		geomStr = strings.ReplaceAll(geomStr, "'", "''")

		// 构建值列表
		valueList := fmt.Sprintf(`%d, ST_GeomFromGeoJSON('%s')`, newID, geomStr)

		// 添加所有附加列的值
		for _, col := range additionalColumns {
			if mappingField != "" && col == mappingField {
				// 使用映射字段的自增值（objectid 或 fid）
				newMappingValue := maxMappingValue + int32(i) + 1
				valueList += fmt.Sprintf(`, %d`, newMappingValue)
			} else {
				// 其他属性从模板继承
				if templateAttributes != nil {
					if val, exists := templateAttributes[col]; exists && val != nil {
						// 根据值类型进行格式化
						switch v := val.(type) {
						case string:
							escapedVal := strings.ReplaceAll(v, "'", "''")
							valueList += fmt.Sprintf(`, '%s'`, escapedVal)
						case int, int32, int64, float32, float64:
							valueList += fmt.Sprintf(`, %v`, v)
						case bool:
							valueList += fmt.Sprintf(`, %t`, v)
						default:
							valueList += `, NULL`
						}
					} else {
						valueList += `, NULL`
					}
				} else {
					valueList += `, NULL`
				}
			}
		}

		valueStrings = append(valueStrings, fmt.Sprintf(`(%s)`, valueList))
	}

	if len(valueStrings) == 0 {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "无法生成有效的插入语句",
			"data":    "",
		})
		return
	}

	insertSQL += strings.Join(valueStrings, ",")
	if err := tx.Exec(insertSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "插入新要素失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 8. 删除原要素
	deleteSQL := fmt.Sprintf(`DELETE FROM "%s" WHERE id IN (%s)`, LayerName, idsStr)
	if err := tx.Exec(deleteSQL).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "删除原要素失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 9. 提交事务
	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "提交事务失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 10. 获取分析后的要素信息（用于记录）
	getdata2 := getDatas{
		TableName: LayerName,
		ID:        newFeatureIDs,
	}
	newGeo := GetGeos(getdata2)
	newGeojson, _ := json.Marshal(newGeo)

	// 11. 统计重叠和非重叠区域
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

	// 12. 记录操作
	RecordResult := models.GeoRecord{
		TableName:    jsonData.LayerName,
		Type:         "面要素去重叠",
		Date:         time.Now().Format("2006-01-02 15:04:05"),
		OldGeojson:   oldGeojson,
		NewGeojson:   newGeojson,
		DelObjectIDs: delObjectIDs,
	}
	DB.Create(&RecordResult)

	// 13. 构建返回数据
	returnData := make([]gin.H, len(newGeo.Features))
	for i, feature := range newGeo.Features {
		returnData[i] = gin.H{
			"id":         newFeatureIDs[i],
			"type":       analysisGeoJSON.Features[i].Properties["type"],
			"geojson":    feature.Geometry,
			"properties": feature.Properties,
		}
	}

	// 返回成功结果
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

// 同步编辑到文件
func (uc *UserController) SyncToFile(c *gin.Context) {
	TableName := c.Query("TableName")

	DB := models.DB
	var Schema models.MySchema
	if err := DB.Where("en = ?", TableName).First(&Schema).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    500,
			"message": fmt.Sprintf("未找到图层配置: %v", err),
		})
		return
	}

	var sourceConfigs []pgmvt.SourceConfig

	if err := json.Unmarshal(Schema.Source, &sourceConfigs); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    500,
			"message": fmt.Sprintf("解析源配置失败: %v", err),
		})
		return
	}
	sourceConfig := sourceConfigs[0]
	if sourceConfig.SourcePath == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    500,
			"message": "该图层没有绑定源文件",
		})
		return
	}

	var GeoRecords []models.GeoRecord
	DB.Where("table_name = ?", TableName).Find(&GeoRecords)

	// 判断源头文件是gdb还是shp
	Soucepath := sourceConfig.SourcePath
	SouceLayer := sourceConfig.SourceLayerName
	AttMap := sourceConfig.AttMap

	// 获取文件扩展名
	ext := strings.ToLower(filepath.Ext(Soucepath))

	if ext != ".shp" && ext != ".gdb" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    500,
			"message": "不支持的文件格式,仅支持GDB和SHP",
		})
		return
	}

	var totalDeleted int
	var totalInserted int
	var errors []string

	// 处理每条记录
	if len(GeoRecords) >= 1 {
		for _, record := range GeoRecords {
			// 1. 处理删除操作
			if len(record.DelObjectIDs) > 0 {
				var delIDs []int32
				if err := json.Unmarshal(record.DelObjectIDs, &delIDs); err != nil {
					errors = append(errors, fmt.Sprintf("解析DelObjectIDs失败: %v", err))
					continue
				}

				if len(delIDs) > 0 {
					// 构建WHERE子句 - 使用源文件的关键字段
					whereClause := buildWhereClause(sourceConfig.KeyAttribute, delIDs)

					var deletedCount int
					var err error

					if ext == ".shp" {
						// Shapefile删除
						deletedCount, err = Gogeo.DeleteShapeFeaturesByFilter(Soucepath, whereClause)
					}
					if ext == ".gdb" {
						deletedCount, err = Gogeo.DeleteFeaturesByFilter(Soucepath, SouceLayer, whereClause)
					}

					if err != nil {
						errors = append(errors, fmt.Sprintf("删除要素失败: %v", err))
						continue
					}
					totalDeleted += deletedCount
				}
			}

			// 2. 处理新增/更新操作
			if len(record.NewGeojson) > 0 {
				var fc geojson.FeatureCollection
				if err := json.Unmarshal(record.NewGeojson, &fc); err != nil {
					errors = append(errors, fmt.Sprintf("解析NewGeojson失败: %v", err))
					continue
				}

				if len(fc.Features) > 0 {
					// **关键步骤: 将GeoJSON字段名映射回源文件字段名**
					mappedFC, err := mapFieldsToSource(&fc, AttMap)
					if err != nil {
						errors = append(errors, fmt.Sprintf("字段映射失败: %v", err))
						continue
					}

					// 转换GeoJSON为GDALLayer
					gdalLayer, err := Gogeo.ConvertGeoJSONToGDALLayer(mappedFC, SouceLayer)
					if err != nil {
						errors = append(errors, fmt.Sprintf("转换GeoJSON失败: %v", err))
						continue
					}

					// 插入选项配置
					options := &Gogeo.InsertOptions{
						StrictMode:          false, // 非严格模式,允许部分失败
						SyncInterval:        100,   // 每100条同步一次
						SkipInvalidGeometry: true,  // 跳过无效几何
						CreateMissingFields: false, // 不创建缺失字段(使用现有字段)
					}

					var insertErr error
					if ext == ".shp" {
						// 插入到Shapefile
						insertErr = Gogeo.InsertLayerToShapefile(gdalLayer, Soucepath, options)
					}
					if ext == ".gdb" {
						// 插入到GDB
						insertErr = Gogeo.InsertLayerToGDB(gdalLayer, Soucepath, SouceLayer, options)
					}

					if insertErr != nil {
						errors = append(errors, fmt.Sprintf("插入要素失败: %v", insertErr))
						continue
					}
					totalInserted += len(fc.Features)

					// 清理资源
					if gdalLayer != nil {
						gdalLayer.Close() // 假设有Close方法
					}
				}
			}
		}
	}

	//同步字段操作
	var FieldRecord []models.FieldRecord
	DB.Where("table_name = ?", TableName).Find(&FieldRecord)
	postGISConfig := &Gogeo.PostGISConfig{
		Host:     config.MainConfig.Host,
		Port:     config.MainConfig.Port,
		Database: config.MainConfig.Dbname,
		User:     config.MainConfig.Username,
		Password: config.MainConfig.Password,
		Schema:   "public",
		Table:    TableName,
	}
	if len(FieldRecord) >= 1 {
		if ext == ".gdb" {
			for _, record := range FieldRecord {
				if record.Type == "value" {
					options := &Gogeo.SyncFieldOptions{
						SourceField:      record.OldFieldName,                   // PostGIS中的字段名
						TargetField:      mapField(record.OldFieldName, AttMap), // GDB中的字段名（可选，默认同名）
						SourceIDField:    "fid",                                 // PostGIS的ID字段（小写）
						TargetIDField:    "FID",
						BatchSize:        1000,
						UseTransaction:   true,
						UpdateNullValues: false,
					}
					_, err := Gogeo.SyncFieldFromPostGIS(
						postGISConfig,
						sourceConfig.SourcePath,
						SouceLayer,
						options,
					)
					if err != nil {
						fmt.Printf("同步失败: %v\n", err)
					}
				}
				if record.Type == "add" {
					gdbFieldType, width, precision := mapPostGISTypeToGDB(record.NewFieldType)
					fieldDef := Gogeo.FieldDefinition{
						Name:      record.NewFieldName,
						Type:      gdbFieldType,
						Width:     width,
						Precision: precision,
						Nullable:  true,
						Default:   nil,
					}
					err := Gogeo.AddField(Soucepath, SouceLayer, fieldDef)
					if err != nil {
						log.Fatal(err)
					}
				}
				if record.Type == "modify" {
					// 1. 删除旧字段
					oldFieldName := mapField(record.OldFieldName, AttMap)
					err := Gogeo.DeleteField(Soucepath, SouceLayer, oldFieldName)
					if err != nil {
						errors = append(errors, fmt.Sprintf("删除旧字段 %s 失败: %v", oldFieldName, err))
						continue
					}

					// 2. 创建新字段
					gdbFieldType, width, precision := mapPostGISTypeToGDB(record.NewFieldType)

					fieldDef := Gogeo.FieldDefinition{
						Name:      record.NewFieldName,
						Type:      gdbFieldType,
						Width:     width,
						Precision: precision,
						Nullable:  true,
						Default:   nil,
					}

					err = Gogeo.AddField(Soucepath, SouceLayer, fieldDef)
					if err != nil {
						errors = append(errors, fmt.Sprintf("创建新字段 %s 失败: %v", record.NewFieldName, err))
					} else {
						fmt.Printf("成功修改字段: %s -> %s\n", oldFieldName, record.NewFieldName)
					}
				}
				if record.Type == "delete" {
					Gogeo.DeleteField(Soucepath, SouceLayer, mapField(record.OldFieldName, AttMap))
				}

			}

		}

	}

	// 同步完成后,可选择清空GeoRecord记录
	DB.Where("table_name = ?", TableName).Delete(&models.GeoRecord{})
	DB.Where("table_name = ?", TableName).Delete(&models.FieldRecord{})
	// 构建响应
	response := gin.H{
		"code":           200,
		"message":        "同步完成",
		"total_deleted":  totalDeleted,
		"total_inserted": totalInserted,
		"records_count":  len(GeoRecords),
	}

	if len(errors) > 0 {
		response["code"] = 207 // 部分成功
		response["message"] = "同步完成,但有部分错误"
		response["errors"] = errors
	}

	c.JSON(http.StatusOK, response)
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
