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
	"net/http"
	"net/url"
	"os"
	"path/filepath"
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
		orderClause := fmt.Sprintf("%s %s", data.SortAttribute, sortType)
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

	switch operator {
	case "=", "!=", ">", "<", ">=", "<=":
		// 基本比较操作符
		sql := fmt.Sprintf("CAST(%s AS TEXT) %s ?", field, operator)
		return ConditionResult{sql: sql, args: []interface{}{value}}

	case "LIKE":
		// 模糊查询
		sql := fmt.Sprintf("CAST(%s AS TEXT) ILIKE ?", field)
		return ConditionResult{sql: sql, args: []interface{}{value}}

	case "NOT LIKE":
		// 不包含
		sql := fmt.Sprintf("CAST(%s AS TEXT) NOT ILIKE ?", field)
		return ConditionResult{sql: sql, args: []interface{}{value}}

	case "IS NULL":
		// 为空
		sql := fmt.Sprintf("%s IS NULL", field)
		return ConditionResult{sql: sql, args: []interface{}{}}

	case "IS NOT NULL":
		// 不为空
		sql := fmt.Sprintf("%s IS NOT NULL", field)
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
		sql := fmt.Sprintf("CAST(%s AS TEXT) IN (?)", field)
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
		sql := fmt.Sprintf("CAST(%s AS TEXT) NOT IN (?)", field)
		return ConditionResult{sql: sql, args: []interface{}{values}}

	default:
		// 默认使用等于
		sql := fmt.Sprintf("CAST(%s AS TEXT) = ?", field)
		return ConditionResult{sql: sql, args: []interface{}{value}}
	}
}

// buildSimpleConditions 构建简单查询条件（兼容旧格式）
func buildSimpleConditions(query *gorm.DB, rule map[string]interface{}, tableName string) *gorm.DB {
	for key, value := range rule {
		searchValue := fmt.Sprintf("%%%v%%", value)

		if key == "all_data_search" {
			// 获取表中所有字段
			atts := GetAtt(tableName, "")
			for _, field := range atts {
				condition := fmt.Sprintf("CAST(%s AS TEXT) ILIKE ?", field)
				query = query.Or(condition, searchValue)
			}
		} else {
			condition := fmt.Sprintf("CAST(%s AS TEXT) ILIKE ?", key)
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
	existingZipPath := "OutFile/" + taskid + "/" + SD.TableName + ".zip"
	if _, err := os.Stat(existingZipPath); err == nil {
		// 文件存在，直接返回路径
		return existingZipPath
	}

	result, _ := queryTable(DB, SD)

	outdir := "OutFile/" + taskid
	os.MkdirAll(outdir, os.ModePerm)
	outshp := "OutFile/" + taskid + "/" + Schema.CN + ".shp"

	// 直接从查询结果转换为Shapefile
	Gogeo.ConvertPostGISToShapefileWithStructure(DB, result.Data, outshp, SD.TableName)

	methods.ZipFolder(outdir, SD.TableName)
	copyFile("./OutFile/"+taskid+"/"+SD.TableName+".zip", config.MainConfig.Download)

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
func (uc *UserController) GetChangeRecord(c *gin.Context) {
	username := c.Query("Username")
	DB := models.DB
	var aa []models.GeoRecord
	DB.Where("username = ?", username).Find(&aa)
	c.JSON(http.StatusOK, aa)
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

	// ========== 查询 id 和 objectid 的最大值 ==========
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

	var maxObjectID int32
	getMaxObjectIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX(objectid), 0) as max_id FROM "%s"`, LayerName)
	if err := tx.Raw(getMaxObjectIDSQL).Scan(&maxObjectID).Error; err != nil {
		// objectid 可能不存在，忽略错误
		maxObjectID = 0
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

	// 构建 SELECT 的列名（包括id的自增逻辑）
	selectCols := fmt.Sprintf(`%d + ROW_NUMBER() OVER () AS id, geom`, maxID)
	for _, col := range additionalColumns {
		if col == "objectid" {
			// 使用 ROW_NUMBER() 生成递增的 objectid
			selectCols += fmt.Sprintf(`, %d + ROW_NUMBER() OVER () AS "%s"`, maxObjectID, col)
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
	// ========== 新增：查询 objectid 的最大值 ==========
	var maxObjectID int32
	getMaxObjectIDSQL := fmt.Sprintf(`SELECT COALESCE(MAX(objectid), 0) FROM "%s"`, LayerName)
	if err := tx.Raw(getMaxObjectIDSQL).Scan(&maxObjectID).Error; err != nil {

	}
	// 4. 构建属性字段列表（排除id和geom）
	var columnNames []string
	var columnValues []string
	for key, value := range originalFeature {
		if key != "id" && key != "geom" {
			columnNames = append(columnNames, fmt.Sprintf(`"%s"`, key))
			if key == "objectid" {
				columnValues = append(columnValues, fmt.Sprintf("%d", maxObjectID+1))
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
	RecordResult := models.GeoRecord{TableName: jsonData.LayerName,
		Type:         "要素合并",
		Date:         time.Now().Format("2006-01-02 15:04:05"),
		OldGeojson:   oldGeojson,
		NewGeojson:   newGeoJson,
		DelObjectIDs: delObjJSON}

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

	// 判断是否为GDB(GDB通常是文件夹,扩展名为.gdb)
	isGDB := ext == ".gdb" || strings.HasSuffix(strings.ToLower(Soucepath), ".gdb")
	isSHP := ext == ".shp"

	if !isGDB && !isSHP {
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

					if isSHP {
						// Shapefile删除
						deletedCount, err = Gogeo.DeleteShapeFeaturesByFilter(Soucepath, whereClause)
					} else if isGDB {
						// GDB删除
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
					if isSHP {
						// 插入到Shapefile
						insertErr = Gogeo.InsertLayerToShapefile(gdalLayer, Soucepath, options)
					} else if isGDB {
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
		if isGDB {
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

// mapPostGISTypeToGDB 将PostgreSQL字段类型映射为GDB字段类型
// 返回: FieldType, Width, Precision
func mapPostGISTypeToGDB(pgType string) (Gogeo.FieldType, int, int) {
	// 转换为小写便于匹配
	pgType = strings.ToLower(strings.TrimSpace(pgType))

	// 处理带长度的类型，如 varchar(255)
	var baseType string
	var width int
	var precision int

	// 解析类型定义
	if strings.Contains(pgType, "(") {
		parts := strings.Split(pgType, "(")
		baseType = parts[0]

		// 提取宽度和精度
		if len(parts) > 1 {
			params := strings.TrimSuffix(parts[1], ")")
			paramParts := strings.Split(params, ",")

			if len(paramParts) > 0 {
				fmt.Sscanf(paramParts[0], "%d", &width)
			}
			if len(paramParts) > 1 {
				fmt.Sscanf(paramParts[1], "%d", &precision)
			}
		}
	} else {
		baseType = pgType
	}

	// 类型映射
	switch baseType {
	case "smallint", "int2":
		return Gogeo.FieldTypeInteger, 0, 0

	case "integer", "int", "int4":
		return Gogeo.FieldTypeInteger, 0, 0

	case "bigint", "int8":
		return Gogeo.FieldTypeInteger64, 0, 0

	case "real", "float4":
		return Gogeo.FieldTypeReal, 0, 0

	case "double precision", "float8", "numeric", "decimal":
		if precision == 0 {
			precision = 2 // 默认精度
		}
		return Gogeo.FieldTypeReal, 0, precision

	case "character varying", "varchar", "character", "char", "text":
		if width == 0 {
			width = 254 // GDB字符串默认长度
		}
		return Gogeo.FieldTypeString, width, 0

	case "date":
		return Gogeo.FieldTypeDate, 0, 0

	case "time", "time without time zone":
		return Gogeo.FieldTypeTime, 0, 0

	case "timestamp", "timestamp without time zone", "timestamp with time zone":
		return Gogeo.FieldTypeDateTime, 0, 0

	case "bytea":
		return Gogeo.FieldTypeBinary, 0, 0

	default:
		// 默认作为字符串处理
		if width == 0 {
			width = 254
		}
		return Gogeo.FieldTypeString, width, 0
	}
}
