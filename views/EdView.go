package views

import (
	"encoding/json"
	"fmt"
	"github.com/GrainArc/Gogeo"
	"github.com/GrainArc/SouceMap/Transformer"
	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/models"
	"github.com/GrainArc/SouceMap/pgmvt"
	"github.com/gin-gonic/gin"
	"github.com/paulmach/orb/geojson"
	"gorm.io/gorm"
	"log"
	"net/http"
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

func GetGeos(jsonData getDatas) (geojson.FeatureCollection, error) {
	if len(jsonData.ID) == 0 {
		return geojson.FeatureCollection{}, fmt.Errorf("ID list cannot be empty")
	}

	DB := models.DB

	// 构建 IN 查询的占位符
	placeholders := make([]string, len(jsonData.ID))
	args := make([]interface{}, len(jsonData.ID)+1)
	args[0] = jsonData.TableName

	for i, id := range jsonData.ID {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args[i+1] = id
	}

	// 使用参数化查询防止 SQL 注入
	sql := fmt.Sprintf(`
		SELECT 
			ST_AsGeoJSON(geom) AS geojson,
			to_jsonb(record) - 'geom' AS properties
		FROM %s AS record
		WHERE id IN (%s);
	`, jsonData.TableName, strings.Join(placeholders, ", "))

	var dataList []outData
	if err := DB.Raw(sql, args[1:]...).Scan(&dataList).Error; err != nil {
		return geojson.FeatureCollection{}, fmt.Errorf("database query error: %w", err)
	}

	newGeo := geojson.FeatureCollection{
		Type:     "FeatureCollection",
		Features: make([]*geojson.Feature, 0, len(dataList)),
	}

	for _, data := range dataList {
		feature := struct {
			Geometry   map[string]interface{} `json:"geometry"`
			Properties map[string]interface{} `json:"properties"`
			Type       string                 `json:"type"`
		}{
			Type: "Feature",
		}

		// 解析 GeoJSON 几何和属性
		if err := json.Unmarshal(data.GeoJson, &feature.Geometry); err != nil {
			return newGeo, fmt.Errorf("failed to unmarshal geometry: %w", err)
		}

		if err := json.Unmarshal(data.Properties, &feature.Properties); err != nil {
			return newGeo, fmt.Errorf("failed to unmarshal properties: %w", err)
		}

		// 转换为 geojson.Feature
		featureData, err := json.Marshal(feature)
		if err != nil {
			return newGeo, fmt.Errorf("failed to marshal feature: %w", err)
		}

		var myFeature *geojson.Feature
		if err := json.Unmarshal(featureData, &myFeature); err != nil {
			return newGeo, fmt.Errorf("failed to unmarshal to geojson.Feature: %w", err)
		}

		newGeo.Features = append(newGeo.Features, myFeature)
	}

	return newGeo, nil
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
	possibleKeys := []string{"objectid", "OBJECTID", "ObjectID", "ObjectId", "objectId"}

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
	DB.Create(&result)
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
	DB.Create(&result)
	geom := geo.Features[0].Geometry
	geom2 := jsonData.GeoJson.Features[0].Geometry
	pgmvt.DelMVT(DB, jsonData.TableName, geom)
	pgmvt.DelMVT(DB, jsonData.TableName, geom2)
	c.JSON(http.StatusOK, jsonData.GeoJson)
}

// 图层要素查询

type searchData struct {
	Rule          map[string]interface{} `json:"Rule"`
	TableName     string                 `json:"TableName"`
	Page          int                    `json:"page"`
	PageSize      int                    `json:"pagesize"`
	SortAttribute string                 `json:"SortAttribute"` // 排序字段
	SortType      string                 `json:"SortType"`      // 排序方式：DESC/ASC
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

	query := db.Table(data.TableName)

	// 动态构建查询条件
	for key, value := range data.Rule {
		searchValue := fmt.Sprintf("%%%v%%", value)

		if key == "all_data_search" {
			// 获取表中所有字段
			atts := GetAtt(data.TableName, "")
			for _, field := range atts {
				condition := fmt.Sprintf("CAST(%s AS TEXT) ILIKE ?", field)
				query = query.Or(condition, searchValue)
			}
		} else {
			condition := fmt.Sprintf("CAST(%s AS TEXT) ILIKE ?", key)
			query = query.Where(condition, searchValue)
		}
	}

	// 添加排序条件
	if data.SortAttribute != "" {
		sortType := strings.ToUpper(data.SortType)
		// 验证排序类型，默认为ASC
		if sortType != "DESC" && sortType != "ASC" {
			sortType = "ASC"
		}

		// 构建排序语句
		orderClause := fmt.Sprintf("%s %s", data.SortAttribute, sortType)
		query = query.Order(orderClause)
	}

	// 获取总数
	if err := query.Count(&total).Error; err != nil {
		fmt.Println("获取总数出错:", err.Error())
		return nil, err
	}

	// 检查是否需要分页
	if data.Page > 0 && data.PageSize > 0 {
		offset := (data.Page - 1) * data.PageSize
		query = query.Offset(offset).Limit(data.PageSize)
	}

	// 执行查询
	if err := query.Find(&results).Error; err != nil {
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

func (uc *UserController) SearchGeoFromSchema(c *gin.Context) {
	var jsonData searchData
	c.BindJSON(&jsonData) //将前端geojson转换为geo对象
	DB := models.DB
	result, _ := queryTable(DB, jsonData)
	data := methods.MakeGeoJSON2(result.Data)
	type outdata struct {
		Data       interface{} `json:"data"`
		Total      int64       `json:"total"`
		Page       int         `json:"page"`
		PageSize   int         `json:"pageSize"`
		TotalPages int         `json:"totalPages"`
		TableName  string      `json:"TableName"`
	}
	response := outdata{
		Data:       data,              // 设置地理数据
		Page:       result.Page,       // 设置当前页码
		Total:      result.Total,      // 设置总记录数
		TotalPages: result.TotalPages, // 设置总页数
		PageSize:   result.PageSize,   // 设置每页大小
		TableName:  jsonData.TableName,
	}
	c.JSON(http.StatusOK, response)
}

// 获取修改记录
func (uc *UserController) GetChangeRecord(c *gin.Context) {
	username := c.Query("Username")
	DB := models.DB
	var aa []models.GeoRecord
	DB.Where("username = ?", username).Find(&aa)
	c.JSON(http.StatusOK, aa)
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
		json.Unmarshal(aa.NewGeojson, &featureCollection)
		methods.UpdateGeojsonToTable(DB, featureCollection, aa.TableName, aa.GeoID)
		pgmvt.DelMVT(DB, aa.TableName, featureCollection2.Features[0].Geometry)
		pgmvt.DelMVT(DB, aa.TableName, featureCollection.Features[0].Geometry)

	}
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
	// 绑定JSON请求体到结构体,并检查绑定是否成功
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

	// 2. 执行切割并插入新要素
	// 获取所有附加列（排除id和geom）
	var additionalColumns []string
	for key := range originalFeature {
		if key != "id" && key != "geom" {
			additionalColumns = append(additionalColumns, key)
		}
	}

	// 构建 split_geom CTE 的选择列
	splitSelectCols := `(ST_Dump(ST_Split(o.geom, ST_GeomFromGeoJSON('%s')))).geom AS geom`
	for _, col := range additionalColumns {
		splitSelectCols += fmt.Sprintf(`, o."%s"`, col)
	}

	// 构建 INSERT 的列名（不包括id，让数据库自动生成）
	insertCols := "geom"
	for _, col := range additionalColumns {
		insertCols += fmt.Sprintf(`, "%s"`, col)
	}

	// 构建 SELECT 的列名（不包括id）
	selectCols := "geom"
	for _, col := range additionalColumns {
		selectCols += fmt.Sprintf(`, "%s"`, col)
	}

	// 方案1：如果id列有DEFAULT值（如SERIAL或使用nextval）
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

	//更新缓存库
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
	splitGeojson, _ := GetGeos(getdata2)
	// 5. 提交事务
	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "提交事务失败: " + err.Error(),
			"data":    "",
		})
		return
	}
	delObjJSON := DelIDGen(geom)
	OldGeojson, _ := json.Marshal(geom)
	NewGeojson, _ := json.Marshal(splitGeojson)
	RecordResult := models.GeoRecord{TableName: jsonData.LayerName,
		GeoID:        jsonData.ID,
		Type:         "要素分割",
		Date:         time.Now().Format("2006-01-02 15:04:05"),
		OldGeojson:   OldGeojson,
		NewGeojson:   NewGeojson,
		DelObjectIDs: delObjJSON}

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

	// 4. 构建属性字段列表（排除id和geom）
	var columnNames []string
	var columnValues []string
	for key, value := range originalFeature {
		if key != "id" && key != "geom" {
			columnNames = append(columnNames, fmt.Sprintf(`"%s"`, key))

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
	oldGeo, _ := GetGeos(getdata2)
	oldGeojson, _ := json.Marshal(oldGeo)

	GetPdata := getDatas{
		TableName: LayerName,
		ID:        jsonData.IDs,
	}
	newGeo, _ := GetGeos(GetPdata)
	for _, feature := range oldGeo.Features {

		pgmvt.DelMVT(DB, jsonData.LayerName, feature.Geometry)
	}
	newGeoJson, _ := json.Marshal(newGeo)
	delObjJSON := DelIDGen(oldGeo)
	RecordResult := models.GeoRecord{TableName: jsonData.LayerName,
		Type:         "要素合并",
		Date:         time.Now().Format("2006-01-02 15:04:05"),
		OldGeojson:   oldGeojson,
		NewGeojson:   newGeoJson,
		DelObjectIDs: delObjJSON}

	DB.Create(&RecordResult)
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

	var sourceConfig pgmvt.SourceConfig
	if err := json.Unmarshal(Schema.Source, &sourceConfig); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    500,
			"message": fmt.Sprintf("解析源配置失败: %v", err),
		})
		return
	}

	if sourceConfig.SourcePath == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    500,
			"message": "该图层没有绑定源文件",
		})
		return
	}

	var GeoRecords []models.GeoRecord
	DB.Where("table_name = ?", TableName).Find(&GeoRecords)
	if len(GeoRecords) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    500,
			"message": "没有修改记录",
		})
		return
	}

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

	// 同步完成后,可选择清空GeoRecord记录
	// DB.Where("table_name = ?", TableName).Delete(&models.GeoRecord{})

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
