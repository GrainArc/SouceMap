package views

import (
	"encoding/json"
	"fmt"
	"github.com/GrainArc/SouceMap/Transformer"
	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/models"
	"github.com/GrainArc/SouceMap/pgmvt"
	"github.com/gin-gonic/gin"
	"github.com/paulmach/orb/geojson"
	"gorm.io/gorm"
	"log"
	"net/http"
	"strings"
	"time"
)

// 单要素图斑获取
type getData struct {
	TableName string `json:"TableName"`
	ID        int32
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
	NewGeojson, _ := json.MarshalIndent(jsonData.GeoJson, "", "  ")
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

func (uc *UserController) DelGeoToSchema(c *gin.Context) {
	var jsonData delData
	c.BindJSON(&jsonData) //将前端geojson转换为geo对象
	DB := models.DB
	getData := getData{ID: jsonData.ID, TableName: jsonData.TableName}
	geo := GetGeo(getData)
	OldGeojson, _ := json.MarshalIndent(geo, "", "  ")
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
	result := models.GeoRecord{TableName: jsonData.TableName, GeoID: jsonData.ID, Username: jsonData.Username, Type: "要素删除", Date: time.Now().Format("2006-01-02 15:04:05"), OldGeojson: OldGeojson, BZ: jsonData.BZ}
	DB.Create(&result)
	geom := geo.Features[0].Geometry
	pgmvt.DelMVT(DB, jsonData.TableName, geom)
	c.JSON(http.StatusOK, "ok")
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
	result := models.GeoRecord{TableName: jsonData.TableName, GeoID: jsonData.ID, Username: jsonData.Username, Type: "要素修改", Date: time.Now().Format("2006-01-02 15:04:05"), OldGeojson: OldGeojson, NewGeojson: NewGeojson, BZ: jsonData.BZ}
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
	if schema.Type != "Polygon" {
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

	// 2. 获取当前表的最大ID，用于生成新ID
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

	// 3. 执行切割并插入新要素，删除原要素
	// 构建属性字段列表（排除id和geom）
	var columnNames []string
	var columnPlaceholders []string
	for key := range originalFeature {
		if key != "id" && key != "geom" {
			columnNames = append(columnNames, fmt.Sprintf(`"%s"`, key))
			columnPlaceholders = append(columnPlaceholders, fmt.Sprintf("t.%s", key))
		}
	}

	columnsStr := ""
	placeholdersStr := ""
	if len(columnNames) > 0 {
		columnsStr = ", " + strings.Join(columnNames, ", ")
		placeholdersStr = ", " + strings.Join(columnPlaceholders, ", ")
	}

	splitAndInsertSQL := fmt.Sprintf(`
		WITH original AS (
			-- 获取原要素
			SELECT * FROM "%s" WHERE id = %d
		),
		split_geom AS (
			-- 执行切割并分解为多个几何
			SELECT 
				ROW_NUMBER() OVER () as seq,
				(ST_Dump(ST_Split(o.geom, ST_GeomFromGeoJSON('%s')))).geom AS geom
				%s
			FROM original o
		)
		-- 插入新要素
		INSERT INTO "%s" (id, geom%s)
		SELECT 
			%d + seq AS id,
			s.geom
			%s
		FROM split_geom s
		RETURNING ST_AsGeoJSON(ST_Collect(geom)) AS geojson
	`, LayerName, jsonData.ID, line,
		func() string {
			if len(columnNames) > 0 {
				return ", o.*"
			}
			return ""
		}(),
		LayerName, columnsStr, maxID, placeholdersStr)

	type SplitResult struct {
		Geojson string
	}
	var splitResult SplitResult

	if err := tx.Raw(splitAndInsertSQL).Scan(&splitResult).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "切割并插入新要素失败: " + err.Error(),
			"data":    "",
		})
		return
	}

	// 4. 删除原要素
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

	// 5. 提交事务
	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "提交事务失败: " + err.Error(),
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
	// 返回成功结果
	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "切割成功，已生成多个新要素",
		"data":    splitResult.Geojson,
	})
}

// 图层要素合并
type DissolveData struct {
	LayerName string  `json:"LayerName"`
	IDs       []int32 `json:"ids"`
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
	if schema.Type != "Polygon" {
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

	// 2. 获取第一个要素的所有属性（作为合并后要素的属性）
	var originalFeature map[string]interface{}
	getOriginalSQL := fmt.Sprintf(`SELECT * FROM "%s" WHERE id = %d`, LayerName, jsonData.IDs[0])
	if err := tx.Raw(getOriginalSQL).Scan(&originalFeature).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "获取原要素属性失败: " + err.Error(),
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
			case int, int32, int64, float32, float64:
				columnValues = append(columnValues, fmt.Sprintf("%v", v))
			case bool:
				columnValues = append(columnValues, fmt.Sprintf("%t", v))
			default:
				// 其他类型尝试转换为字符串
				columnValues = append(columnValues, fmt.Sprintf("'%v'", v))
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
	for id := range jsonData.IDs {
		GetPdata := getData{
			TableName: LayerName,
			ID:        int32(id),
		}
		geom := GetGeo(GetPdata)
		pgmvt.DelMVT(DB, jsonData.LayerName, geom.Features[0].Geometry)
	}

	// 返回成功结果
	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": fmt.Sprintf("成功合并%d个要素为1个新要素", len(jsonData.IDs)),
		"data": gin.H{
			"geojson": dissolveResult.Geojson,
			"new_id":  dissolveResult.ID,
		},
	})
}
