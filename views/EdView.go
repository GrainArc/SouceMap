package views

import (
	"encoding/json"
	"fmt"
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
