package views

import (
	"context"
	"encoding/json"
	"fmt"
	"gitee.com/gooffice/gooffice/document"
	"github.com/GrainArc/SouceMap/Transformer"
	"github.com/GrainArc/SouceMap/WordGenerator"
	"github.com/GrainArc/SouceMap/config"
	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/models"
	"github.com/GrainArc/SouceMap/pgmvt"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/paulmach/orb/geojson"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type Statistic struct {
	TableName     string
	TableEN       string
	Attribute     string
	GroupedResult []methods.Result
}
type SearchDataChilden struct {
	TableName   string
	TableNameCN string
	Attribute   string
}
type SearchData struct {
	IntersectList []SearchDataChilden
	Geojson       geojson.FeatureCollection
}

func (uc *UserController) SpaceIntersect(c *gin.Context) {
	var jsonData SearchData
	c.BindJSON(&jsonData) //将前端geojson转换为geo对象
	var result_data []interface{}
	for _, item := range jsonData.IntersectList {
		TableName := item.TableName
		groupedResult := methods.GeoIntersect(jsonData.Geojson, TableName, item.Attribute)
		var data = Statistic{
			TableName:     item.TableNameCN,
			Attribute:     item.Attribute,
			TableEN:       item.TableName,
			GroupedResult: groupedResult,
		}
		out := methods.LowerJSONTransform(data)

		result_data = append(result_data, out)
	}

	c.JSON(http.StatusOK, result_data)
}

// 导出相交的矢量
type OutData struct {
	Tablename string
	Attribute string
	Geojson   geojson.FeatureCollection
}

func (uc *UserController) OutIntersect(c *gin.Context) {
	var jsonData OutData
	c.BindJSON(&jsonData) //将前端geojson转换为geo对象

	atts := strings.Join(GetAtt(jsonData.Tablename, ""), ",")
	geo2 := methods.GetIntersectGeo(jsonData.Geojson, jsonData.Tablename, atts)

	if len(geo2.Features) > 0 {
		groupedResult := methods.GeoIntersect(jsonData.Geojson, jsonData.Tablename, jsonData.Attribute)
		doc, _ := document.Open("./word/空模板.docx")
		area := 0.00
		for _, item := range groupedResult {
			area = area + item.Area
		}
		WordGenerator.OutTable(doc, groupedResult, area)

		geo, _ := Transformer.GeoJsonTransformToCGCS(&geo2)
		bsm := uuid.New().String()
		os.Mkdir(filepath.Join("OutFile", bsm), os.ModePerm)
		ctime := time.Now().Format("2006-01-02")
		outDir := filepath.Join("OutFile", bsm)
		Transformer.ConvertGeoJSONToSHP(geo, filepath.Join(outDir, "矢量.shp"))
		absolutePath2, _ := filepath.Abs(outDir)
		doc.SaveToFile(absolutePath2 + "/分析表格.docx")
		methods.ZipFolder(absolutePath2, ctime+jsonData.Tablename+"分析成果")
		copyFile("./OutFile/"+bsm+"/"+ctime+jsonData.Tablename+"分析成果"+".zip", config.Download)
		host := c.Request.Host
		url := &url.URL{
			Scheme: "http",
			Host:   host,
			Path:   "/geo/OutFile/" + bsm + "/" + ctime + jsonData.Tablename + "分析成果" + ".zip",
		}
		c.String(http.StatusOK, url.String())
	} else {
		c.String(http.StatusOK, "图形未相交")
	}

}

type Att struct {
	Text  string `json:"text"`
	Value string `json:"value"`
}
type Res struct {
	ColumnName string
}

// 获取图层的字段
func (uc *UserController) GetTableAttributes(c *gin.Context) {
	TableName := strings.ToLower(c.Query("TableName"))
	var result []Res
	DB := models.DB
	sql := `SELECT  column_name
                        FROM  information_schema.columns
                        WHERE  table_name  =  ?
` //  在GORM中使用原生SQL查询时，你应当使用Raw方法来执行查询，而Scan方法用于扫描结果到一个结果集合
	err := DB.Raw(sql, TableName).Scan(&result).Error

	//  如果执行数据库操作时发生错误，向客户端返回错误信息
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal  Server  Error"}) //  使用http状态码500示意服务器内部错误
		return                                                                            //  提前返回，防止进一步执行
	}
	var atts []Att
	for _, item := range result {
		switch item.ColumnName {
		case "id", "geom", "tbmj", "mj":
			// 如果不是"id", "geom", "tbmj"，则创建一个新的Att结构体并添加到atts中
		default:
			attmap := GetCEMap(TableName)
			text, ok := attmap[item.ColumnName]
			if !ok {
				text = item.ColumnName
			}
			aa := Att{text, item.ColumnName}
			atts = append(atts, aa)
		}
	}
	c.JSON(http.StatusOK, atts)
}

func (uc *UserController) Area(c *gin.Context) {
	var jsonData *geojson.Feature
	c.BindJSON(&jsonData) //将前端geojson转换为geo对象
	area := methods.CalculateArea(jsonData)
	c.JSON(http.StatusOK, area)
}

func (uc *UserController) GeodesicArea(c *gin.Context) {
	var jsonData *geojson.Feature
	c.BindJSON(&jsonData) //将前端geojson转换为geo对象
	area := methods.CalculateGeodesicArea(jsonData)
	c.JSON(http.StatusOK, area)
}

// 表名获取
type LayerSchema struct {
	ID       int64
	Main     string
	CN       string
	EN       string
	Date     string
	Type     string
	Opacity  string
	Color    string    `json:"Color"`
	ColorSet ColorData `json:"ColorSet"`
}

func sortByID(data []LayerSchema) {
	sort.Slice(data, func(i, j int) bool {
		return data[i].ID < data[j].ID
	})
}
func (uc *UserController) GetSchema(c *gin.Context) {
	db := models.DB
	var result []models.MySchema
	db.Find(&result)

	dataMap := make(map[string]models.MySchema)
	for _, item := range result {
		item.EN = strings.ToLower(item.EN)
		dataMap[item.EN] = item
	}
	var data []LayerSchema
	for _, value := range dataMap {
		C := GetColor(value.EN)
		if len(C) > 0 {
			data = append(data, LayerSchema{
				ID:       value.ID,
				Main:     value.Main,
				CN:       value.CN,
				EN:       value.EN,
				Type:     value.Type,
				Date:     value.UpdatedDate,
				Opacity:  value.Opacity,
				ColorSet: C[0],
			})
		} else {
			data = append(data, LayerSchema{
				ID:      value.ID,
				Main:    value.Main,
				CN:      value.CN,
				EN:      value.EN,
				Date:    value.UpdatedDate,
				Type:    value.Type,
				Opacity: value.Opacity,
				Color:   value.Color,
			})
		}

	}
	sortByID(data)

	c.JSON(http.StatusOK, data)
}

func (uc *UserController) SchemaToExcel(c *gin.Context) {
	db := models.DB
	var result []models.MySchema
	db.Find(&result)
	dataMap := make(map[string]models.MySchema)
	for _, item := range result {
		item.EN = strings.ToLower(item.EN)
		dataMap[item.EN] = item
	}
	var data []WordGenerator.LayerSchema2
	for _, value := range dataMap {
		data = append(data, WordGenerator.LayerSchema2{
			ID:      value.ID,
			Main:    value.Main,
			CN:      value.CN,
			EN:      value.EN,
			Date:    value.UpdatedDate,
			Type:    value.Type,
			Opacity: value.Opacity,
		})

	}
	sort.Slice(data, func(i, j int) bool {
		return data[i].Main < data[j].Main
	})
	doc, _ := document.Open("./word/空模板.docx")
	WordGenerator.OutSchema(doc, data)
	bsm := uuid.New().String()
	os.Mkdir(filepath.Join("OutFile", bsm), os.ModePerm)
	outDir := filepath.Join("OutFile", bsm)
	absolutePath2, _ := filepath.Abs(outDir)
	doc.SaveToFile(absolutePath2 + "/图层总表预览.docx")
	host := c.Request.Host
	url := &url.URL{
		Scheme: "http",
		Host:   host,
		Path:   "/geo/OutFile/" + bsm + "/图层总表预览.docx",
	}
	c.String(http.StatusOK, url.String())
}

func (uc *UserController) GetSchemaByUnits(c *gin.Context) {
	userunits := c.Query("userunits")
	db := models.DB
	var result []models.MySchema
	db.Where("userunits = ?  OR userunits = '' OR userunits IS NULL", userunits).Find(&result)

	dataMap := make(map[string]models.MySchema)
	for _, item := range result {
		item.EN = strings.ToLower(item.EN)
		dataMap[item.EN] = item
	}
	var data []LayerSchema
	for _, value := range dataMap {
		C := GetColor(value.EN)
		if len(C) > 0 {
			data = append(data, LayerSchema{
				ID:       value.ID,
				Main:     value.Main,
				CN:       value.CN,
				EN:       value.EN,
				Type:     value.Type,
				Opacity:  value.Opacity,
				ColorSet: C[0],
			})
		} else {
			data = append(data, LayerSchema{
				ID:      value.ID,
				Main:    value.Main,
				CN:      value.CN,
				EN:      value.EN,
				Type:    value.Type,
				Opacity: value.Opacity,
				Color:   value.Color,
			})
		}

	}
	sortByID(data)

	c.JSON(http.StatusOK, data)
}

func (uc *UserController) AddSchema(c *gin.Context) {
	Main := c.PostForm("Main")
	CN := c.PostForm("CN")
	EN := methods.ConvertToInitials(Main) + "_" + methods.ConvertToInitials(CN)

	Color := c.PostForm("Color")
	Opacity := c.PostForm("Opacity")
	Userunits := c.PostForm("userunits")
	file, err := c.FormFile("file")
	if err != nil {
		c.String(400, "Bad request")
		return
	}

	taskid := uuid.New().String()
	path, _ := filepath.Abs("./TempFile/" + taskid + "/" + "/" + file.Filename)
	dirpath := filepath.Dir(path)
	err = c.SaveUploadedFile(file, path)
	if err != nil {
		c.String(500, "Internal server error")
		return
	}
	if filepath.Ext(path) == ".zip" || filepath.Ext(path) == ".rar" {
		methods.Unzip(path)
	}
	DB := models.DB

	// 直接处理SHP文件并写入数据库
	gdbfiles := Transformer.FindFiles(dirpath, "gdb")
	if len(gdbfiles) != 0 {
		for _, gdbfile := range gdbfiles {
			ENS := pgmvt.AddGDBDirectlyOptimized(DB, gdbfile, Main, Color, Opacity, Userunits)
			for _, item := range ENS {
				MakeGeoIndex(item)
			}
		}

	}
	shpfiles := Transformer.FindFiles(dirpath, "shp")
	if len(shpfiles) != 0 {
		EN2 := pgmvt.AddSHPDirectlyOptimized(DB, shpfiles[0], EN, CN, Main, Color, Opacity, Userunits)
		MakeGeoIndex(EN2)
	}
	c.String(http.StatusOK, "ok")
}

// 删除图层
func isEndWithNumber(s string) bool {
	for _, char := range s {
		if unicode.IsDigit(char) && s[len(s)-1] == byte(char) {
			return true
		}
	}
	return false
}
func (uc *UserController) DelSchema(c *gin.Context) {
	TableName := strings.ToLower(c.Query("TableName"))
	var TableNames string
	if isEndWithNumber(TableName) {
		TableNames = TableName + "," + TableName + "_mvt"
	} else {
		TableNames = TableName + "," + TableName + "mvt"
	}
	DB := models.DB
	var Schemas []models.MySchema
	DB.Where("LOWER(en) = LOWER(?)", TableName).Find(&Schemas)
	if len(Schemas) > 0 {
		DB.Delete(&Schemas)
	}
	sql := fmt.Sprintf("DROP  TABLE  IF  EXISTS  %s", TableNames)
	if err := DB.Exec(sql).Error; err != nil {
		c.String(http.StatusOK, "Failed  to  delete  table")

	} else {
		c.String(http.StatusOK, "ok")
	}

}

// 修改图层信息
func (uc *UserController) ChangeSchema(c *gin.Context) {
	CN := c.PostForm("CN")
	Main := c.PostForm("Main")
	Color := c.PostForm("Color")
	ID := c.PostForm("ID")
	id, _ := strconv.Atoi(ID)
	var Schemas models.MySchema
	DB := models.DB
	DB.Where("ID = ?", id).Find(&Schemas)

	Schemas.CN = CN
	if len(Color) >= 254 {
		Schemas.Color = ""
	} else {
		Schemas.Color = Color
	}
	Schemas.Main = Main
	DB.Save(&Schemas)

	c.JSON(http.StatusOK, Schemas)
}

type CaptureType struct {
	Point     []float64
	Layer     string
	TempLayer []string
}

type CaptureData struct {
	Distance float64 `gorm:"column:distance"`
	GeoJSON  []byte  `gorm:"column:geojson"`
}
type GeometryPoint struct {
	Coordinates []float64 `json:"coordinates"`
	Type        string    `json:"type"`
}

func GetTempLayers(layernameSlice []string) []*geojson.Feature {
	var mytable []models.TempLayer
	DB := models.DB
	// 构建存放 layername 的切片
	DB.Where("bsm IN ? ", layernameSlice).Find(&mytable)
	var data []*geojson.Feature
	for index, item := range mytable {
		var featureCollection struct {
			Features []*geojson.Feature `json:"features"`
		}

		error := json.Unmarshal(item.Geojson, &featureCollection)
		if error == nil {
			if _, exists := featureCollection.Features[0].Properties["name"]; exists {
			} else {
				featureCollection.Features[0].Properties["name"] = strconv.Itoa(index)
			}
			featureCollection.Features[0].ID = item.TBID
			featureCollection.Features[0].Properties["zt"] = item.ZT
			featureCollection.Features[0].Properties["TBID"] = item.TBID
			data = append(data, featureCollection.Features...)
		}
	}

	return data
}

// 图形捕捉
func (uc *UserController) Capture(c *gin.Context) {
	var jsonData CaptureType
	c.BindJSON(&jsonData)
	DB := models.DB

	Templarers := jsonData.TempLayer
	//临时图层同时存在
	if len(Templarers) != 0 && jsonData.Layer != "" {
		var sql string
		geojsons := GetTempLayers(Templarers)
		//点线面分离
		var PolygonJson []*geojson.Feature
		var LineJson []*geojson.Feature
		var PointJson []*geojson.Feature
		for _, item := range geojsons {
			switch item.Geometry.GeoJSONType() {
			case "Polygon":
				PolygonJson = append(PolygonJson, item)
			case "MultiPolygon":
				PolygonJson = append(PolygonJson, item)
			case "LineString":
				LineJson = append(LineJson, item)
			case "Point":
				PointJson = append(PointJson, item)
			}
		}
		//面数据捕捉
		if len(PolygonJson) != 0 {
			geo := Transformer.GetFeatureString(PolygonJson)
			sql = fmt.Sprintf(`
WITH input_point AS (
    SELECT ST_SetSRID(ST_MakePoint(%f, %f), 4326) AS geom
),
geojson_data AS (
    SELECT 
        jsonb_array_elements('%s'::jsonb) AS feature
),
nearest_polygon AS (
    SELECT 
        ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326) AS poly_geom
    FROM 
        geojson_data, input_point AS input
    WHERE 
        ST_Distance(input.geom, ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326)) < 0.0005 
    ORDER BY 
        ST_Distance(input.geom, ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326)) 
    LIMIT 1
),
line_geom AS (
    SELECT 
        ST_Boundary(poly_geom) AS line_geom
    FROM 
        nearest_polygon
)
SELECT 
    ST_AsGeoJSON(ST_ClosestPoint(line_geom.line_geom, input.geom)) AS geojson,
    ST_Distance(input.geom, ST_ClosestPoint(line_geom.line_geom, input.geom))::float AS distance
FROM 
    input_point AS input, line_geom;

			`, jsonData.Point[0], jsonData.Point[1], geo)
		}
		//线数据捕捉
		if len(LineJson) != 0 {
			geo := Transformer.GetFeatureString(LineJson)
			sql = fmt.Sprintf(`
				WITH input_point AS (
					SELECT ST_SetSRID(ST_MakePoint(%f, %f), 4326) AS geom
				),
				geojson_data AS (
					SELECT 
						jsonb_array_elements('%s'::jsonb) AS feature
				),
				nearest_line AS (
					SELECT 
						ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326) AS line_geom
					FROM 
						geojson_data, input_point AS input
					WHERE 
						ST_Distance(input.geom, ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326)) < 0.0005 
					ORDER BY 
						ST_Distance(input.geom, ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326)) 
					LIMIT 1
				)
				SELECT 
					ST_AsGeoJSON(ST_ClosestPoint(line_geom, input.geom)) AS geojson,
					ST_Distance(input.geom, ST_ClosestPoint(line_geom, input.geom))::float AS distance
				FROM 
					input_point AS input, nearest_line;
			`, jsonData.Point[0], jsonData.Point[1], geo)
		}
		//点数据捕捉
		if len(PointJson) != 0 {
			geo := Transformer.GetFeatureString(PointJson)
			sql = fmt.Sprintf(`
				WITH input_point AS (
					SELECT ST_SetSRID(ST_MakePoint(%f, %f), 4326) AS geom
				),
				geojson_data AS (
					SELECT 
						jsonb_array_elements('%s'::jsonb) AS feature
				),
				nearest_point AS (
					SELECT 
						ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326) AS point_geom
					FROM 
						geojson_data, input_point AS input
					WHERE 
						ST_Distance(input.geom, ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326)) < 0.0005 
					ORDER BY 
						ST_Distance(input.geom, ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326)) 
					LIMIT 1
				)
				SELECT 
					ST_AsGeoJSON(point_geom) AS geojson,
					ST_Distance(input.geom, point_geom)::float AS distance
				FROM 
					input_point AS input, nearest_point;
			`, jsonData.Point[0], jsonData.Point[1], geo)
		}
		var geomData CaptureData
		err := DB.Raw(sql).Scan(&geomData)
		if err.Error != nil {
			c.String(http.StatusBadRequest, "err")
			return
		}
		var feature struct {
			Geometry GeometryPoint `json:"geometry"`
		}
		json.Unmarshal(geomData.GeoJSON, &feature.Geometry)
		if geomData.Distance <= 0.00015 && len(feature.Geometry.Coordinates) != 0 {
			c.JSON(http.StatusOK, feature.Geometry.Coordinates)
		} else {
			layer := jsonData.Layer
			var schema []models.MySchema
			DB.Where("en = ?", layer).Find(&schema)
			if len(schema) >= 1 {
				switch schema[0].Type {
				case "polygon":
					sql = fmt.Sprintf(`
			WITH input_point AS (
				SELECT ST_SetSRID(ST_MakePoint(%f, %f), 4326) AS geom
			),
			nearest_polygon AS (
				SELECT 
					%s.geom AS poly_geom
				FROM 
					%s, input_point AS input
				WHERE 
					ST_Distance(input.geom, %s.geom) < 0.0005  
				ORDER BY 
					ST_Distance(input.geom, %s.geom) 
				LIMIT 1
			),
			line_geom AS (
				SELECT 
					ST_Boundary(poly_geom) AS line_geom
				FROM 
					nearest_polygon
			)
			SELECT 
				ST_AsGeoJSON(ST_ClosestPoint(line_geom.line_geom, input.geom)) AS geojson,
				ST_Distance(input.geom, ST_ClosestPoint(line_geom.line_geom, input.geom))::float AS distance
			FROM 
				input_point AS input, line_geom;
			`, jsonData.Point[0], jsonData.Point[1], jsonData.Layer, jsonData.Layer, jsonData.Layer, jsonData.Layer)
				case "line":
					sql = fmt.Sprintf(`
			WITH input_point AS (
				SELECT ST_SetSRID(ST_MakePoint(%f, %f), 4326) AS geom
			),
			closest_line AS (
				SELECT 
					%s.geom AS line_geom
				FROM 
					%s, input_point AS input
				WHERE 
					ST_Distance(input.geom, %s.geom) < 0.0005  
				ORDER BY 
					ST_Distance(input.geom, %s.geom) 
				LIMIT 1
			)
			SELECT 
				ST_AsGeoJSON(ST_ClosestPoint(closest_line.line_geom, input.geom)) AS geojson,
				ST_Distance(input.geom, ST_ClosestPoint(closest_line.line_geom, input.geom))::float AS distance
			FROM 
				input_point AS input, closest_line;
			`, jsonData.Point[0], jsonData.Point[1], jsonData.Layer, jsonData.Layer, jsonData.Layer, jsonData.Layer)
				case "point":
					sql = fmt.Sprintf(`
			WITH input_point AS (
				SELECT ST_SetSRID(ST_MakePoint(%f, %f), 4326) AS geom
			),
			nearest_point AS (
				SELECT 
					%s.geom AS nearest_geom
				FROM 
					%s, input_point AS input
				ORDER BY 
					ST_Distance(input.geom, %s.geom) 
				LIMIT 1
			)
			SELECT 
				ST_AsGeoJSON(nearest_geom) AS geojson,
				ST_Distance(input.geom, nearest_geom)::float AS distance
			FROM 
				input_point AS input, nearest_point;
			`, jsonData.Point[0], jsonData.Point[1], jsonData.Layer, jsonData.Layer, jsonData.Layer)
				}
			}
			err := DB.Raw(sql).Scan(&geomData)
			if err.Error != nil {
				c.String(http.StatusBadRequest, "err")
				return
			}
			json.Unmarshal(geomData.GeoJSON, &feature.Geometry)
			if geomData.Distance <= 0.00015 {
				c.JSON(http.StatusOK, feature.Geometry.Coordinates)
			} else {
				c.String(http.StatusBadRequest, "err")
			}
		}

	} else {
		var sql string
		if len(Templarers) != 0 && jsonData.Layer == "" {
			geojsons := GetTempLayers(Templarers)
			//点线面分离
			var PolygonJson []*geojson.Feature
			var LineJson []*geojson.Feature
			var PointJson []*geojson.Feature
			for _, item := range geojsons {
				switch item.Geometry.GeoJSONType() {
				case "Polygon":
					PolygonJson = append(PolygonJson, item)
				case "MultiPolygon":
					PolygonJson = append(PolygonJson, item)
				case "LineString":
					LineJson = append(LineJson, item)
				case "Point":
					PointJson = append(PointJson, item)
				}
			}
			//面数据捕捉
			if len(PolygonJson) != 0 {
				geo := Transformer.GetFeatureString(PolygonJson)
				sql = fmt.Sprintf(`
						WITH input_point AS (
							SELECT ST_SetSRID(ST_MakePoint(%f, %f), 4326) AS geom
						),
						geojson_data AS (
							SELECT 
								jsonb_array_elements('%s'::jsonb) AS feature
						),
						nearest_polygon AS (
							SELECT 
								ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326) AS poly_geom
							FROM 
								geojson_data, input_point AS input
							WHERE 
								ST_Distance(input.geom, ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326)) < 0.0005 
							ORDER BY 
								ST_Distance(input.geom, ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326)) 
							LIMIT 1
						),
						line_geom AS (
							SELECT 
								ST_Boundary(poly_geom) AS line_geom
							FROM 
								nearest_polygon
						),
						points_geom AS ( 
							SELECT 
								ST_Collect(geom) AS point_geom 
							FROM (
								SELECT (ST_DumpPoints(line_geom)).geom AS geom -- 从 line_geom 中打散点
								FROM line_geom -- 从 line_geom 查询
							) AS dumped_points -- 中间结果作为子查询
						),
						closest_line AS (
							SELECT ST_ClosestPoint(line_geom.line_geom, input.geom) AS closest_point
							FROM input_point AS input, line_geom
						),
						closest_points AS (
							SELECT ST_ClosestPoint(points_geom.point_geom, input.geom) AS closest_point
							FROM input_point AS input, points_geom
						),
						distance_a AS (
							SELECT ST_Distance(input.geom, closest_line.closest_point) AS distance_a
							FROM input_point AS input, closest_line
						),
						distance_b AS (
							SELECT ST_Distance(input.geom, closest_points.closest_point) AS distance_b
							FROM input_point AS input, closest_points
						)
						SELECT 
							CASE 
								WHEN distance_b.distance_b < 0.000005 THEN ST_AsGeoJSON(closest_points.closest_point)
								ELSE ST_AsGeoJSON(closest_line.closest_point)
							END AS geojson,
							CASE 
								WHEN distance_b.distance_b < 0.000005 THEN distance_b.distance_b::float
								ELSE distance_a.distance_a::float
							END AS distance
						FROM distance_a, distance_b, closest_line, closest_points;
			`, jsonData.Point[0], jsonData.Point[1], geo)
			}
			//线数据捕捉
			if len(LineJson) != 0 {
				geo := Transformer.GetFeatureString(LineJson)
				sql = fmt.Sprintf(`
						WITH input_point AS (
							SELECT ST_SetSRID(ST_MakePoint(%f, %f), 4326) AS geom
						),
						geojson_data AS (
							SELECT 
								jsonb_array_elements('%s'::jsonb) AS feature
						),
						nearest_polygon AS (
							SELECT 
								ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326) AS poly_geom
							FROM 
								geojson_data, input_point AS input
							WHERE 
								ST_Distance(input.geom, ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326)) < 0.0005 
							ORDER BY 
								ST_Distance(input.geom, ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326)) 
							LIMIT 1
						),
						line_geom AS (
							SELECT 
								poly_geom AS line_geom
							FROM 
								nearest_polygon
						),
						points_geom AS ( 
							SELECT 
								ST_Collect(geom) AS point_geom 
							FROM (
								SELECT (ST_DumpPoints(line_geom)).geom AS geom -- 从 line_geom 中打散点
								FROM line_geom -- 从 line_geom 查询
							) AS dumped_points -- 中间结果作为子查询
						),
						closest_line AS (
							SELECT ST_ClosestPoint(line_geom.line_geom, input.geom) AS closest_point
							FROM input_point AS input, line_geom
						),
						closest_points AS (
							SELECT ST_ClosestPoint(points_geom.point_geom, input.geom) AS closest_point
							FROM input_point AS input, points_geom
						),
						distance_a AS (
							SELECT ST_Distance(input.geom, closest_line.closest_point) AS distance_a
							FROM input_point AS input, closest_line
						),
						distance_b AS (
							SELECT ST_Distance(input.geom, closest_points.closest_point) AS distance_b
							FROM input_point AS input, closest_points
						)
						SELECT 
							CASE 
								WHEN distance_b.distance_b < 0.000005 THEN ST_AsGeoJSON(closest_points.closest_point)
								ELSE ST_AsGeoJSON(closest_line.closest_point)
							END AS geojson,
							CASE 
								WHEN distance_b.distance_b < 0.000005 THEN distance_b.distance_b::float
								ELSE distance_a.distance_a::float
							END AS distance
						FROM distance_a, distance_b, closest_line, closest_points;
			`, jsonData.Point[0], jsonData.Point[1], geo)
			}
			//点数据捕捉
			if len(PointJson) != 0 {
				geo := Transformer.GetFeatureString(PointJson)
				sql = fmt.Sprintf(`
				WITH input_point AS (
					SELECT ST_SetSRID(ST_MakePoint(%f, %f), 4326) AS geom
				),
				geojson_data AS (
					SELECT 
						jsonb_array_elements('%s'::jsonb) AS feature
				),
				nearest_point AS (
					SELECT 
						ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326) AS point_geom
					FROM 
						geojson_data, input_point AS input
					WHERE 
						ST_Distance(input.geom, ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326)) < 0.0005 
					ORDER BY 
						ST_Distance(input.geom, ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326)) 
					LIMIT 1
				)
				SELECT 
					ST_AsGeoJSON(point_geom) AS geojson,
					ST_Distance(input.geom, point_geom)::float AS distance
				FROM 
					input_point AS input, nearest_point;
			`, jsonData.Point[0], jsonData.Point[1], geo)
			}
		} else if len(Templarers) == 0 && jsonData.Layer != "" {
			layer := jsonData.Layer
			var schema []models.MySchema
			DB.Where("en = ?", layer).Find(&schema)
			if len(schema) >= 1 {
				switch schema[0].Type {
				case "polygon":
					sql = fmt.Sprintf(`
						WITH input_point AS (
							SELECT ST_SetSRID(ST_MakePoint(%f, %f), 4326) AS geom
						),
						nearest_polygon AS (
							SELECT %s.geom AS poly_geom
							FROM %s, input_point AS input
							WHERE ST_Distance(input.geom, %s.geom) < 0.0005
							ORDER BY ST_Distance(input.geom, %s.geom)
							LIMIT 1
						),
						line_geom AS (
							SELECT ST_Boundary(poly_geom) AS line_geom
							FROM nearest_polygon
						),
						points_geom AS ( -- 定义一个公共表表达式（CTE）名为 points_geom
							SELECT 
								ST_Collect(geom) AS point_geom -- 聚合点为一个几何体
							FROM (
								SELECT (ST_DumpPoints(line_geom)).geom AS geom -- 从 line_geom 中打散点
								FROM line_geom -- 从 line_geom 查询
							) AS dumped_points -- 中间结果作为子查询
						),
						closest_line AS (
							SELECT ST_ClosestPoint(line_geom.line_geom, input.geom) AS closest_point
							FROM input_point AS input, line_geom
						),
						closest_points AS (
							SELECT ST_ClosestPoint(points_geom.point_geom, input.geom) AS closest_point
							FROM input_point AS input, points_geom
						),
						distance_a AS (
							SELECT ST_Distance(input.geom, closest_line.closest_point) AS distance_a
							FROM input_point AS input, closest_line
						),
						distance_b AS (
							SELECT ST_Distance(input.geom, closest_points.closest_point) AS distance_b
							FROM input_point AS input, closest_points
						)
						SELECT 
							CASE 
								WHEN distance_b.distance_b < 0.000005 THEN ST_AsGeoJSON(closest_points.closest_point)
								ELSE ST_AsGeoJSON(closest_line.closest_point)
							END AS geojson,
							CASE 
								WHEN distance_b.distance_b < 0.000005 THEN distance_b.distance_b::float
								ELSE distance_a.distance_a::float
							END AS distance
						FROM distance_a, distance_b, closest_line, closest_points;
`, jsonData.Point[0], jsonData.Point[1], jsonData.Layer, jsonData.Layer, jsonData.Layer, jsonData.Layer)
				case "line":
					sql = fmt.Sprintf(`
						WITH input_point AS (
							SELECT ST_SetSRID(ST_MakePoint(%f, %f), 4326) AS geom
						),
						nearest_polygon AS (
							SELECT %s.geom AS poly_geom
							FROM %s, input_point AS input
							WHERE ST_Distance(input.geom, %s.geom) < 0.0005
							ORDER BY ST_Distance(input.geom, %s.geom)
							LIMIT 1
						),
						line_geom AS (
							SELECT poly_geom AS line_geom
							FROM nearest_polygon
						),
						points_geom AS (
							SELECT 
								ST_Collect(geom) AS point_geom -- 聚合点为一个几何体
							FROM (
								SELECT (ST_DumpPoints(line_geom)).geom AS geom -- 从 line_geom 中打散点
								FROM line_geom -- 从 line_geom 查询
							) AS dumped_points -- 中间结果作为子查询
						),
						closest_line AS (
							SELECT ST_ClosestPoint(line_geom.line_geom, input.geom) AS closest_point
							FROM input_point AS input, line_geom
						),
						closest_points AS (
							SELECT ST_ClosestPoint(points_geom.point_geom, input.geom) AS closest_point
							FROM input_point AS input, points_geom
						),
						distance_a AS (
							SELECT ST_Distance(input.geom, closest_line.closest_point) AS distance_a
							FROM input_point AS input, closest_line
						),
						distance_b AS (
							SELECT ST_Distance(input.geom, closest_points.closest_point) AS distance_b
							FROM input_point AS input, closest_points
						)
						SELECT 
							CASE 
								WHEN distance_b.distance_b < 0.000005 THEN ST_AsGeoJSON(closest_points.closest_point)
								ELSE ST_AsGeoJSON(closest_line.closest_point)
							END AS geojson,
							CASE 
								WHEN distance_b.distance_b < 0.000005 THEN distance_b.distance_b::float
								ELSE distance_a.distance_a::float
							END AS distance
						FROM distance_a, distance_b, closest_line, closest_points;
`, jsonData.Point[0], jsonData.Point[1], jsonData.Layer, jsonData.Layer, jsonData.Layer, jsonData.Layer)
				case "point":
					sql = fmt.Sprintf(`
			WITH input_point AS (
				SELECT ST_SetSRID(ST_MakePoint(%f, %f), 4326) AS geom
			),
			nearest_point AS (
				SELECT 
					%s.geom AS nearest_geom
				FROM 
					%s, input_point AS input
				ORDER BY 
					ST_Distance(input.geom, %s.geom) 
				LIMIT 1
			)
			SELECT 
				ST_AsGeoJSON(nearest_geom) AS geojson,
				ST_Distance(input.geom, nearest_geom)::float AS distance
			FROM 
				input_point AS input, nearest_point;
			`, jsonData.Point[0], jsonData.Point[1], jsonData.Layer, jsonData.Layer, jsonData.Layer)
				}
			}
		}
		var geomData CaptureData
		err := DB.Raw(sql).Scan(&geomData)
		if err.Error != nil {
			c.String(http.StatusBadRequest, "err")
			return
		}
		var feature struct {
			Geometry GeometryPoint `json:"geometry"`
		}
		json.Unmarshal(geomData.GeoJSON, &feature.Geometry)

		if geomData.Distance <= 0.00015 {
			c.JSON(http.StatusOK, feature.Geometry.Coordinates)
		} else {
			c.String(http.StatusBadRequest, "err")
		}
	}

}

//自动追踪构面

type AutoData struct {
	Line      geojson.FeatureCollection `json:"Line"`
	Layer     string
	TempLayer []string
}

func (uc *UserController) AutoPolygon(c *gin.Context) {
	var jsonData AutoData
	c.BindJSON(&jsonData)
	DB := models.DB
	var sql string
	line := Transformer.GetGeometryString(jsonData.Line.Features[0])
	Templarers := jsonData.TempLayer
	if len(Templarers) != 0 {
		geojsons := GetTempLayers(Templarers)
		//点线面分离
		var PolygonJson []*geojson.Feature
		var LineJson []*geojson.Feature
		for _, item := range geojsons {
			switch item.Geometry.GeoJSONType() {
			case "Polygon":
				PolygonJson = append(PolygonJson, item)
			case "MultiPolygon":
				PolygonJson = append(PolygonJson, item)
			case "LineString":
				LineJson = append(LineJson, item)
			}
		}
		//面数据捕捉
		if len(PolygonJson) != 0 {
			geo := Transformer.GetFeatureString(PolygonJson)
			sql = fmt.Sprintf(`
					WITH input_linefrist AS (
						SELECT ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326) AS geom
					),
					input_line AS (
						SELECT
						ST_LineMerge(ST_Union( ARRAY[
								ST_MakeLine(ST_StartPoint(geom),ST_Project(ST_StartPoint(geom), 0.000001, ST_Azimuth(ST_PointN(geom,2),ST_StartPoint(geom)))::geometry),
								geom,
								ST_MakeLine(ST_EndPoint(geom),ST_Project(ST_EndPoint(geom), 0.000001, ST_Azimuth(ST_PointN(geom,ST_NumPoints(geom) - 1),ST_EndPoint(geom)))::geometry)
							 ])) AS geom
						FROM input_linefrist
					),
					intersecting_areas AS (
						SELECT ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326) AS geom
						FROM jsonb_array_elements('%s'::jsonb) AS feature
						WHERE ST_Intersects(ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326), (SELECT geom FROM input_line))
					),
					boundary_lines AS (
						SELECT ST_Boundary(geom) AS geom
						FROM intersecting_areas
					),
					closed_geometries AS (
						SELECT ST_Union(ST_Collect((SELECT geom FROM input_line), boundary_lines.geom)) AS lines
						FROM boundary_lines
					),
					newpolygons AS (
						SELECT ST_Polygonize(lines) AS polygon_geoms
						FROM closed_geometries
					),
					boundary_lines2 AS (
						SELECT (ST_Dump(ST_Boundary(polygon_geoms))).geom AS geom 
						FROM newpolygons
					),
					intersecting_lines AS (
						SELECT ST_Intersection(input_line.geom, boundary_lines2.geom,0.0000001) AS geom, boundary_lines2.geom AS boundary_geom
						FROM input_line, boundary_lines2
					),
					max_overlap AS (
						SELECT 
							boundary_geom, 
							ST_Length(geom)/ST_Length(boundary_geom) AS overlap_length
						FROM intersecting_lines
						ORDER BY overlap_length DESC
						LIMIT 1
					)
					SELECT ST_AsGeoJSON(ST_MakePolygon(boundary_geom)) AS geojson,
					overlap_length::float AS lenth
					FROM max_overlap
			`, line, geo)
		}
		if len(LineJson) != 0 {
			geo := Transformer.GetFeatureString(LineJson)
			sql = fmt.Sprintf(`
					WITH input_linefrist AS (
						SELECT ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326) AS geom
					),
					input_line AS (
						SELECT
						ST_LineMerge(ST_Union( ARRAY[
								ST_MakeLine(ST_StartPoint(geom),ST_Project(ST_StartPoint(geom), 0.000001, ST_Azimuth(ST_PointN(geom,2),ST_StartPoint(geom)))::geometry),
								geom,
								ST_MakeLine(ST_EndPoint(geom),ST_Project(ST_EndPoint(geom), 0.000001, ST_Azimuth(ST_PointN(geom,ST_NumPoints(geom) - 1),ST_EndPoint(geom)))::geometry)
							 ])) AS geom
						FROM input_linefrist
					),
					intersecting_areas AS (
						SELECT ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326) AS geom
						FROM jsonb_array_elements('%s'::jsonb) AS feature
						WHERE ST_Intersects(ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326), (SELECT geom FROM input_line))
					),
					boundary_lines AS (
						SELECT geom AS geom
						FROM intersecting_areas
					),
					closed_geometries AS (
						SELECT ST_Union(ST_Collect((SELECT geom FROM input_line), boundary_lines.geom)) AS lines
						FROM boundary_lines
					),
					newpolygons AS (
						SELECT ST_Polygonize(lines) AS polygon_geoms
						FROM closed_geometries
					),
					boundary_lines2 AS (
						SELECT (ST_Dump(ST_Boundary(polygon_geoms))).geom AS geom 
						FROM newpolygons
					),
					intersecting_lines AS (
						SELECT ST_Intersection(input_line.geom, boundary_lines2.geom,0.0000001) AS geom, boundary_lines2.geom AS boundary_geom
						FROM input_line, boundary_lines2
					),
					max_overlap AS (
						SELECT 
							boundary_geom, 
							ST_Length(geom)/ST_Length(boundary_geom) AS overlap_length
						FROM intersecting_lines
						ORDER BY overlap_length DESC
						LIMIT 1
					)
					SELECT ST_AsGeoJSON(ST_MakePolygon(boundary_geom)) AS geojson,
					overlap_length::float AS lenth
					FROM max_overlap
			`, line, geo)
		}
	} else {
		layer := jsonData.Layer
		var schema []models.MySchema
		DB.Where("en = ?", layer).Find(&schema)
		if len(schema) >= 1 {
			switch schema[0].Type {
			case "polygon":
				sql = fmt.Sprintf(`
					WITH input_linefrist AS (
						SELECT ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326) AS geom
					),
					input_line AS (
						SELECT
						ST_LineMerge(ST_Union( ARRAY[
								ST_MakeLine(ST_StartPoint(geom),ST_Project(ST_StartPoint(geom), 0.000001, ST_Azimuth(ST_PointN(geom,2),ST_StartPoint(geom)))::geometry),
								geom,
								ST_MakeLine(ST_EndPoint(geom),ST_Project(ST_EndPoint(geom), 0.000001, ST_Azimuth(ST_PointN(geom,ST_NumPoints(geom) - 1),ST_EndPoint(geom)))::geometry)
							 ])) AS geom
						FROM input_linefrist
					),
					intersecting_areas AS (
						SELECT *
						FROM %s
						WHERE ST_Intersects(%s.geom, (SELECT geom FROM input_line))
					),
					boundary_lines AS (
						SELECT ST_Boundary(geom) AS geom
						FROM intersecting_areas
					),
					closed_geometries AS (
						SELECT ST_Union(ST_Collect((SELECT geom FROM input_line), boundary_lines.geom)) AS lines
						FROM boundary_lines
					),
					newpolygons AS (
						SELECT ST_Polygonize(lines) AS polygon_geoms
						FROM closed_geometries
					),
					boundary_lines2 AS (
						SELECT (ST_Dump(ST_Boundary(polygon_geoms))).geom AS geom 
						FROM newpolygons
					),
					intersecting_lines AS (
						SELECT ST_Intersection(input_line.geom, boundary_lines2.geom,0.0000001) AS geom, boundary_lines2.geom AS boundary_geom
						FROM input_line, boundary_lines2
					),
					max_overlap AS (
						SELECT 
							boundary_geom, 
							ST_Length(geom)/ST_Length(boundary_geom) AS overlap_length
						FROM intersecting_lines
						ORDER BY overlap_length DESC
						LIMIT 1
					)
					SELECT ST_AsGeoJSON(ST_MakePolygon(boundary_geom)) AS geojson,
					overlap_length::float AS lenth
					FROM max_overlap
				`, line, jsonData.Layer, jsonData.Layer)
			case "line":
				sql = fmt.Sprintf(`
					WITH input_linefrist AS (
						SELECT ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326) AS geom
					),
					input_line AS (
						SELECT
						ST_LineMerge(ST_Union( ARRAY[
								ST_MakeLine(ST_StartPoint(geom),ST_Project(ST_StartPoint(geom), 0.000001, ST_Azimuth(ST_PointN(geom,2),ST_StartPoint(geom)))::geometry),
								geom,
								ST_MakeLine(ST_EndPoint(geom),ST_Project(ST_EndPoint(geom), 0.000001, ST_Azimuth(ST_PointN(geom,ST_NumPoints(geom) - 1),ST_EndPoint(geom)))::geometry)
							 ])) AS geom
						FROM input_linefrist
					),
					intersecting_areas AS (
						SELECT *
						FROM %s
						WHERE ST_Intersects(%s.geom, (SELECT geom FROM input_line))
					),
					boundary_lines AS (
						SELECT geom AS geom
						FROM intersecting_areas
					),
					closed_geometries AS (
						SELECT ST_Union(ST_Collect((SELECT geom FROM input_line), boundary_lines.geom)) AS lines
						FROM boundary_lines
					),
					newpolygons AS (
						SELECT ST_Polygonize(lines) AS polygon_geoms
						FROM closed_geometries
					),
					boundary_lines2 AS (
						SELECT (ST_Dump(ST_Boundary(polygon_geoms))).geom AS geom 
						FROM newpolygons
					),
					intersecting_lines AS (
						SELECT ST_Intersection(input_line.geom, boundary_lines2.geom,0.0000001) AS geom, boundary_lines2.geom AS boundary_geom
						FROM input_line, boundary_lines2
						WHERE ST_Intersects(input_line.geom, boundary_lines2.geom)
					),
					max_overlap AS (
						SELECT 
							boundary_geom, 
							ST_Length(geom)/ST_Length(boundary_geom) AS overlap_length
						FROM intersecting_lines
						ORDER BY overlap_length DESC
						LIMIT 1
					)
					SELECT ST_AsGeoJSON(ST_MakePolygon(boundary_geom)) AS geojson,
					overlap_length::float AS lenth
					FROM max_overlap
			`, line, jsonData.Layer, jsonData.Layer)
			}
		}
	}

	var geomData Transformer.GeometryData
	err := DB.Raw(sql).Scan(&geomData)

	if err.Error != nil {
		c.String(http.StatusBadRequest, "err")
		return
	}

	var feature struct {
		Geometry   map[string]interface{} `json:"geometry"`
		Properties map[string]interface{} `json:"properties"`
		Type       string                 `json:"type"`
	}
	feature.Type = "Feature"
	json.Unmarshal(geomData.GeoJSON, &feature.Geometry)
	feature.Properties = make(map[string]interface{})
	feature.Properties["name"] = ""
	var NewGeo geojson.FeatureCollection
	data2, _ := json.Marshal(feature)
	var myfeature *geojson.Feature
	aa := json.Unmarshal(data2, &myfeature)
	if aa != nil {
		fmt.Println(aa.Error())
	}
	NewGeo.Features = append(NewGeo.Features, myfeature)
	c.JSON(http.StatusOK, NewGeo)
}

func (uc *UserController) GetDeviceName(c *gin.Context) {
	c.String(http.StatusOK, config.DeviceName)
}

func (uc *UserController) OutMVT(c *gin.Context) {
	dbname := strings.ToLower(c.Param("tablename"))
	x, _ := strconv.Atoi(c.Param("x"))
	y, _ := strconv.Atoi(strings.TrimSuffix(c.Param("y.pbf"), ".pbf"))
	z, _ := strconv.Atoi(c.Param("z"))
	DB := models.DB
	mvtdata := pgmvt.MakeMvtNew(x, y, z, dbname, DB)
	if mvtdata != nil {
		c.Data(http.StatusOK, "application/x-protobuf", mvtdata)
	} else {
		c.String(http.StatusOK, "err")
	}
}

// 获取图层的范围 - 返回GeoJSON格式
func (uc *UserController) GetLayerExtent(c *gin.Context) {
	layername := strings.ToLower(c.Query("tablename"))

	// 参数验证
	if layername == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "tablename参数不能为空",
		})
		return
	}

	// 安全性检查 - 防止SQL注入
	if !isValidTableName(layername) {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "无效的表名",
		})
		return
	}

	// 获取采样比例参数，默认10%
	samplePercent := c.DefaultQuery("sample", "10")

	// 构建高性能SQL查询 - 直接返回GeoJSON
	sql := fmt.Sprintf(`
		SELECT ST_AsGeoJSON(
			ST_Envelope(
				ST_Collect(ST_Envelope(geom))
			)
		) as bbox_geojson,
		ST_XMin(ST_Collect(ST_Envelope(geom))) as min_x,
		ST_YMin(ST_Collect(ST_Envelope(geom))) as min_y,
		ST_XMax(ST_Collect(ST_Envelope(geom))) as max_x,
		ST_YMax(ST_Collect(ST_Envelope(geom))) as max_y
		FROM %s 
		TABLESAMPLE SYSTEM(%s)
		WHERE geom IS NOT NULL
	`, layername, samplePercent)

	// 执行查询
	var result struct {
		BboxGeoJSON string  `gorm:"column:bbox_geojson"`
		MinX        float64 `gorm:"column:min_x"`
		MinY        float64 `gorm:"column:min_y"`
		MaxX        float64 `gorm:"column:max_x"`
		MaxY        float64 `gorm:"column:max_y"`
	}

	// 设置查询超时
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	DB := models.DB
	// 使用原生SQL执行，避免GORM开销
	err := DB.WithContext(ctx).Raw(sql).Scan(&result).Error
	if err != nil {
		log.Printf("获取图层范围失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "查询失败",
			"error":   err.Error(),
		})
		return
	}

	// 检查结果有效性
	if result.BboxGeoJSON == "" || (result.MinX == 0 && result.MinY == 0 && result.MaxX == 0 && result.MaxY == 0) {
		c.JSON(http.StatusNotFound, gin.H{
			"code":    404,
			"message": "表不存在或无几何数据",
		})
		return
	}

	// 解析GeoJSON字符串为JSON对象
	var geoJSON map[string]interface{}
	if err := json.Unmarshal([]byte(result.BboxGeoJSON), &geoJSON); err != nil {
		log.Printf("解析GeoJSON失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "GeoJSON解析失败",
		})
		return
	}

	// 返回结果
	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "success",
		"data": gin.H{
			"tablename": layername,
			"extent": gin.H{
				"minX": result.MinX,
				"minY": result.MinY,
				"maxX": result.MaxX,
				"maxY": result.MaxY,
			},
			"bbox":    []float64{result.MinX, result.MinY, result.MaxX, result.MaxY},
			"geojson": geoJSON,
		},
	})
}

// 表名安全验证函数
func isValidTableName(tablename string) bool {
	// 只允许字母、数字和下划线，长度限制
	matched, _ := regexp.MatchString(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`, tablename)
	return matched
}
