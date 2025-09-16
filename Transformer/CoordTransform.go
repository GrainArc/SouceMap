package Transformer

import (
	"encoding/json"
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"math"
	"strings"
)

type ConvertedPoint struct {
	Lat float64
	Lng float64
}

// CoordTransform 函数将EPSG:4523坐标转换为EPSG:4326坐标
func CoordTransformAToB(x float64, y float64, A string, B string) (x1, y1 float64) {
	// 初始化GORM连接
	var point ConvertedPoint
	// 定义SQL查询，使用ST_Transform进行坐标系转换
	sql := fmt.Sprintf("SELECT ST_Y(ST_Transform(ST_SetSRID(ST_Point(?, ?), %s), %s)) AS lat,ST_X(ST_Transform(ST_SetSRID(ST_Point(?, ?), %s), %s)) AS lng", A, B, A, B)
	// 执行查询并扫描结果到我们的结构体中
	db := models.DB
	db.Raw(sql, x, y, x, y).Scan(&point)
	return point.Lng, point.Lat
}

// CoordTransform 函数将EPSG:4523坐标转换为EPSG:4326坐标
func CoordTransform4326To4523(x, y float64) (x1, y1 float64) {
	// 初始化GORM连接
	var point ConvertedPoint
	// 定义SQL查询，使用ST_Transform进行坐标系转换
	sql := fmt.Sprintf("SELECT ST_Y(ST_Transform(ST_SetSRID(ST_Point(?, ?), 4326), 4523)) AS lat,ST_X(ST_Transform(ST_SetSRID(ST_Point(?, ?), 4326), 4523)) AS lng")
	// 执行查询并扫描结果到我们的结构体中
	db := models.DB
	db.Raw(sql, x, y, x, y).Scan(&point)
	return point.Lng, point.Lat
}

func GetGeometryString(original *geojson.Feature) string {
	originalJSON, _ := json.Marshal(original)

	var feature struct {
		Geometry map[string]interface{} `json:"geometry"`
	}
	json.Unmarshal(originalJSON, &feature)
	data, _ := json.Marshal(feature.Geometry)
	return string(data)
}

func GetFeatureString(originals []*geojson.Feature) string {
	var datas []string

	for _, original := range originals {
		var feature struct {
			Geometry map[string]interface{} `json:"geometry"`
		}
		originalJSON, _ := json.Marshal(original)
		json.Unmarshal(originalJSON, &feature)
		aa, _ := json.Marshal(feature)
		datas = append(datas, string(aa))
	}
	data := `[` + strings.Join(datas, ",") + `]`
	return data
}

type GeometryData struct {
	GeoJSON []byte  `gorm:"column:geojson"` // 假设数据库中存储geojson的列名为"geojson"
	Lenth   float64 `gorm:"column:lenth"`
}

// 35带转换
func GeoJsonTransformTo4326(original *geojson.FeatureCollection, EPSG string) (*geojson.FeatureCollection, error) {

	db := models.DB
	// 将FeatureCollection转换为字符串形式的GeoJSON
	for i, item := range original.Features {
		originalJSON := GetGeometryString(item)
		// 假设数据库中的EPSG代码为4523和4326

		sql := fmt.Sprintf(`SELECT ST_AsGeoJSON(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(?), %s), 4326)) as geojson;`, EPSG)
		var geomData GeometryData
		// 执行查询并扫描结果到结构体中
		if err := db.Raw(sql, originalJSON).Scan(&geomData).Error; err == nil {
			//获取feature字符串
			data, _ := json.Marshal(item)
			var feature struct {
				Geometry   map[string]interface{} `json:"geometry"`
				Properties map[string]interface{} `json:"properties"`
				Type       string                 `json:"type"`
			}
			json.Unmarshal(data, &feature)
			json.Unmarshal(geomData.GeoJSON, &feature.Geometry)
			data2, _ := json.Marshal(feature)
			json.Unmarshal(data2, &original.Features[i])
		}
	}

	return original, nil // 转换成功，返回转换后的FeatureCollection
}

// 35带转换
func GeoJsonTransformToCGCS(original *geojson.FeatureCollection) (*geojson.FeatureCollection, error) {
	db := models.DB
	// 将FeatureCollection转换为字符串形式的GeoJSON
	featurex := original.Features[0]
	var cx float64
	switch geom := featurex.Geometry.(type) {
	case orb.Polygon:
		for _, pt := range geom[0] {
			cx = pt[0]
			break
		}
	case orb.MultiPolygon:
		for _, pt := range geom[0][0] {
			cx = pt[0]
			break
		}
	case orb.LineString:
		for _, pt := range geom {
			cx = pt[0]
			break
		}
	case orb.Point:
		cx = geom[0]
	case orb.MultiPoint:
		cx = geom[0][0]
	}
	EPSG := int(4488 + math.Round((cx / 3)))

	for i, item := range original.Features {
		//获取目标坐标系

		originalJSON := GetGeometryString(item)

		// 假设数据库中的EPSG代码为4523和4326
		sql := fmt.Sprintf(`SELECT ST_AsGeoJSON(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(?), 4490), %d)) as geojson;`, EPSG)
		var geomData GeometryData
		// 执行查询并扫描结果到结构体中
		if err := db.Raw(sql, originalJSON).Scan(&geomData).Error; err != nil {
			fmt.Println(err.Error())
			return nil, err // 发生数据库查询错误，返回错误信息
		}
		//获取feature字符串
		data, _ := json.Marshal(item)
		var feature struct {
			Geometry   map[string]interface{} `json:"geometry"`
			Properties map[string]interface{} `json:"properties"`
			Type       string                 `json:"type"`
		}
		json.Unmarshal(data, &feature)
		json.Unmarshal(geomData.GeoJSON, &feature.Geometry)
		data2, _ := json.Marshal(feature)
		json.Unmarshal(data2, &original.Features[i])

	}

	return original, nil // 转换成功，返回转换后的FeatureCollection
}
