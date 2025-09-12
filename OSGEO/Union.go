package OSGEO

import (
	"encoding/json"
	"fmt"
	"github.com/fmecool/Gogeo"
	"github.com/fmecool/SouceMap/models"
	"github.com/paulmach/orb/geojson"
)

func UnionGeo(geo1 *geojson.FeatureCollection, geo2 *geojson.FeatureCollection) (*geojson.FeatureCollection, error) {
	DB := models.DB

	// 将GeoJSON转换为JSON字符串
	geo1JSON, err := json.Marshal(geo1)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal geo1: %v", err)
	}

	geo2JSON, err := json.Marshal(geo2)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal geo2: %v", err)
	}

	// 构建SQL查询，使用PostGIS的ST_UNION函数
	sql := `
		WITH geo1_features AS (
			SELECT ST_GeomFromGeoJSON(feature->>'geometry') as geom
			FROM json_array_elements($1::json->'features') as feature
		),
		geo2_features AS (
			SELECT ST_GeomFromGeoJSON(feature->>'geometry') as geom
			FROM json_array_elements($2::json->'features') as feature
		),
		all_geoms AS (
			SELECT geom FROM geo1_features
			UNION ALL
			SELECT geom FROM geo2_features
		),
		union_result AS (
			SELECT ST_Union(geom) as union_geom
			FROM all_geoms
		)
		SELECT ST_AsGeoJSON(union_geom) as result
		FROM union_result
	`

	var result string
	err = DB.Raw(sql, string(geo1JSON), string(geo2JSON)).Scan(&result).Error
	if err != nil {
		return nil, fmt.Errorf("failed to execute union query: %v", err)
	}

	// 如果结果为空，返回空的FeatureCollection
	if result == "" {
		return &geojson.FeatureCollection{
			Type:     "FeatureCollection",
			Features: []*geojson.Feature{},
		}, nil
	}

	// 解析结果几何体
	var feature struct {
		Geometry   map[string]interface{} `json:"geometry"`
		Properties map[string]interface{} `json:"properties"`
		Type       string                 `json:"type"`
	}
	err = json.Unmarshal([]byte(result), &feature.Geometry)

	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal geometry result: %v", err)
	}
	feature.Type = "Feature"
	data2, _ := json.Marshal(feature)
	var feature2 geojson.Feature
	err = json.Unmarshal(data2, &feature2)
	if err != nil {
		fmt.Println(err.Error())
	}

	// 创建FeatureCollection
	featureCollection := &geojson.FeatureCollection{
		Type:     "FeatureCollection",
		Features: []*geojson.Feature{&feature2},
	}

	return featureCollection, nil
}

func SpatialUnionAnalysis(tableName string, groupFields []string, outputTableName string,
	precisionConfig *Gogeo.GeometryPrecisionConfig, progressCallback Gogeo.ProgressCallback) (*Gogeo.GeosAnalysisResult, error) {
	// 读取输入几何表
	inputReader := Gogeo.MakePGReader(tableName)
	inputLayer, err := inputReader.ReadGeometryTable()
	if err != nil {
		return nil, fmt.Errorf("读取输入表 %s 失败: %v", tableName, err)
	}
	defer inputLayer.Close()

	resultLayer, err := Gogeo.UnionAnalysis(inputLayer, groupFields, outputTableName, precisionConfig, progressCallback)
	if err != nil {
		return nil, fmt.Errorf("执行并行裁剪分析失败: %v", err)
	}

	return resultLayer, nil
}
