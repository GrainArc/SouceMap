// views/helper.go
package views

import (
	"encoding/json"
	"fmt"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"gorm.io/gorm"
)

// buildFeatureCollectionFromMaps 从数据库查询结果构建FeatureCollection
// 用于事务提交后原要素已删除的场景
func buildFeatureCollectionFromMaps(features []map[string]interface{}, tableName string) *geojson.FeatureCollection {
	fc := &geojson.FeatureCollection{}

	for _, feat := range features {
		feature := geojson.NewFeature(nil)

		// 解析geom字段
		if geomVal, ok := feat["geom"]; ok && geomVal != nil {
			switch g := geomVal.(type) {
			case string:
				// WKB hex string - 需要通过数据库转换
				// 这里简化处理，实际场景中geom通常已经是解析后的
			case []byte:
				// 尝试解析为GeoJSON
				var geometry orb.Geometry
				geojsonGeom := &geojson.Geometry{}
				if err := json.Unmarshal(g, geojsonGeom); err == nil {
					geometry = geojsonGeom.Geometry()
					feature.Geometry = geometry
				}
			default:
				_ = g
			}
		}

		// 设置属性（排除geom）
		for key, val := range feat {
			if key != "geom" {
				feature.Properties[key] = val
			}
		}

		fc.Features = append(fc.Features, feature)
	}

	return fc
}

// GetOldGeoBeforeDelete 在事务中删除前获取原要素GeoJSON
// 用于需要在事务内获取即将被删除的要素数据的场景
func GetOldGeoBeforeDelete(tx *gorm.DB, tableName string, ids []int32) geojson.FeatureCollection {
	var NewGeo geojson.FeatureCollection

	for _, id := range ids {
		sql := fmt.Sprintf(`
			SELECT 
				ST_AsGeoJSON(geom) AS geojson,
				to_jsonb(record) - 'geom' AS properties
			FROM "%s" AS record
			WHERE id = %d;
		`, tableName, id)

		var data outData
		if err := tx.Raw(sql).Scan(&data).Error; err != nil {
			continue
		}

		if data.GeoJson == nil {
			continue
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
		if err := json.Unmarshal(data2, &myfeature); err != nil {
			continue
		}
		NewGeo.Features = append(NewGeo.Features, myfeature)
	}

	return NewGeo
}

// GetOldGeoSingleBeforeDelete 事务内获取单个要素
func GetOldGeoSingleBeforeDelete(tx *gorm.DB, tableName string, id int32) geojson.FeatureCollection {
	return GetOldGeoBeforeDelete(tx, tableName, []int32{id})
}
