package methods

import (
	"encoding/hex"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/paulmach/orb/geojson"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"log"
	"strings"
	"sync"
	"unicode/utf8"
)

func MakeGeoJSON2(items []map[string]interface{}) interface{} {
	var FeaturesList []*geojson.Feature
	for _, item := range items {
		geomStr := item["geom"].(string)
		wkbBytes, _ := hex.DecodeString(strings.Trim(geomStr, "  "))
		geom, _ := wkb.Unmarshal(wkbBytes)
		feature := geojson.NewFeature(geom)
		properties := make(map[string]interface{})
		for key, value := range item {
			if key != "geom" {
				properties[key] = value
			}
		}
		feature.Properties = properties
		FeaturesList = append(FeaturesList, feature)
	}
	features := geojson.NewFeatureCollection()
	features.Features = FeaturesList
	return features
}

func GeoJsonToWKB(geo geojson.Feature) string {
	//  检查几何类型是否为  Polygon，如果是，则转换为  MultiPolygon
	if polygon, ok := geo.Geometry.(orb.Polygon); ok {
		geo.Geometry = orb.MultiPolygon{polygon}
	}

	TempWkb, _ := wkb.Marshal(geo.Geometry)
	WkbHex := hex.EncodeToString(TempWkb)
	return WkbHex
}

func SavaGeojsonToDb(db *gorm.DB, jsonData geojson.FeatureCollection, table interface{}) {
	for _, t := range jsonData.Features {
		wkb_result := GeoJsonToWKB(*t)
		TempAttr := make(map[string]interface{})
		for key, value := range t.Properties {
			if key != "id" {
				TempAttr[key] = value
			}
		}
		TempAttr["geom"] = clause.Expr{SQL: "ST_GeomFromWKB(decode(?, 'hex'))", Vars: []interface{}{wkb_result}}
		db.Model(table).Create(TempAttr)
	}
}

func SavaGeojsonToTable(db *gorm.DB, jsonData geojson.FeatureCollection, tablename string) {
	const workerCount = 8 // 并发数为8

	features := jsonData.Features
	featureCount := len(features)

	if featureCount == 0 {
		return
	}

	// 创建通道和等待组
	featureChan := make(chan *geojson.Feature, workerCount)
	var wg sync.WaitGroup

	// 启动8个工作协程
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// 每个协程处理分配给它的feature
			for t := range featureChan {
				wkb_result := GeoJsonToWKB(*t)
				TempAttr := make(map[string]interface{})
				for key, value := range t.Properties {
					rawName, ok := value.(string)
					if ok == true {
						// 清理空字符和其他非法字符
						cleanedName := strings.Map(func(r rune) rune {
							if r == 0x00 || !utf8.ValidRune(r) {
								return -1 // 移除非法字符
							}
							return r
						}, rawName)

						// 检查并修复 UTF-8 编码
						if !utf8.ValidString(cleanedName) {
							validName := string([]rune(cleanedName)) // 强制转换为 UTF-8
							log.Printf("检测到非 UTF-8 编码字符串，已修复: %s -> %s", cleanedName, validName)
							cleanedName = validName
						}

						value = cleanedName
					}

					TempAttr[strings.ToLower(key)] = value
				}

				TempAttr["geom"] = clause.Expr{
					SQL:  "ST_MakeValid(ST_SetSRID(ST_GeomFromWKB(decode(?, 'hex')), ?))",
					Vars: []interface{}{wkb_result, 4326}, // 4326 是 WGS84 坐标系，根据实际情况修改
				}
				// 使用gorm插入数据
				if err := db.Table(tablename).Create(TempAttr).Error; err != nil {
					log.Printf("Failed to insert feature: %v, error: %v", t, err)
				}
			}
		}()
	}

	// 将所有features发送到通道
	go func() {
		defer close(featureChan)
		for _, feature := range features {
			featureChan <- feature
		}
	}()

	// 等待所有协程完成
	wg.Wait()
}

// CloseRing 确保环是闭合的（首尾点相同）
func CloseRing(ring orb.Ring) orb.Ring {
	if len(ring) < 2 {
		return ring
	}
	// 检查首尾点是否相同
	if ring[0] != ring[len(ring)-1] {
		// 添加首点到末尾以闭合环
		ring = append(ring, ring[0])
	}
	return ring
}

// FixPolygon 修复多边形，确保所有环都是闭合的
func FixPolygon(polygon orb.Polygon) orb.Polygon {
	fixed := make(orb.Polygon, len(polygon))
	for i, ring := range polygon {
		fixed[i] = CloseRing(ring)
	}
	return fixed
}

// FixMultiPolygon 修复多多边形
func FixMultiPolygon(mp orb.MultiPolygon) orb.MultiPolygon {
	fixed := make(orb.MultiPolygon, len(mp))
	for i, polygon := range mp {
		fixed[i] = FixPolygon(polygon)
	}
	return fixed
}

// FixGeometry 修复几何图形，确保多边形环是闭合的
func FixGeometry(geom orb.Geometry) orb.Geometry {
	if geom == nil {
		return nil
	}

	switch g := geom.(type) {
	case orb.Polygon:
		return FixPolygon(g)
	case orb.MultiPolygon:
		return FixMultiPolygon(g)
	default:
		// 其他类型（Point, LineString等）不需要修复
		return geom
	}
}

// FixFeature 修复Feature中的几何图形
func FixFeature(f *geojson.Feature) *geojson.Feature {
	if f == nil || f.Geometry == nil {
		return f
	}
	f.Geometry = FixGeometry(f.Geometry)
	return f
}

func UpdateGeojsonToTable(db *gorm.DB, jsonData geojson.FeatureCollection, tablename string, id int32) {

	for _, t := range jsonData.Features {
		// 先修复几何图形
		fixedFeature := FixFeature(t)

		wkb_result := GeoJsonToWKB(*fixedFeature)
		TempAttr := make(map[string]interface{})
		for key, value := range fixedFeature.Properties {
			TempAttr[strings.ToLower(key)] = value
		}

		// 使用 ST_SetSRID 设置坐标系
		TempAttr["geom"] = clause.Expr{
			SQL:  "ST_SetSRID(ST_MakeValid(ST_GeomFromWKB(decode(?, 'hex'))), ?)",
			Vars: []interface{}{wkb_result, 4326},
		}

		// 使用gorm更新数据
		if err := db.Table(tablename).Where("id = ?", id).Updates(TempAttr).Error; err != nil {
			log.Printf("Failed to update feature: %v, error: %v", t, err)
		}
	}
}
