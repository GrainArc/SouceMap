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

				TempAttr["geom"] = clause.Expr{SQL: "ST_GeomFromWKB(decode(?, 'hex'))", Vars: []interface{}{wkb_result}}
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

func UpdateGeojsonToTable(db *gorm.DB, jsonData geojson.FeatureCollection, tablename string, id int32) {

	for _, t := range jsonData.Features {
		wkb_result := GeoJsonToWKB(*t)
		TempAttr := make(map[string]interface{})
		for key, value := range t.Properties {
			TempAttr[strings.ToLower(key)] = value
		}

		TempAttr["geom"] = clause.Expr{SQL: "ST_GeomFromWKB(decode(?, 'hex'))", Vars: []interface{}{wkb_result}}
		// 使用gorm插入数据
		if err := db.Table(tablename).Where("id = ?", id).Updates(TempAttr).Error; err != nil {
			log.Printf("Failed to insert feature: %v, error: %v", t, err)
		}
	}
}
