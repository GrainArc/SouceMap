package methods

import (
	"encoding/json"
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"github.com/paulmach/orb/geojson"
	"math"
	"sort"
	"strconv"
	"strings"
)

func CalculateArea(jsonData *geojson.Feature) float64 {
	geo := jsonData
	jsonString, _ := json.Marshal(geo)
	var data map[string]interface{}
	json.Unmarshal([]byte(jsonString), &data)
	value := data["geometry"]
	geodata, _ := json.Marshal(value)
	jsonDataStr := string(geodata)
	var area float64
	// 使用参数化查询以避免SQL注入
	DB := models.DB
	// 构建SQL语句
	sql := `
            SELECT ST_Area(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(?), 4326), 4523))
            AS area;`
	// 执行查询
	// 注意，这里是用占位符 ? 以保证参数化查询，并把jsonDataStr 作为参数传给SQL查询
	DB.Raw(sql, jsonDataStr).Scan(&area)
	area = math.Round(area*100) / 100
	return area

}
func CalculateAreaByStr(jsonDataStr string) float64 {

	var area float64
	// 使用参数化查询以避免SQL注入
	DB := models.DB
	// 构建SQL语句
	sql := `
            SELECT ST_Area(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(?), 4326), 4523))
            AS area;`
	// 执行查询
	// 注意，这里是用占位符 ? 以保证参数化查询，并把jsonDataStr 作为参数传给SQL查询
	DB.Raw(sql, jsonDataStr).Scan(&area)
	area = math.Round(area*10000) / 10000
	return area

}

func CalculateGeodesicArea(jsonData *geojson.Feature) float64 {
	geo := jsonData
	jsonString, err := json.Marshal(geo)
	if err != nil {
		return 0.0
	}
	var data map[string]interface{}
	json.Unmarshal([]byte(jsonString), &data)
	value := data["geometry"]
	geodata, err := json.Marshal(value)
	if err != nil {
		return 0.0
	}
	jsonDataStr := string(geodata)
	var area float64
	// 使用参数化查询以避免SQL注入
	DB := models.DB
	// 构建SQL语句
	sql := `
            SELECT ST_Area(ST_SetSRID(ST_GeomFromGeoJSON(?), 4490)::geography)
            AS area;`
	// 执行查询
	// 注意，这里是用占位符 ? 以保证参数化查询，并把jsonDataStr 作为参数传给SQL查询
	DB.Raw(sql, jsonDataStr).Scan(&area)
	area = math.Round(area*1000) / 1000
	return area

}

type Result struct {
	Area          float64 // 假设 area 指的是一个面积，使用 float64 类型来接收
	Dlmc          string  // 假设 dlmc 是一个字符串
	AttributeName string  // 额外的动态字段名，仅在程序内部使用
}

func moveUnoccupiedToEndWithSort(text []Result) {
	sort.SliceStable(text, func(i, j int) bool {
		return text[i].Dlmc != "未占用"
	})
}
func GroupAndSum(data []Result) []Result {
	groupMap := make(map[string]float64)
	for _, item := range data {
		groupMap[item.Dlmc] += item.Area
	}
	var groupedData []Result
	for dlmc, area := range groupMap {
		if area < 1 {
			continue
		}
		groupedData = append(groupedData, Result{
			Area: math.Round(area*100) / 100,
			Dlmc: dlmc,
		})

	}
	moveUnoccupiedToEndWithSort(groupedData)
	return groupedData
}

func GetMaxItem(text []Result) string {
	MaxDL := ""
	if len(text) >= 2 {
		maxAreaIndex := 0
		maxArea := text[0].Area
		for i, result := range text {
			if result.Area > maxArea {
				maxArea = result.Area
				maxAreaIndex = i
			}

		}
		MaxDL = text[maxAreaIndex].Dlmc
	} else if len(text) == 1 {
		MaxDL = text[0].Dlmc
	}
	return MaxDL
}

func DLMCReplace(tablename string) string {
	tablename = strings.ToLower(tablename)
	DB := models.DB
	var data []models.MySchema
	DB.Where("en = ?", tablename).Find(&data)
	if len(data) >= 1 {
		return data[0].CN
	}
	return tablename

}
func getFeatureString(original geojson.FeatureCollection) string {
	var datas []string
	var originals []*geojson.Feature
	for _, item := range original.Features {
		originals = append(originals, item)
	}
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
func mergeMapData(tempResults []map[string]interface{}) []map[string]interface{} {
	if len(tempResults) == 0 {
		return tempResults
	}

	// 首先收集所有可能的字段名
	allFields := make(map[string]struct{})
	for _, m := range tempResults {
		for k := range m {
			allFields[k] = struct{}{}
		}
	}

	// 找出需要合并的字段组
	fieldGroups := make(map[string][]string)
	for field1 := range allFields {
		for field2 := range allFields {
			if field1 != field2 {
				// 检查字段名是否有包含关系
				if strings.Contains(field1, field2) || strings.Contains(field2, field1) {
					// 找出更长的字段名作为主键
					mainField := field1
					if len(field2) > len(field1) {
						mainField = field2
					}
					fieldGroups[mainField] = append(fieldGroups[mainField], field1, field2)
				}
			}
		}
	}

	// 去重每个组中的字段
	for mainField, fields := range fieldGroups {
		uniqueFields := make(map[string]struct{})
		for _, f := range fields {
			uniqueFields[f] = struct{}{}
		}
		var uniqueList []string
		for f := range uniqueFields {
			if f != mainField {
				uniqueList = append(uniqueList, f)
			}
		}
		fieldGroups[mainField] = uniqueList
	}

	mergedResults := make([]map[string]interface{}, 0, len(tempResults))

	for _, m := range tempResults {
		merged := make(map[string]interface{})

		// 复制所有字段到新map
		for k, v := range m {
			merged[k] = v
		}

		// 处理每个需要合并的字段组
		for mainField, subFields := range fieldGroups {
			if _, ok := merged[mainField]; !ok {
				merged[mainField] = ""
			}

			var values []string
			if mainValue, ok := merged[mainField].(string); ok && mainValue != "" {
				values = append(values, mainValue)
			}

			for _, subField := range subFields {
				if subValue, ok := merged[subField].(string); ok && subValue != "" {
					values = append(values, subValue)
					delete(merged, subField)
				}
			}

			if len(values) > 0 {
				merged[mainField] = strings.Join(values, "\n/")
			}
		}

		mergedResults = append(mergedResults, merged)
	}

	return mergedResults
}

func GeoIntersect(jsonData geojson.FeatureCollection, tablename string, att string) []Result {
	tablename = strings.ToLower(tablename)
	att = strings.ToLower(att)
	geo := jsonData.Features[0]
	jsonString, _ := json.Marshal(geo)
	var data map[string]interface{}
	json.Unmarshal([]byte(jsonString), &data)
	value := data["geometry"]
	geodata, _ := json.Marshal(value)
	jsonDataStr := string(geodata)
	var sql string
	DB := models.DB
	var tempResults []map[string]interface{}
	var Area float64
	if strings.Contains(tablename, "dltb") {
		Area = CalculateGeodesicArea(jsonData.Features[0])
		// 将GeoJSON字符串转为几何类型，并设定SRID为4326，同时计算交集的面积，并将结果作为area返回
		if strings.Contains(att, "dlbm") == false && strings.Contains(att, "dlmc") == false {
			sql = fmt.Sprintf("SELECT \"numeric\"(ST_Area(ST_Intersection(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326)), ST_MakeValid(geom))::geography)) AS area, %s AS \"%s\" FROM \"%s\" WHERE ST_Intersects(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326), geom)", jsonDataStr, att, att, tablename, jsonDataStr) // 合并SQL查询
			err := DB.Raw(sql).Scan(&tempResults)
			if err.Error != nil { // 检查是否发生错误
				// 错误处理逻辑
				fmt.Println(err.Error)
				return make([]Result, 0)
			}
		} else {
			if strings.Contains(att, "tkj") == true {
				sql = fmt.Sprintf("SELECT \"numeric\"(ST_Area(ST_Intersection(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326)), ST_MakeValid(geom))::geography)) AS area, kcxs ,%s AS \"%s\" FROM \"%s\" WHERE ST_Intersects(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326), geom)", jsonDataStr, att, att, tablename, jsonDataStr)
			} else {
				sql = fmt.Sprintf("SELECT \"numeric\"(ST_Area(ST_Intersection(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326)), ST_MakeValid(geom))::geography)) AS area, kcxs ,%s AS \"%s\" FROM \"%s\" WHERE ST_Intersects(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326), geom)", jsonDataStr, att, att, tablename, jsonDataStr)
			}

			var KCdata []map[string]interface{}
			err := DB.Raw(sql).Scan(&KCdata)
			if err.Error != nil { // 检查是否发生错误
				// 错误处理逻辑
				fmt.Println(err.Error)
				return make([]Result, 0)
			}
			TKMJ := 0.00
			for _, item := range KCdata {
				tempResult := make(map[string]interface{})
				var kcxsFloat float64
				if strings.Contains(att, "tkj") == true {
					switch v := item["kcxs"].(type) {
					case float64: // 如果Kcxs是float64类型
						kcxsFloat = v // 直接赋值
					case string: // 如果Kcxs是string类型
						if value, err := strconv.ParseFloat(v, 64); err == nil { // 尝试转换为float
							kcxsFloat = value // 转换成功则赋值
						} else {
							continue // 转换失败，跳过当前item
						}
					default: // 如果Kcxs是其他类型
						continue // 跳过当前item
					}
				} else {
					switch v := item["kcxs"].(type) {
					case float64: // 如果Kcxs是float64类型
						kcxsFloat = v // 直接赋值
					case string: // 如果Kcxs是string类型
						if value, err := strconv.ParseFloat(v, 64); err == nil { // 尝试转换为float
							kcxsFloat = value // 转换成功则赋值
						} else {
							continue // 转换失败，跳过当前item
						}
					default: // 如果Kcxs是其他类型
						continue // 跳过当前item
					}
				}

				if kcxsFloat > 0.00 { // 检查Kcxs是否大于0
					Tarea := item["area"].(float64) * kcxsFloat          // 计算Tarea，等于面积与Kcxs的乘积
					TKMJ = TKMJ + Tarea                                  // 将Tarea累加到TKMJ中
					Realarea := item["area"].(float64) * (1 - kcxsFloat) // 计算Realarea，等于面积减去Tarea
					tempResult["area"] = Realarea                        // 将Realarea存入tempResult中
					tempResult[att] = item[att]                          // 将Dlmc存入tempResult中
					tempResults = append(tempResults, tempResult)        // 将tempResult添加到tempResults中
				} else {
					if item[att].(string) == "田坎" || item[att].(string) == "1203" {
						Tarea := item["area"].(float64)
						TKMJ = TKMJ + Tarea
					}
					tempResult["area"] = item["area"]
					tempResult[att] = item[att]
					tempResults = append(tempResults, tempResult)
				}

			}
			if TKMJ > 0.00 {
				tempResult := make(map[string]interface{})
				tempResult["area"] = TKMJ
				if strings.Contains(att, "dlbm") == true {
					tempResult[att] = "1203"
				} else {
					tempResult[att] = "田坎"
				}

				tempResults = append(tempResults, tempResult)
			}

		}

	} else if strings.Contains(tablename, "yjjbnt") {

		if att == "" {
			Area = CalculateGeodesicArea(jsonData.Features[0])
			att = "dlmc"
			sql = fmt.Sprintf("SELECT \"numeric\"(ST_Area(ST_Intersection(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326)), ST_MakeValid(geom))::geography)) AS area, kcxs  AS \"%s\" FROM \"%s\" WHERE ST_Intersects(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326), geom)", jsonDataStr, "dlmc", tablename, jsonDataStr) // 合并SQL查询

			var KCdata []map[string]interface{}
			err := DB.Raw(sql).Scan(&KCdata)

			if err.Error != nil { // 检查是否发生错误
				// 错误处理逻辑
				fmt.Println(err.Error)
				return make([]Result, 0)
			}
			TKMJ := 0.00
			for _, item := range KCdata {
				tempResult := make(map[string]interface{})
				var kcxsFloat float64
				switch v := item["dlmc"].(type) {
				case float64: // 如果Kcxs是float64类型
					kcxsFloat = v // 直接赋值
				case string: // 如果Kcxs是string类型
					if value, err := strconv.ParseFloat(v, 64); err == nil { // 尝试转换为float
						kcxsFloat = value // 转换成功则赋值
					} else {
						continue // 转换失败，跳过当前item
					}
				default: // 如果Kcxs是其他类型
					continue // 跳过当前item
				}

				if kcxsFloat > 0.00 {
					Tarea := item["area"].(float64) * kcxsFloat
					TKMJ = TKMJ + Tarea
					Realarea := item["area"].(float64) * (1 - kcxsFloat)
					tempResult["area"] = Realarea
					tempResult["dlmc"] = tablename
					tempResults = append(tempResults, tempResult)

				} else {
					tempResult["area"] = item["area"].(float64)
					tempResult["dlmc"] = tablename
					tempResults = append(tempResults, tempResult)
				}

			}
			if TKMJ > 0.00 {
				tempResult := make(map[string]interface{})
				tempResult["area"] = TKMJ
				tempResult["dlmc"] = "田坎"
				tempResults = append(tempResults, tempResult)
			}
		} else {
			Area = CalculateArea(jsonData.Features[0])
			attrSelect := fmt.Sprintf("\"numeric\"(ST_Area(St_Transform(ST_Intersection(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326)), ST_MakeValid(geom)),4523))) AS area, %s AS \""+att+"\"", jsonDataStr, att)
			sql = fmt.Sprintf("SELECT %s FROM \"%s\" WHERE ST_Intersects(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326), geom)", attrSelect, tablename, jsonDataStr)
			// 将GeoJSON字符串转为几何类型，并设定SRID为4326，同时计算交集的面积，并将结果作为area返回
			err := DB.Raw(sql).Scan(&tempResults)
			if err.Error != nil { // 检查是否发生错误
				// 错误处理逻辑
				fmt.Println(err.Error)
				return make([]Result, 0)
			}
		}

	} else {
		//获取图形总面积
		Area = CalculateArea(jsonData.Features[0])
		// 动态构建查询字符串
		if att != "" {
			attrSelect := fmt.Sprintf("\"numeric\"(ST_Area(St_Transform(ST_Intersection(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326)), ST_MakeValid(geom)),4523))) AS area, %s AS \""+att+"\"", jsonDataStr, att)
			sql = fmt.Sprintf("SELECT %s FROM \"%s\" WHERE ST_Intersects(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326), geom)", attrSelect, tablename, jsonDataStr)
			// 将GeoJSON字符串转为几何类型，并设定SRID为4326，同时计算交集的面积，并将结果作为area返回
			err := DB.Raw(sql).Scan(&tempResults)
			if err.Error != nil { // 检查是否发生错误
				// 错误处理逻辑
				fmt.Println(err.Error)
				return make([]Result, 0)
			}

		} else {
			att = "aa"
			attrSelect := fmt.Sprintf("\"numeric\"(ST_Area(St_Transform(ST_Intersection(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326)), ST_MakeValid(geom)),4523))) AS area", jsonDataStr)
			sql = fmt.Sprintf("SELECT %s FROM \"%s\" WHERE ST_Intersects(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326), geom)", attrSelect, tablename, jsonDataStr)
			err := DB.Raw(sql).Scan(&tempResults)
			if err.Error != nil { // 检查是否发生错误
				// 错误处理逻辑
				return make([]Result, 0)
			}
			for i, _ := range tempResults {
				tempResults[i]["aa"] = tablename
			}

		}

	}
	if strings.Contains(att, ",") == true {
		tempResults = mergeMapData(tempResults)
	}
	//获取未压占部分
	used_area := 0.00
	for _, item := range tempResults {
		used_area = used_area + item["area"].(float64)
	}
	used_area = math.Round(used_area*100) / 100
	if Area-used_area >= 1 {
		tempResults = append(tempResults, map[string]interface{}{
			"area": Area - used_area,
			att:    "未占用",
		})
	}

	// 构造Result切片
	var finalResults []Result
	for _, tempResult := range tempResults {
		if area, ok := tempResult["area"].(float64); ok {
			if dlmc, ok := tempResult[att].(string); ok {
				finalResults = append(finalResults, Result{
					Area:          area,
					Dlmc:          DLMCReplace(dlmc),
					AttributeName: att, // 虽然我们不会使用这个字段，但它有助于调试和理解代码
				})
			}
		}
	}

	groupedResult := GroupAndSum(finalResults)

	return groupedResult
}

func GetIntersectGeo(jsonData geojson.FeatureCollection, tablename string, atts string) geojson.FeatureCollection {
	tablename = strings.ToLower(tablename)
	var result geojson.FeatureCollection
	geo := jsonData.Features[0]
	jsonString, _ := json.Marshal(geo)
	var data map[string]interface{}
	json.Unmarshal([]byte(jsonString), &data)
	value := data["geometry"]
	geodata, _ := json.Marshal(value)
	jsonDataStr := string(geodata)
	DB := models.DB
	sql := fmt.Sprintf(`
SELECT 
    ST_AsGeoJSON(ST_Intersection(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326)), ST_MakeValid(geom))) AS geojson, -- 将相交后的几何转换为GeoJSON格式
    %s 
FROM 
    "%s" 
WHERE 
    ST_Intersects(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326), geom)`,
		jsonDataStr, atts, tablename, jsonDataStr) // 合并SQL查询

	var tempResults []map[string]interface{}
	err := DB.Raw(sql).Scan(&tempResults)
	if err.Error != nil { // 检查是否发生错误
		// 错误处理逻辑
		fmt.Println(err.Error)
	}

	for _, item := range tempResults {
		var feature struct {
			Geometry   map[string]interface{} `json:"geometry"`
			Properties map[string]interface{} `json:"properties"`
			Type       string                 `json:"type"`
		}

		data, _ := item["geojson"].(string)
		err := json.Unmarshal([]byte(data), &feature.Geometry) // 将序列化后的数据解码为geojson.Feature
		if err != nil {
			fmt.Println("Unmarshal错误:", err.Error()) // 打印解码错误
			continue                                 // 跳过当前项
		}
		feature.Type = "Feature"
		att := make(map[string]interface{})
		for key, value := range item {
			if key != "geojson" {
				att[key] = value
			}

		}
		feature.Properties = att
		data2, _ := json.Marshal(feature)
		var feature2 geojson.Feature
		err = json.Unmarshal(data2, &feature2)
		if err != nil {
			fmt.Println("Unmarshal错误:", err.Error()) // 打印解码错误
			continue                                 // 跳过当前项
		}

		// 打印feature
		result.Features = append(result.Features, &feature2) // 将feature添加到结果中
	}

	return result
}

func GeoIntersectForBG(jsonData geojson.FeatureCollection, tablename string, att string) []Result {
	tablename = strings.ToLower(tablename)
	att = strings.ToLower(att)
	jsonDataStr := getFeatureString(jsonData)

	var sql string
	DB := models.DB
	var tempResults []map[string]interface{}

	Area := 0.00
	for _, item := range jsonData.Features {
		Area = Area + CalculateArea(item)
	}
	// 将GeoJSON字符串转为几何类型，并设定SRID为4326，同时计算交集的面积，并将结果作为area返回
	sql = fmt.Sprintf(`WITH geojson_data AS (
    SELECT
        jsonb_array_elements('%s'::jsonb) AS feature
)
SELECT "numeric"(ST_Area(St_Transform(ST_Intersection(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326)), geom), 4523)))
     AS area, %s AS "%s" 
FROM geojson_data, "%s" 
WHERE ST_Intersects(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON(feature->'geometry'::text), 4326)), geom)`, jsonDataStr, att, att, tablename)
	err := DB.Raw(sql).Scan(&tempResults)
	if err.Error != nil { // 检查是否发生错误
		// 错误处理逻辑
		fmt.Println(sql)
		fmt.Println(err.Error)
		return make([]Result, 0)
	}

	//获取未压占部分
	used_area := 0.00
	for _, item := range tempResults {
		used_area = used_area + item["area"].(float64)
	}
	used_area = math.Round(used_area*100) / 100
	if Area-used_area >= 1 {
		tempResults = append(tempResults, map[string]interface{}{
			"area": Area - used_area,
			att:    "未占用",
		})
	}

	// 构造Result切片
	var finalResults []Result
	for _, tempResult := range tempResults {
		if area, ok := tempResult["area"].(float64); ok {
			if dlmc, ok := tempResult[att].(string); ok {
				finalResults = append(finalResults, Result{
					Area:          area,
					Dlmc:          DLMCReplace(dlmc),
					AttributeName: att, // 虽然我们不会使用这个字段，但它有助于调试和理解代码
				})
			}
		}
	}

	groupedResult := GroupAndSum(finalResults)

	return groupedResult
}

func groupAndSum(data []map[string]interface{}, tt interface{}) []map[string]interface{} {
	grouped := make(map[string]map[string]interface{})
	var keys []string
	ttFields, ok := tt.([]string) // 尝试将tt参数转换为[]string类型

	if !ok {
		// 如果转换失败，说明tt不是[]string类型，封装成slice处理
		ttFields = []string{tt.(string)}
	}

	for _, item := range data {
		keys = keys[:0] // 重用keys切片，减少内存分配
		for k := range item {
			// 排除tt中的字段
			if !IsStringInSlice(k, ttFields) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys) // 确保字段顺序一致

		key := ""
		for _, k := range keys {
			key += fmt.Sprintf("%v=%v;", k, item[k])
		}

		if groupedData, found := grouped[key]; found {
			// 循环ttFields累加值
			for _, field := range ttFields {
				groupedData[field] = groupedData[field].(float64) + item[field].(float64)
			}
		} else {
			newGroupedData := make(map[string]interface{}, len(item))
			for k, v := range item {
				newGroupedData[k] = v
			}
			grouped[key] = newGroupedData
		}
	}

	var result []map[string]interface{}
	for _, val := range grouped {
		result = append(result, val)
	}
	return result
}

// 体积计算
func cylinderVolume(height float64, diameter float64) float64 {
	r := diameter / 2
	volume := math.Pi * math.Pow(r, 2) * height
	return volume
}
func GetVolume(data []map[string]interface{}) []map[string]interface{} {

	for i, item := range data {
		length := item["length"].(float64)
		r := item["zhijin"].(string)
		f, _ := strconv.ParseFloat(r, 64)
		volume := cylinderVolume(length, f/100)
		data[i]["volume"] = volume
	}
	return data
}

func GeoIntersectLine(jsonData geojson.FeatureCollection, tablename string, attributes []string, IsVolume bool) []map[string]interface{} {
	geo := jsonData.Features[0]
	jsonString, _ := json.Marshal(geo)
	var data map[string]interface{}
	json.Unmarshal([]byte(jsonString), &data)
	value := data["geometry"]
	geodata, _ := json.Marshal(value)
	jsonDataStr := string(geodata)

	DB := models.DB
	var tempResults []map[string]interface{}

	// 动态构建查询字符串

	selectParts := make([]string, 0, len(attributes))
	for _, attr := range attributes {
		selectPart := fmt.Sprintf("\"numeric\"(st_length(st_transform(st_intersection(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326)), ST_MakeValid(geom)), 4523))) AS length, \"%s\" AS \"%s\"", jsonDataStr, attr, attr)
		selectParts = append(selectParts, selectPart)
	}
	// 拼接所有的SELECT部分
	attrSelect := fmt.Sprintf("SELECT %s FROM \"%s\" WHERE ST_Intersects(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326), geom)",
		strings.Join(selectParts, ", "), tablename, jsonDataStr)

	DB.Raw(attrSelect).Scan(&tempResults)
	result := groupAndSum(tempResults, "length")
	if IsStringInSlice("zhijin", attributes) {
		result = GetVolume(result)
		if IsVolume == true {
			result = RemoveKeyFromMapArray(result, "zhijin")
		}

		result = groupAndSum(result, []string{"volume", "length"})
	}
	return result
}
