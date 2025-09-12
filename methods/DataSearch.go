package methods

import (
	"github.com/fmecool/SouceMap/models"
	"gorm.io/gorm"
	"reflect"
	"strings"
)

func DataSearch(query *gorm.DB, jsonData map[string]interface{}) []models.TempLayHeader {
	v := reflect.ValueOf(jsonData)
	page := 1
	limit := 20
	keys := v.MapKeys()
	queries := []string{}
	for _, k := range keys {
		fieldName := k.String()
		if fieldName != "page" && fieldName != "limit" {
			value := v.MapIndex(k).Interface()
			t := reflect.TypeOf(value)
			if t != nil && t.String() == "string" {
				vv := value.(string)
				if vv != "" {
					querie := fieldName + " LIKE '%" + vv + "%'"
					queries = append(queries, querie)
				}
			}

		} else if fieldName == "limit" {
			value := v.MapIndex(k).Interface().(float64)
			limit = int(value)
		} else {
			value := v.MapIndex(k).Interface().(float64)
			page = int(value)
		}
	}
	finalQuery := strings.Join(queries, " AND ")

	var data []models.TempLayHeader
	query1 := query.Where(finalQuery)
	query = query1.Offset((page - 1) * limit).Limit(limit)
	if err := query.Find(&data).Error; err != nil {
		return nil
	}
	return data
}

func TempDataSearch(query *gorm.DB, jsonData map[string]interface{}) []models.TempGeo {
	v := reflect.ValueOf(jsonData)
	page := 1
	limit := 20
	keys := v.MapKeys()
	queries := []string{}
	for _, k := range keys {
		fieldName := k.String()

		if fieldName != "page" && fieldName != "limit" {
			value := v.MapIndex(k).Interface()

			t := reflect.TypeOf(value)

			if t != nil && t.String() == "string" {
				vv := value.(string)
				if vv != "" {
					querie := fieldName + " LIKE '%" + vv + "%'"
					queries = append(queries, querie)
				}
			}

		} else if fieldName == "limit" {
			value := v.MapIndex(k).Interface().(float64)
			limit = int(value)
		} else {
			value := v.MapIndex(k).Interface().(float64)
			page = int(value)
		}
	}

	finalQuery := strings.Join(queries, " AND ")

	var data []models.TempGeo
	query1 := query.Where(finalQuery)
	query = query1.Offset((page - 1) * limit).Limit(limit)
	if err := query.Find(&data).Error; err != nil {
		return nil
	}
	return data
}

type Att struct {
	Text  string `json:"text"`
	Value string `json:"value"`
}
type Res struct {
	ColumnName string
}

func getAtt(TableName string, QZ string) []string {
	var result []Res
	DB := models.DB
	sql := `SELECT  column_name
                        FROM  information_schema.columns
                        WHERE  table_name  =  ?
` //  在GORM中使用原生SQL查询时，你应当使用Raw方法来执行查询，而Scan方法用于扫描结果到一个结果集合
	err := DB.Raw(sql, TableName).Scan(&result).Error
	//  如果执行数据库操作时发生错误，向客户端返回错误信息
	if err != nil {
		return make([]string, 0)
	}
	var atts []string
	for _, item := range result {
		switch item.ColumnName {
		case "id", "geom", "zzzmj", "qznydmj", "qzgdmj", "qzjbntmj", "qzjsydmj", "qzwlydmj", "xzjsyd":
			// 如果不是"id", "geom", "tbmj"，则创建一个新的Att结构体并添加到atts中
		default:
			atts = append(atts, QZ+item.ColumnName)
		}
	}
	return atts
}
