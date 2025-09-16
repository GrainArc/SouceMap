package pgmvt

import (
	"fmt"
	"github.com/GrainArc/SouceMap/methods"
	"github.com/paulmach/orb"
	"gorm.io/gorm"
	"log"
	"reflect"
	"strings"
	"unicode"
)

type MVTTile struct {
	MVT []byte
}

func removeValue(s []string, value string) []string {
	var result []string
	for _, v := range s {
		if v != value { // 仅将不等于指定值的元素加入结果切片
			result = append(result, v)
		}
	}
	return result
}
func GetTableColumns(db *gorm.DB, tableName string) ([]string, error) {
	var columns []string
	rows, err := db.Raw("SELECT column_name FROM information_schema.columns WHERE table_name = ?", tableName).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	newData := removeValue(columns, "geom")
	return newData, nil
}

func MakeMvt(x int, y int, z int, items interface{}, db *gorm.DB, TempModel interface{}) []byte {

	db.Where("X = ? AND Y = ? AND Z = ?", x, y, z).Find(&TempModel)

	rt := reflect.TypeOf(items)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	tableName := methods.CamelCaseToUnderscore(rt.Name()) //查询表名

	fieldNames, _ := GetTableColumns(db, tableName)
	result := strings.Join(fieldNames, ",")
	//映射TempModel
	v := reflect.ValueOf(TempModel)
	t := reflect.TypeOf(TempModel)
	var sliceValue reflect.Value
	if t.Kind().String() == "slice" {
		sliceValue = v
	}

	if sliceValue.Len() == 1 {
		t := sliceValue.Index(0).Interface()
		value := reflect.ValueOf(t)
		var mvt_bytes []byte
		mvt_bytes = value.FieldByName("Byte").Interface().([]byte)
		return mvt_bytes

	} else if sliceValue.Len() > 1 {
		for i := 1; i < sliceValue.Len(); i++ {
			t := sliceValue.Index(i).Interface() //获取切片对应项
			id := reflect.ValueOf(t).FieldByName("ID").Interface()
			db.Delete(&TempModel, id)
		}
		value := reflect.ValueOf(sliceValue.Index(0).Interface())
		var mvt_bytes []byte
		mvt_bytes = value.FieldByName("Byte").Interface().([]byte)
		return mvt_bytes
	} else {
		boundbox_min := XyzLonLat(float64(x), float64(y), float64(z))
		boundbox_max := XyzLonLat(float64(x)+1, float64(y)+1, float64(z))
		sql := fmt.Sprintf("SELECT ST_AsMVT(P, 'polygon', 256, 'geom') AS \"mvt\" "+
			"FROM (SELECT ST_AsMVTGeom(ST_Simplify(ST_Transform(geom, 3857), 0.1), ST_Transform(ST_MakeEnvelope(%v, %v, %v, %v, 4326), 3857), 256, 64, TRUE) AS geom, %s "+
			"FROM \"%s\" WHERE \"geom\" && ST_MakeEnvelope(%v, %v, %v, %v, 4326)) AS P", boundbox_min[0], boundbox_min[1], boundbox_max[0], boundbox_max[1], result, tableName, boundbox_min[0], boundbox_min[1], boundbox_max[0], boundbox_max[1])

		var mvttile MVTTile
		db.Raw(sql).Scan(&mvttile)
		if len(mvttile.MVT) != 0 {
			TempAttr := map[string]interface{}{
				"x":    x,
				"y":    y,
				"z":    z,
				"byte": mvttile.MVT,
			}
			db.Model(&TempModel).Create(TempAttr)
			return mvttile.MVT
		} else {
			return nil
		}

	}
}
func isEndWithNumber(s string) bool {
	for _, char := range s {
		if unicode.IsDigit(char) && s[len(s)-1] == byte(char) {
			return true
		}
	}
	return false
}

func IsEndWithNumber(s string) bool {
	for _, char := range s {
		if unicode.IsDigit(char) && s[len(s)-1] == byte(char) {
			return true
		}
	}
	return false
}
func MakeMvtNew(x int, y int, z int, tableName string, db *gorm.DB) []byte {
	var TempModelName string
	if isEndWithNumber(tableName) {
		TempModelName = tableName + "_mvt"
	} else {
		TempModelName = tableName + "mvt"
	}

	var TempModel []map[string]interface{}

	query := fmt.Sprintf("SELECT * FROM %s WHERE x = ? AND y = ? AND z = ?", TempModelName)
	db.Raw(query, x, y, z).Scan(&TempModel)

	fieldNames, _ := GetTableColumns(db, tableName)
	result := strings.Join(fieldNames, ",")

	if len(TempModel) == 1 {
		byteData, _ := TempModel[0]["byte"].([]byte)
		return byteData

	} else if len(TempModel) > 1 {
		for i := 1; i < len(TempModel); i++ {
			id := TempModel[i]["id"]
			query = fmt.Sprintf("DELETE FROM %s WHERE id = ?", TempModelName)
			db.Exec(query, id)
		}
		byteData, _ := TempModel[0]["byte"].([]byte)
		return byteData
	} else {
		boundbox_min := XyzLonLat(float64(x), float64(y), float64(z))
		boundbox_max := XyzLonLat(float64(x)+1, float64(y)+1, float64(z))
		sql := fmt.Sprintf("SELECT ST_AsMVT(P, 'polygon', 512, 'geom') AS \"mvt\" "+
			"FROM (SELECT ST_AsMVTGeom(ST_Simplify(ST_Transform(geom, 3857), 0.1), ST_Transform(ST_MakeEnvelope(%v, %v, %v, %v, 4326), 3857), 512, 64, TRUE) AS geom, %s "+
			"FROM \"%s\" WHERE \"geom\" && ST_MakeEnvelope(%v, %v, %v, %v, 4326)) AS P", boundbox_min[0], boundbox_min[1], boundbox_max[0], boundbox_max[1], result, tableName, boundbox_min[0], boundbox_min[1], boundbox_max[0], boundbox_max[1])

		var mvttile MVTTile
		db.Raw(sql).Scan(&mvttile)
		if len(mvttile.MVT) != 0 {

			query = fmt.Sprintf("INSERT  INTO  %s  (x,  y,  z,  byte)  VALUES  (?,  ?,  ?,  ?)", TempModelName)
			db.Exec(query, x, y, z, mvttile.MVT)
			ensureTableAndIndex(db, TempModelName)
			return mvttile.MVT
		} else {
			return nil
		}

	}
}

func ensureTableAndIndex(db *gorm.DB, tableName string) {

	if !indexExists(db, tableName, "idx_xyz_"+tableName) {
		createIndexSQL := fmt.Sprintf("CREATE INDEX idx_xyz_%s ON %s (x, y, z)", tableName, tableName)
		db.Exec(createIndexSQL)
	}
}

// 检查索引是否存在
func indexExists(db *gorm.DB, tableName, indexName string) bool {
	var count int64
	db.Raw(`
		SELECT COUNT(*) 
		FROM pg_indexes 
		WHERE tablename = ? AND indexname = ?
	`, tableName, indexName).Scan(&count)
	return count > 0
}

func DelMVT(DB *gorm.DB, tablename string, geom orb.Geometry) {
	tile := Bounds(geom)
	for _, value := range tile {
		if err := DB.Table(tablename+"mvt").Where("x = ? AND y = ? AND z = ?", value.X, value.Y, value.Z).Delete(nil).Error; err != nil {
			log.Printf(" error: %v", err)
		}
	}
}
