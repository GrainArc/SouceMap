package pgmvt

import (
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"github.com/paulmach/orb"
	"gorm.io/gorm"
	"log"
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
	var Tb models.MySchema
	db.Where("en = ?", tableName).First(&Tb)
	var tileSize int64
	if Tb.TileSize != 0 {
		tileSize = Tb.TileSize
	}
	var TempModel []map[string]interface{}

	query := fmt.Sprintf("SELECT * FROM %s WHERE x = ? AND y = ? AND z = ?", TempModelName)
	db.Raw(query, x, y, z).Scan(&TempModel)

	fieldNames, _ := GetTableColumns(db, tableName)
	// 给每个字段名用双引号包裹，防止关键字冲突
	quotedFields := make([]string, len(fieldNames))
	for i, field := range fieldNames {
		quotedFields[i] = fmt.Sprintf("\"%s\"", field)
	}
	result := strings.Join(quotedFields, ",")

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
		sql := fmt.Sprintf("SELECT ST_AsMVT(P, 'polygon', %d, 'geom') AS \"mvt\" "+
			"FROM (SELECT ST_AsMVTGeom(ST_Simplify(ST_Transform(geom, 3857), 0.1), ST_Transform(ST_MakeEnvelope(%v, %v, %v, %v, 4326), 3857), %d, 32, TRUE) AS geom, %s "+
			"FROM \"%s\" WHERE \"geom\" && ST_MakeEnvelope(%v, %v, %v, %v, 4326)) AS P", tileSize, boundbox_min[0], boundbox_min[1], boundbox_max[0], boundbox_max[1], tileSize, result, tableName, boundbox_min[0], boundbox_min[1], boundbox_max[0], boundbox_max[1])

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
	if len(tile) == 0 {
		return
	}
	if len(tile) > 200 {
		DelMVTALL(DB, tablename)
		return
	}
	var TempModelName string
	if isEndWithNumber(tablename) {
		TempModelName = tablename + "_mvt"
	} else {
		TempModelName = tablename + "mvt"
	}

	// 构建 OR 条件，一次性删除所有匹配的记录
	query := DB.Table(TempModelName)
	for i, value := range tile {
		if i == 0 {
			query = query.Where("(x = ? AND y = ? AND z = ?)", value.X, value.Y, value.Z)
		} else {
			query = query.Or("(x = ? AND y = ? AND z = ?)", value.X, value.Y, value.Z)
		}
	}

	if err := query.Delete(nil).Error; err != nil {
		log.Printf("error: %v", err)
	}
	if err := DB.Table(TempModelName).Where("z > ?", 19).Delete(nil).Error; err != nil {
		log.Printf("error deleting tiles with z > 19: %v", err)
	}
}

func DelMVTALL(DB *gorm.DB, tablename string) {
	var TempModelName string
	if isEndWithNumber(tablename) {
		TempModelName = tablename + "_mvt"
	} else {
		TempModelName = tablename + "mvt"
	}

	result := DB.Table(TempModelName).Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(nil)
	if result.Error != nil {
		log.Printf("error deleting all from %s: %v", TempModelName, result.Error)
		return
	}

}
