package views

import (
	"fmt"
	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/models"

	"github.com/paulmach/orb/geojson"
)

type TempLayerGeo struct {
	ID int64 `gorm:"primary_key"`

	Geojson geojson.Feature
}
type UserController struct {
	fieldService      *methods.FieldService
	service           *methods.GeometryService
	calculatorService *methods.FieldCalculatorService
}

func GetAtt(TableName string, QZ string) []string {
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

func MakeGeoIndex(TableName string) {
	DB := models.DB

	// 查询索引是否已存在
	var exists bool
	checkIndexSql := fmt.Sprintf(`SELECT EXISTS (
		SELECT 1 
		FROM pg_indexes 
		WHERE schemaname = 'public' 
		AND indexname = 'idx_%s_geom'
	);`, TableName)

	err := DB.Raw(checkIndexSql).Scan(&exists).Error
	if err != nil {
		fmt.Println("Error checking index existence:", err.Error())
		return
	}

	if !exists {
		// 如果索引不存在，则创建索引
		createIndexSql := fmt.Sprintf(`CREATE INDEX idx_%s_geom ON %s USING GIST (geom);`, TableName, TableName)
		err := DB.Exec(createIndexSql).Error
		if err != nil {
			fmt.Println("Error creating index:", err.Error())
		} else {

		}
	} else {

		// 如果索引已存在，可以选择更新或忽略

	}
}
