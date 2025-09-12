package models

import (
	"github.com/fmecool/SouceMap/config"

	"errors"
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

var DB *gorm.DB

var DemDB *gorm.DB
var err error

func makeTileIndex(DB *gorm.DB) {
	// 查询索引是否已存在
	var exists bool
	checkIndexSql := fmt.Sprintf(`
		SELECT COUNT(*) > 0 
		FROM sqlite_master 
		WHERE type = 'index' AND name = 'idx_tile_xyz'
	`)

	err := DB.Raw(checkIndexSql).Scan(&exists).Error
	if err != nil {
		fmt.Println("Error checking index existence:", err.Error())
		return
	}

	if !exists {
		// 如果索引不存在，则创建索引
		createIndexSql := fmt.Sprintf(`CREATE INDEX idx_tile_xyz ON tiles (tile_column, tile_row, zoom_level);`)
		err := DB.Exec(createIndexSql).Error
		if err != nil {
			fmt.Println("Error creating index:", err.Error())
		} else {
			fmt.Println("成功创建索引")
		}
	} else {

	}
}

func InitDB() {
	DemDB, err = gorm.Open(sqlite.Open(config.Dem), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		fmt.Println(err)
	} else {
		makeTileIndex(DemDB)
	}

	DB, err = gorm.Open(postgres.Open(config.DSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		fmt.Println(err)
	}
	DB.NamingStrategy = schema.NamingStrategy{
		SingularTable: true,
	}

	DB.AutoMigrate(&TempGeo{})

	DB.AutoMigrate(&LoginUser{})
	DB.AutoMigrate(&TempLayer{})
	DB.AutoMigrate(&TempLayHeader{})

	DB.AutoMigrate(&AttColor{})
	DB.AutoMigrate(&ChineseProperty{})

	DB.AutoMigrate(&GeoPic{})
	DB.AutoMigrate(&TempLayerAttribute{})

	//新增

	DB.AutoMigrate(&UpdateMessage{})

	DB.AutoMigrate(&MySchema{})

	DB.AutoMigrate(&ZDTPic{})
	DB.AutoMigrate(&GeoRecord{})

	user := LoginUser{}
	user.Token = "0"
	user.Name = "本地"
	user.ID = 1
	result := DB.First(&LoginUser{}, user.ID)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		DB.Create(&user)
	}

}
