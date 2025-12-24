package models

import (
	"github.com/GrainArc/SouceMap/config"
	"log"
	"os"
	"path/filepath"

	"errors"
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

var DB *gorm.DB

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

var TextureDB *gorm.DB

// InitDatabase 初始化SQLite数据库
func InitDatabase() error {
	// 确保目录存在
	StoragePath := config.MainConfig.Download + "/Texture"
	DBFileName := "texture.db"
	if err := os.MkdirAll(StoragePath, os.ModePerm); err != nil {
		log.Printf("创建存储目录失败: %v", err)
		return err
	}

	dbPath := filepath.Join(StoragePath, DBFileName)
	log.Printf("数据库路径: %s", dbPath)

	var err error
	TextureDB, err = gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		log.Printf("连接数据库失败: %v", err)
		return err
	}

	// 自动迁移，创建表结构
	if err := TextureDB.AutoMigrate(&Texture{}); err != nil {
		log.Printf("数据库迁移失败: %v", err)
		return err
	}
	if err := TextureDB.AutoMigrate(&Symbol{}); err != nil {
		log.Printf("数据库迁移失败: %v", err)
		return err
	}

	log.Println("数据库初始化成功")
	return nil
}

func GetDB() *gorm.DB {
	return TextureDB
}

func InitDB() {
	DemDB, err := gorm.Open(sqlite.Open(config.Dem), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		fmt.Println(err)
	} else {
		makeTileIndex(DemDB)
	}
	defer func() {
		if eDB, err := DemDB.DB(); err == nil {
			eDB.Close()
		}
	}()
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
	DB.AutoMigrate(&LayerMXD{})
	DB.AutoMigrate(&LayerHeader{})
	DB.AutoMigrate(&GeoPic{})
	DB.AutoMigrate(&TempLayerAttribute{})
	DB.AutoMigrate(&FieldRecord{})

	//新增

	DB.AutoMigrate(&UpdateMessage{})

	DB.AutoMigrate(&MySchema{})

	DB.AutoMigrate(&ZDTPic{})
	DB.AutoMigrate(&GeoRecord{})
	DB.AutoMigrate(&Report{})

	user := LoginUser{}
	user.Token = "0"
	user.Name = "本地"
	user.ID = 1
	result := DB.First(&LoginUser{}, user.ID)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		DB.Create(&user)
	}

}
