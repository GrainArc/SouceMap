package config

import (
	"log"
	"os"
	"path/filepath"

	"github.com/GrainArc/SouceMap/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var DB *gorm.DB

// InitDatabase 初始化SQLite数据库
func InitDatabase() error {
	// 确保目录存在
	StoragePath := MainConfig.Download + "/Texture"
	DBFileName := "texture.db"
	if err := os.MkdirAll(StoragePath, os.ModePerm); err != nil {
		log.Printf("创建存储目录失败: %v", err)
		return err
	}

	dbPath := filepath.Join(StoragePath, DBFileName)
	log.Printf("数据库路径: %s", dbPath)

	var err error
	DB, err = gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		log.Printf("连接数据库失败: %v", err)
		return err
	}

	// 自动迁移，创建表结构
	if err := DB.AutoMigrate(&models.Texture{}); err != nil {
		log.Printf("数据库迁移失败: %v", err)
		return err
	}

	log.Println("数据库初始化成功")
	return nil
}

// GetDB 获取数据库实例
func GetDB() *gorm.DB {
	return DB
}
