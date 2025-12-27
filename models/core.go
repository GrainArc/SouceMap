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
	// 初始化 DEM 数据库
	DemDB, err := gorm.Open(sqlite.Open(config.Dem), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Printf("Failed to open DEM database: %v", err)
	} else {
		makeTileIndex(DemDB)
		// 立即关闭 DEM 数据库连接
		if sqlDB, err := DemDB.DB(); err == nil {
			defer sqlDB.Close()
		}
	}

	// 初始化主数据库
	DB, err = gorm.Open(postgres.Open(config.DSN), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// 设置命名策略
	DB.NamingStrategy = schema.NamingStrategy{
		SingularTable: true,
	}

	// 批量迁移所有表
	if err := migrateAllTables(DB); err != nil {
		log.Printf("Failed to migrate tables: %v", err)
	}

	//// 修复所有表的索引
	//if err := repairAllIndexes(DB); err != nil {
	//	log.Printf("Failed to repair indexes: %v", err)
	//}

	// 初始化默认用户
	initDefaultUser(DB)
}

// migrateAllTables 批量迁移所有表
func migrateAllTables(db *gorm.DB) error {
	models := []interface{}{
		&TempGeo{},
		&LoginUser{},
		&TempLayer{},
		&TempLayHeader{},
		&AttColor{},
		&ChineseProperty{},
		&LayerMXD{},
		&LayerHeader{},
		&GeoPic{},
		&TempLayerAttribute{},
		&FieldRecord{},
		&UpdateMessage{},
		&MySchema{},
		&ZDTPic{},
		&GeoRecord{},
		&Report{},
		&NetMap{},
	}

	return db.AutoMigrate(models...)
}

// repairAllIndexes 修复所有表的索引
func repairAllIndexes(db *gorm.DB) error {
	models := []interface{}{
		&TempGeo{},
		&LoginUser{},
		&TempLayer{},
		&TempLayHeader{},
		&AttColor{},
		&ChineseProperty{},
		&LayerMXD{},
		&LayerHeader{},
		&GeoPic{},
		&TempLayerAttribute{},
		&FieldRecord{},
		&UpdateMessage{},
		&MySchema{},
		&ZDTPic{},
		&GeoRecord{},
		&Report{},
	}

	for _, model := range models {
		if err := repairTableIndexes(db, model); err != nil {
			log.Printf("Failed to repair indexes for %T: %v", model, err)
			// 继续处理其他表，不中断
			continue
		}
	}

	return nil
}

// repairTableIndexes 修复单个表的索引
func repairTableIndexes(db *gorm.DB, model interface{}) error {
	// 获取表名
	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(model); err != nil {
		return fmt.Errorf("failed to parse model: %w", err)
	}
	tableName := stmt.Schema.Table

	log.Printf("Repairing indexes for table: %s", tableName)

	// PostgreSQL 特定的索引修复
	// 1. 重建所有索引
	if err := reindexTable(db, tableName); err != nil {
		return fmt.Errorf("failed to reindex table %s: %w", tableName, err)
	}

	// 2. 检查并创建缺失的索引
	if err := ensureModelIndexes(db, model); err != nil {
		return fmt.Errorf("failed to ensure indexes for %s: %w", tableName, err)
	}

	return nil
}

// 重建表的所有索引（PostgreSQL）
func reindexTable(db *gorm.DB, tableName string) error {
	sql := fmt.Sprintf("REINDEX TABLE %s", tableName)
	return db.Exec(sql).Error
}

// 确保模型定义的索引都存在
func ensureModelIndexes(db *gorm.DB, model interface{}) error {
	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(model); err != nil {
		return err
	}

	// 遍历所有字段，检查索引定义
	for _, field := range stmt.Schema.Fields {
		// 处理普通索引
		if field.TagSettings["INDEX"] != "" {
			indexName := field.TagSettings["INDEX"]
			if indexName == "" {
				indexName = fmt.Sprintf("idx_%s_%s", stmt.Schema.Table, field.DBName)
			}

			// 检查索引是否存在
			if !indexExists(db, stmt.Schema.Table, indexName) {
				if err := createIndex(db, stmt.Schema.Table, field.DBName, indexName, false); err != nil {
					log.Printf("Failed to create index %s: %v", indexName, err)
				}
			}
		}

		// 处理唯一索引
		if field.TagSettings["UNIQUEINDEX"] != "" {
			indexName := field.TagSettings["UNIQUEINDEX"]
			if indexName == "" {
				indexName = fmt.Sprintf("uidx_%s_%s", stmt.Schema.Table, field.DBName)
			}

			if !indexExists(db, stmt.Schema.Table, indexName) {
				if err := createIndex(db, stmt.Schema.Table, field.DBName, indexName, true); err != nil {
					log.Printf("Failed to create unique index %s: %v", indexName, err)
				}
			}
		}
	}

	return nil
}

// indexExists 检查索引是否存在（PostgreSQL）
func indexExists(db *gorm.DB, tableName, indexName string) bool {
	var count int64
	db.Raw(`
        SELECT COUNT(*) 
        FROM pg_indexes 
        WHERE tablename = ? AND indexname = ?
    `, tableName, indexName).Scan(&count)

	return count > 0
}

// createIndex 创建索引
func createIndex(db *gorm.DB, tableName, columnName, indexName string, unique bool) error {
	uniqueStr := ""
	if unique {
		uniqueStr = "UNIQUE"
	}

	sql := fmt.Sprintf("CREATE %s INDEX IF NOT EXISTS %s ON %s (%s)",
		uniqueStr, indexName, tableName, columnName)

	return db.Exec(sql).Error
}

// initDefaultUser 初始化默认用户
func initDefaultUser(db *gorm.DB) {
	user := LoginUser{
		ID:    1,
		Token: "0",
		Name:  "本地",
	}

	var existingUser LoginUser
	result := db.First(&existingUser, user.ID)

	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		if err := db.Create(&user).Error; err != nil {
			log.Printf("Failed to create default user: %v", err)
		} else {
			log.Println("Default user created successfully")
		}
	}
}
