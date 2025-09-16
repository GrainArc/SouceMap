package views

import (
	"encoding/json"
	"fmt"
	"github.com/GrainArc/SouceMap/config"
	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/models"
	"github.com/gin-gonic/gin"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func (uc *UserController) Raster(c *gin.Context) {
	dbname := c.Param("dbname")
	dbpath := filepath.Join(config.Raster, dbname) + ".mbtiles"
	DB, _ := gorm.Open(sqlite.Open(dbpath), &gorm.Config{})
	defer func() {
		if DB, err := DB.DB(); err == nil {
			DB.Close()
		}
	}()
	x, _ := strconv.Atoi(c.Param("x"))
	y, _ := strconv.Atoi(strings.TrimSuffix(c.Param("y.png"), ".png"))
	z, _ := strconv.Atoi(c.Param("z"))

	var TempModel models.Tile

	DB.Where("tile_column = ? AND tile_row = ? AND zoom_level = ?", x, y, z).First(&TempModel)
	c.Header("Cache-Control", "public, max-age=0")

	if TempModel.TileData != nil {
		c.Data(http.StatusOK, "image/png", TempModel.TileData)
	} else {
		c.String(http.StatusOK, "err")
	}
}

func (uc *UserController) Dem(c *gin.Context) {
	x, _ := strconv.Atoi(c.Param("x"))
	y, _ := strconv.Atoi(strings.TrimSuffix(c.Param("y.webp"), ".webp"))
	z, _ := strconv.Atoi(c.Param("z"))
	DB := models.DemDB
	var TempModel models.Tile
	DB.Where("tile_column = ? AND tile_row = ? AND zoom_level = ?", x, y, z).First(&TempModel)
	c.Header("Cache-Control", "public, max-age=0")
	if TempModel.TileData != nil {
		c.Data(http.StatusOK, "text/html", TempModel.TileData)
	} else {
		c.String(http.StatusOK, "err")
	}
}

func (uc *UserController) GetRasterName(c *gin.Context) {
	dirPath := config.Raster
	files, _ := os.ReadDir(dirPath)
	// 创建一个用于存储符合条件的文件名的切片
	var mbtilesFiles []string
	// 遍历读取的文件和子目录
	for _, file := range files {
		// 判断是否是文件
		if !file.IsDir() {
			// 判断文件名是否以".mbtiles"结尾
			if strings.HasSuffix(file.Name(), ".mbtiles") {
				// 将符合条件的文件名添加到切片中（去除.mbtiles后缀）
				mbtilesFiles = append(mbtilesFiles, strings.TrimSuffix(file.Name(), ".mbtiles"))

				// 构建数据库文件的完整路径
				dbname := file.Name()                          // 获取完整的数据库文件名
				dbpath := filepath.Join(config.Raster, dbname) // 拼接数据库文件的绝对路径

				// 打开SQLite数据库连接，使用GORM的SQLite驱动
				DB, err := gorm.Open(sqlite.Open(dbpath), &gorm.Config{})
				if err != nil {
					// 记录数据库打开失败的错误，但不中断整个流程

					continue // 跳过当前文件，继续处理下一个文件
				}

				// 为当前数据库建立瓦片查询索引以提高查询性能
				methods.MakeTileIndex(DB)

				// 获取底层的sql.DB实例并立即关闭连接，避免资源泄漏
				if sqlDB, err := DB.DB(); err == nil {
					sqlDB.Close() // 立即关闭数据库连接，释放资源
				} else {

				}
			}

		}
	}

	c.JSON(http.StatusOK, mbtilesFiles)
}
func createIndexes(db *gorm.DB) error {
	// 检查并创建 TilesHeader 表的复合索引
	if !indexExists(db, "idx_tiles_header_folder_json_name") {
		err := db.Exec("CREATE INDEX idx_tiles_header_folder_json_name ON tiles_headers(folder, json_name)").Error
		if err != nil {
			return fmt.Errorf("failed to create index for tiles_headers: %w", err)
		}
	}

	// 检查并创建 TilesByte 表的复合索引
	if !indexExists(db, "idx_tiles_byte_folder_tile_name") {
		err := db.Exec("CREATE INDEX idx_tiles_byte_folder_tile_name ON tiles_bytes(folder, tile_name)").Error
		if err != nil {
			return fmt.Errorf("failed to create index for tiles_bytes: %w", err)
		}
	}

	return nil
}

// 检查索引是否存在
func indexExists(db *gorm.DB, indexName string) bool {
	var count int64

	// SQLite 查询索引是否存在
	err := db.Raw("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?", indexName).Scan(&count).Error
	if err != nil {
		// 如果查询出错，返回 false，让程序尝试创建索引
		return false
	}

	return count > 0
}

// 获取3dtiles文件名称接口
func (uc *UserController) GetTilesName(c *gin.Context) {
	dirPath := config.Tiles3d
	files, _ := os.ReadDir(dirPath)
	DB := models.DB
	// 创建一个用于存储符合条件的文件名的切片
	var mbtilesFiles []models.TilesSet
	// 遍历读取的文件和子目录
	for _, file := range files {
		// 判断是否是文件
		if !file.IsDir() {
			// 判断文件名是否以".mbtiles"结尾
			if strings.HasSuffix(file.Name(), ".gl") {
				// 将符合条件的文件名添加到切片中
				filename := strings.TrimSuffix(file.Name(), ".gl")
				//查询数据库中是否存在数据
				var tiles models.TilesSet
				err := DB.Where("name = ?", filename).First(&tiles).Error
				if err == nil { // 检查查询是否出现错误
					mbtilesFiles = append(mbtilesFiles, tiles)
				} else {
					tiles.Name = filename
					tiles.UpDown = "0"
					DB.Create(&tiles)
					mbtilesFiles = append(mbtilesFiles, tiles)
				}

			}
		}
	}
	c.JSON(http.StatusOK, mbtilesFiles)
}

type TilesXYZ struct {
	Dbname string  `json:"Dbname"`
	DX     float64 `json:"DX"`
	DY     float64 `json:"DY"`
	DZ     float64 `json:"DZ"`
}

// 获取3dtilejson接口
func (uc *UserController) Tiles3DJson(c *gin.Context) {
	dbname := c.Param("dbname")

	dbpath := filepath.Join(config.Tiles3d, dbname) + ".gl"
	DB, _ := gorm.Open(sqlite.Open(dbpath), &gorm.Config{})

	defer func() {
		if DB, err := DB.DB(); err == nil {
			DB.Close()
		}
	}()
	folder := c.Param("folder")
	var TempModel models.TilesHeader
	var TempModelBytes models.TilesByte
	name := c.Param("name")
	if strings.HasSuffix(name, ".json") {
		DB.Where("folder = ? AND json_name = ?", folder, name).Find(&TempModel)
		var data models.TilesJson
		json.Unmarshal(TempModel.TileJson, &data)

		c.JSON(200, data)

	} else {
		DB.Where("folder = ? AND tile_name = ?", folder, name).Find(&TempModelBytes)

		c.Data(http.StatusOK, "application/octet-stream", TempModelBytes.TileData)

	}

}

// 获取头文件接口
func (uc *UserController) TileSetGet(c *gin.Context) {
	dbname := c.Param("dbname")
	dbpath := filepath.Join(config.Tiles3d, dbname) + ".gl"
	DB, _ := gorm.Open(sqlite.Open(dbpath), &gorm.Config{})
	folder := "main"
	var TempModel models.TilesHeader
	name := "tileset.json"
	DB.Where("folder = ? AND json_name = ?", folder, name).Find(&TempModel)
	createIndexes(DB)
	var TileSet models.TileSetJson
	json.Unmarshal(TempModel.TileJson, &TileSet)

	c.JSON(http.StatusOK, TileSet)

}

// 字体接口
func (uc *UserController) FontGet(c *gin.Context) {
	c.File("font.pbf")

}
