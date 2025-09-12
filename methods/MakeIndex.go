package methods

import (
	"fmt"
	"gorm.io/gorm"
)

// 获取3dtiles文件名称接口
func MakeTileIndex(DB *gorm.DB) {
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
