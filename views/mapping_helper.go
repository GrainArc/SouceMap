// views/mapping_helper.go
package views

import (
	"encoding/json"
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"gorm.io/gorm"
	"time"
)

// InitOriginMapping 数据首次导入时初始化映射表
func InitOriginMapping(db *gorm.DB, tableName string, keyField string) error {
	sql := fmt.Sprintf(`
		INSERT INTO origin_mapping (table_name, post_gis_id, source_object_id, origin, session_id, is_deleted)
		SELECT '%s', id, "%s", 'original', 0, false
		FROM "%s"
	`, tableName, keyField, tableName)
	return db.Exec(sql).Error
}

// MarkMappingDeleted 标记映射为已删除
func MarkMappingDeleted(db *gorm.DB, tableName string, postGISIDs []int32) error {
	if len(postGISIDs) == 0 {
		return nil
	}
	return db.Model(&models.OriginMapping{}).
		Where("table_name = ? AND post_gis_id IN ? AND is_deleted = false", tableName, postGISIDs).
		Update("is_deleted", true).Error
}

// RestoreMappingActive 恢复映射为活跃状态（回退时用）
func RestoreMappingActive(db *gorm.DB, tableName string, postGISIDs []int32) error {
	if len(postGISIDs) == 0 {
		return nil
	}
	return db.Model(&models.OriginMapping{}).
		Where("table_name = ? AND post_gis_id IN ?", tableName, postGISIDs).
		Update("is_deleted", false).Error
}

// CreateDerivedMappings 为派生要素创建映射记录
func CreateDerivedMappings(db *gorm.DB, tableName string, newIDs []int32, parentID int32, sessionID int64) error {
	for _, nid := range newIDs {
		mapping := models.OriginMapping{
			TableName:       tableName,
			PostGISID:       nid,
			SourceObjectID:  -1,
			Origin:          "derived",
			ParentPostGISID: parentID,
			SessionID:       sessionID,
			IsDeleted:       false,
		}
		if err := db.Create(&mapping).Error; err != nil {
			return err
		}
	}
	return nil
}

// CreateDerivedMappingsMultiParent 多父要素派生（合并/聚合场景）
func CreateDerivedMappingsMultiParent(db *gorm.DB, tableName string, newIDs []int32, parentIDs []int32, sessionID int64) error {
	for _, nid := range newIDs {
		// 以第一个parent作为主parent记录，完整parent列表通过GeoRecord的InputIDs追溯
		var mainParent int32
		if len(parentIDs) > 0 {
			mainParent = parentIDs[0]
		}
		mapping := models.OriginMapping{
			TableName:       tableName,
			PostGISID:       nid,
			SourceObjectID:  -1,
			Origin:          "derived",
			ParentPostGISID: mainParent,
			SessionID:       sessionID,
			IsDeleted:       false,
		}
		if err := db.Create(&mapping).Error; err != nil {
			return err
		}
	}
	return nil
}

// GetOrCreateSession 获取或创建编辑会话
func GetOrCreateSession(db *gorm.DB, tableName string, username string) models.EditSession {
	session := models.EditSession{
		TableName: tableName,
		Username:  username,
		CreatedAt: timeNowStr(),
		Status:    "active",
	}
	db.Create(&session)
	return session
}

// GetNextSeqNo 获取会话内下一个操作序号
func GetNextSeqNo(db *gorm.DB, sessionID int64) int {
	var maxSeq int
	db.Model(&models.GeoRecord{}).
		Where("session_id = ?", sessionID).
		Select("COALESCE(MAX(seq_no), 0)").
		Scan(&maxSeq)
	return maxSeq + 1
}

// MarshalIDs 序列化ID列表
func MarshalIDs(ids []int32) []byte {
	data, _ := json.Marshal(ids)
	return data
}

func timeNowStr() string {
	return time.Now().Format("2006-01-02 15:04:05")
}
