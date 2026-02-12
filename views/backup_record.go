// views/backup_record.go
package views

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"

	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/models"
	"github.com/GrainArc/SouceMap/pgmvt"
	"github.com/gin-gonic/gin"
	"github.com/paulmach/orb/geojson"
	"gorm.io/gorm"
)

// ==================== 依赖查找 ====================

// findDependentRecords 递归查找所有依赖于给定outputIDs的后续操作
func findDependentRecords(db *gorm.DB, tableName string, outputIDs []int32, afterRecordID int64) []models.GeoRecord {
	if len(outputIDs) == 0 {
		return nil
	}

	seen := make(map[int64]bool)
	return findDependentRecordsRecursive(db, tableName, outputIDs, afterRecordID, seen)
}

func findDependentRecordsRecursive(db *gorm.DB, tableName string, outputIDs []int32, afterRecordID int64, seen map[int64]bool) []models.GeoRecord {
	if len(outputIDs) == 0 {
		return nil
	}

	var allDependents []models.GeoRecord

	for _, oid := range outputIDs {
		var records []models.GeoRecord
		// 使用jsonb @> 操作符查找InputIDs中包含该oid的记录
		db.Where("table_name = ? AND id > ? AND input_ids @> ?",
			tableName, afterRecordID, fmt.Sprintf("[%d]", oid)).
			Find(&records)

		for _, r := range records {
			if !seen[r.ID] {
				seen[r.ID] = true
				allDependents = append(allDependents, r)

				// 递归查找更深层依赖
				var depOutputIDs []int32
				json.Unmarshal(r.OutputIDs, &depOutputIDs)
				deeper := findDependentRecordsRecursive(db, tableName, depOutputIDs, r.ID, seen)
				allDependents = append(allDependents, deeper...)
			}
		}
	}

	return allDependents
}

// ==================== 单条回退 ====================

func rollbackSingleRecord(db *gorm.DB, record models.GeoRecord) error {
	var inputIDs []int32
	var outputIDs []int32
	json.Unmarshal(record.InputIDs, &inputIDs)
	json.Unmarshal(record.OutputIDs, &outputIDs)

	switch record.Type {
	case "要素添加":
		// 添加的回退：删除新要素
		for _, oid := range outputIDs {
			db.Table(record.TableName).Where("id = ?", oid).Delete(nil)
		}
		// 映射：删除新要素的映射
		deleteDerivedMappings(db, record.TableName, outputIDs)

		var fc geojson.FeatureCollection
		json.Unmarshal(record.NewGeojson, &fc)
		if len(fc.Features) > 0 {
			pgmvt.DelMVT(db, record.TableName, fc.Features[0].Geometry)
		}

	case "要素删除":
		// 删除的回退：恢复原要素
		var fc geojson.FeatureCollection
		json.Unmarshal(record.OldGeojson, &fc)
		methods.SavaGeojsonToTable(db, fc, record.TableName)
		// 映射：恢复输入要素的映射
		RestoreMappingActive(db, record.TableName, inputIDs)
		pgmvt.DelMVTALL(db, record.TableName)

	case "批量要素删除":
		var fc geojson.FeatureCollection
		json.Unmarshal(record.OldGeojson, &fc)
		methods.SavaGeojsonToTable(db, fc, record.TableName)
		RestoreMappingActive(db, record.TableName, inputIDs)
		pgmvt.DelMVTALL(db, record.TableName)

	case "要素修改", "要素环岛构造":
		// 修改的回退：用旧数据覆盖
		var oldFC geojson.FeatureCollection
		json.Unmarshal(record.OldGeojson, &oldFC)
		methods.UpdateGeojsonToTable(db, oldFC, record.TableName, record.GeoID)
		// 修改不改变映射关系，无需处理映射
		pgmvt.DelMVTALL(db, record.TableName)

	case "要素分割", "要素打散", "要素批量打散":
		// 分割/打散的回退：删除新要素，恢复原要素
		for _, oid := range outputIDs {
			db.Table(record.TableName).Where("id = ?", oid).Delete(nil)
		}
		var fc geojson.FeatureCollection
		json.Unmarshal(record.OldGeojson, &fc)
		methods.SavaGeojsonToTable(db, fc, record.TableName)
		// 映射：删除派生映射，恢复原映射
		deleteDerivedMappings(db, record.TableName, outputIDs)
		RestoreMappingActive(db, record.TableName, inputIDs)
		pgmvt.DelMVTALL(db, record.TableName)

	case "要素合并", "要素聚合":
		// 合并/聚合的回退：删除合并后的要素，恢复原要素
		for _, oid := range outputIDs {
			db.Table(record.TableName).Where("id = ?", oid).Delete(nil)
		}
		var fc geojson.FeatureCollection
		json.Unmarshal(record.OldGeojson, &fc)
		methods.SavaGeojsonToTable(db, fc, record.TableName)
		deleteDerivedMappings(db, record.TableName, outputIDs)
		RestoreMappingActive(db, record.TableName, inputIDs)
		pgmvt.DelMVTALL(db, record.TableName)

	case "要素平移":
		// 平移的回退：用旧几何覆盖
		var oldFC geojson.FeatureCollection
		json.Unmarshal(record.OldGeojson, &oldFC)
		// 平移是原地更新，逐个恢复
		for _, feature := range oldFC.Features {
			var id int32
			switch v := feature.Properties["id"].(type) {
			case float64:
				id = int32(v)
			case int:
				id = int32(v)
			case int32:
				id = v
			default:
				log.Printf("unexpected type for id: %T", v)
				continue
			}
			singleFC := geojson.FeatureCollection{}
			singleFC.Features = append(singleFC.Features, feature)
			methods.UpdateGeojsonToTable(db, singleFC, record.TableName, id)
		}
		// 平移不改变映射关系
		pgmvt.DelMVTALL(db, record.TableName)

	case "面要素去重叠":
		// 去重叠的回退：删除分析结果，恢复原要素
		for _, oid := range outputIDs {
			db.Table(record.TableName).Where("id = ?", oid).Delete(nil)
		}
		var fc geojson.FeatureCollection
		json.Unmarshal(record.OldGeojson, &fc)
		methods.SavaGeojsonToTable(db, fc, record.TableName)
		deleteDerivedMappings(db, record.TableName, outputIDs)
		RestoreMappingActive(db, record.TableName, inputIDs)
		pgmvt.DelMVTALL(db, record.TableName)

	default:
		log.Printf("未知的操作类型: %s", record.Type)
		return fmt.Errorf("未知的操作类型: %s", record.Type)
	}

	// 删除该条记录
	db.Delete(&record)
	return nil
}

// deleteDerivedMappings 删除派生要素的映射记录
func deleteDerivedMappings(db *gorm.DB, tableName string, postGISIDs []int32) {
	if len(postGISIDs) == 0 {
		return
	}
	db.Where("table_name = ? AND post_gis_id IN ?", tableName, postGISIDs).
		Delete(&models.OriginMapping{})
}

// ==================== 回退入口 ====================

func (uc *UserController) BackUpRecord(c *gin.Context) {
	ID := c.Query("ID")
	DB := models.DB

	var record models.GeoRecord
	if err := DB.Where("id = ?", ID).First(&record).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    404,
			"message": "未找到指定的编辑记录",
		})
		return
	}

	// 获取当前记录的输出IDs
	var outputIDs []int32
	json.Unmarshal(record.OutputIDs, &outputIDs)

	// 查找所有依赖于当前操作输出的后续操作
	dependentRecords := findDependentRecords(DB, record.TableName, outputIDs, record.ID)

	// 构建需要回退的完整列表（包含当前记录）
	allRecordsToRollback := make([]models.GeoRecord, 0, len(dependentRecords)+1)
	allRecordsToRollback = append(allRecordsToRollback, dependentRecords...)
	allRecordsToRollback = append(allRecordsToRollback, record)

	// 按ID倒序排列（最新的先回退）
	sort.Slice(allRecordsToRollback, func(i, j int) bool {
		return allRecordsToRollback[i].ID > allRecordsToRollback[j].ID
	})

	// 逐条回退
	rollbackErrors := make([]string, 0)
	rollbackCount := 0

	for _, rec := range allRecordsToRollback {
		if err := rollbackSingleRecord(DB, rec); err != nil {
			rollbackErrors = append(rollbackErrors, fmt.Sprintf("回退记录ID=%d失败: %v", rec.ID, err))
		} else {
			rollbackCount++
		}
	}

	if len(rollbackErrors) > 0 {
		c.JSON(http.StatusOK, gin.H{
			"code":         207,
			"message":      "部分回退成功",
			"rolled_back":  rollbackCount,
			"total":        len(allRecordsToRollback),
			"errors":       rollbackErrors,
			"had_cascades": len(dependentRecords) > 0,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":         200,
		"message":      "回退成功",
		"rolled_back":  rollbackCount,
		"had_cascades": len(dependentRecords) > 0, "cascade_info": func() string {
			if len(dependentRecords) > 0 {
				return fmt.Sprintf("级联回退了%d条依赖操作", len(dependentRecords))
			}
			return ""
		}(),
	})
}
