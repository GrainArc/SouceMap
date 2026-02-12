// views/sync_to_file.go
package views

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/GrainArc/Gogeo"
	"github.com/GrainArc/SouceMap/config"
	"github.com/GrainArc/SouceMap/models"
	"github.com/GrainArc/SouceMap/pgmvt"
	"github.com/gin-gonic/gin"
)

func (uc *UserController) SyncToFile(c *gin.Context) {
	TableName := c.Query("TableName")
	DB := models.DB

	var Schema models.MySchema
	if err := DB.Where("en = ?", TableName).First(&Schema).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": fmt.Sprintf("未找到图层配置: %v", err)})
		return
	}

	var sourceConfigs []pgmvt.SourceConfig
	if err := json.Unmarshal(Schema.Source, &sourceConfigs); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": fmt.Sprintf("解析源配置失败: %v", err)})
		return
	}

	sourceConfig := sourceConfigs[0]
	if sourceConfig.SourcePath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": "该图层没有绑定源文件"})
		return
	}
	Soucepath := sourceConfig.SourcePath
	SouceLayer := sourceConfig.SourceLayerName
	AttMap := sourceConfig.AttMap
	keyField := sourceConfig.KeyAttribute
	if keyField == "" {
		keyField = "OBJECTID"
	}

	ext := strings.ToLower(filepath.Ext(Soucepath))
	if ext != ".shp" && ext != ".gdb" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 500, "message": "不支持的文件格式,仅支持GDB和SHP"})
		return
	}

	var syncErrors []string
	var totalDeleted int
	var totalInserted int
	var totalUpdated int

	// ==================== 1. 删除：源文件中有映射且已被标记删除的要素 ====================
	var toDeleteMappings []models.OriginMapping
	DB.Where("table_name = ? AND source_object_id != -1 AND is_deleted = true",
		TableName).Find(&toDeleteMappings)

	if len(toDeleteMappings) > 0 {
		deleteIDs := make([]int32, len(toDeleteMappings))
		for i, m := range toDeleteMappings {
			deleteIDs[i] = int32(m.SourceObjectID)
		}
		whereClause := buildWhereClause(keyField, deleteIDs)

		var deletedCount int
		var err error
		if ext == ".shp" {
			deletedCount, err = Gogeo.DeleteShapeFeaturesByFilter(Soucepath, whereClause)
		} else if ext == ".gdb" {
			deletedCount, err = Gogeo.DeleteFeaturesByFilter(Soucepath, SouceLayer, whereClause)
		}

		if err != nil {
			syncErrors = append(syncErrors, fmt.Sprintf("删除源文件要素失败: %v", err))
		} else {
			totalDeleted += deletedCount
		}
	}

	// ==================== 2. 更新：源文件中有映射、未被删除、但被修改过的要素 ====================
	// 通过GeoRecord中Type="要素修改"或"要素平移"等原地更新操作来判断
	var modifiedRecords []models.GeoRecord
	DB.Where("table_name = ? AND type IN ?", TableName,
		[]string{"要素修改", "要素平移", "要素环岛构造"}).Find(&modifiedRecords)

	// 收集所有被原地修改过的PostGIS ID（去重）
	modifiedIDSet := make(map[int32]bool)
	for _, record := range modifiedRecords {
		var recInputIDs []int32
		json.Unmarshal(record.InputIDs, &recInputIDs)
		for _, pid := range recInputIDs {
			modifiedIDSet[pid] = true
		}
	}

	for pid := range modifiedIDSet {
		// 检查这个要素是否有源文件映射且未被删除
		var mapping models.OriginMapping
		err := DB.Where("table_name = ? AND post_gis_id = ? AND source_object_id != -1 AND is_deleted = false",
			TableName, pid).First(&mapping).Error
		if err != nil {
			continue // 没有源文件映射或已删除，跳过
		}

		// 先删后插
		whereClause := buildWhereClause(keyField, []int32{int32(mapping.SourceObjectID)})
		if ext == ".shp" {
			Gogeo.DeleteShapeFeaturesByFilter(Soucepath, whereClause)
		} else if ext == ".gdb" {
			Gogeo.DeleteFeaturesByFilter(Soucepath, SouceLayer, whereClause)
		}

		// 从PostGIS获取最新数据并插入
		getdata := getDatas{TableName: TableName, ID: []int32{pid}}
		geos := GetGeos(getdata)
		if len(geos.Features) > 0 {
			mappedFC, mapErr := mapFieldsToSource(&geos, AttMap)
			if mapErr != nil {
				syncErrors = append(syncErrors, fmt.Sprintf("字段映射失败(ID=%d): %v", pid, mapErr))
				continue
			}
			gdalLayer, convErr := Gogeo.ConvertGeoJSONToGDALLayer(mappedFC, SouceLayer)
			if convErr != nil {
				syncErrors = append(syncErrors, fmt.Sprintf("转换GeoJSON失败(ID=%d): %v", pid, convErr))
				continue
			}

			options := &Gogeo.InsertOptions{
				StrictMode:          false,
				SkipInvalidGeometry: true,
				CreateMissingFields: false,
			}
			if ext == ".shp" {
				if err := Gogeo.InsertLayerToShapefile(gdalLayer, Soucepath, options); err != nil {
					syncErrors = append(syncErrors, fmt.Sprintf("更新要素失败(ID=%d): %v", pid, err))
				} else {
					totalUpdated++
				}
			} else if ext == ".gdb" {
				if err := Gogeo.InsertLayerToGDB(gdalLayer, Soucepath, SouceLayer, options); err != nil {
					syncErrors = append(syncErrors, fmt.Sprintf("更新要素失败(ID=%d): %v", pid, err))
				} else {
					totalUpdated++
				}
			}
			if gdalLayer != nil {
				gdalLayer.Close()
			}
		}
	}

	// ==================== 3. 新增：源文件中无映射且未被删除的要素（派生要素） ====================
	var toInsertMappings []models.OriginMapping
	DB.Where("table_name = ? AND source_object_id = -1 AND is_deleted = false",
		TableName).Find(&toInsertMappings)

	if len(toInsertMappings) > 0 {
		insertIDs := make([]int32, len(toInsertMappings))
		for i, m := range toInsertMappings {
			insertIDs[i] = m.PostGISID
		}

		getdata := getDatas{TableName: TableName, ID: insertIDs}
		geos := GetGeos(getdata)

		if len(geos.Features) > 0 {
			mappedFC, mapErr := mapFieldsToSource(&geos, AttMap)
			if mapErr != nil {
				syncErrors = append(syncErrors, fmt.Sprintf("新增要素字段映射失败: %v", mapErr))
			} else {
				gdalLayer, convErr := Gogeo.ConvertGeoJSONToGDALLayer(mappedFC, SouceLayer)
				if convErr != nil {
					syncErrors = append(syncErrors, fmt.Sprintf("新增要素转换失败: %v", convErr))
				} else {
					options := &Gogeo.InsertOptions{
						StrictMode:          false,
						SyncInterval:        100,
						SkipInvalidGeometry: true,
						CreateMissingFields: false,
					}

					if ext == ".shp" {
						if err := Gogeo.InsertLayerToShapefile(gdalLayer, Soucepath, options); err != nil {
							syncErrors = append(syncErrors, fmt.Sprintf("插入新要素失败: %v", err))
						} else {
							totalInserted += len(geos.Features)
						}
					} else if ext == ".gdb" {
						if err := Gogeo.InsertLayerToGDB(gdalLayer, Soucepath, SouceLayer, options); err != nil {
							syncErrors = append(syncErrors, fmt.Sprintf("插入新要素失败: %v", err))
						} else {
							totalInserted += len(geos.Features)
						}
					}
					if gdalLayer != nil {
						gdalLayer.Close()
					}
				}
			}
		}
	}

	// ==================== 4. 同步字段操作 ====================
	var FieldRecord []models.FieldRecord
	DB.Where("table_name = ?", TableName).Find(&FieldRecord)

	postGISConfig := &Gogeo.PostGISConfig{
		Host:     config.MainConfig.Host,
		Port:     config.MainConfig.Port,
		Database: config.MainConfig.Dbname,
		User:     config.MainConfig.Username,
		Password: config.MainConfig.Password,
		Schema:   "public",
		Table:    TableName,
	}

	if len(FieldRecord) >= 1 && ext == ".gdb" {
		for _, record := range FieldRecord {
			switch record.Type {
			case "value":
				options := &Gogeo.SyncFieldOptions{
					SourceField:      record.OldFieldName,
					TargetField:      mapField(record.OldFieldName, AttMap),
					SourceIDField:    "fid",
					TargetIDField:    "FID",
					BatchSize:        1000,
					UseTransaction:   true,
					UpdateNullValues: false,
				}
				_, err := Gogeo.SyncFieldFromPostGIS(postGISConfig, sourceConfig.SourcePath, SouceLayer, options)
				if err != nil {
					syncErrors = append(syncErrors, fmt.Sprintf("同步字段值失败(%s): %v", record.OldFieldName, err))
				}

			case "add":
				gdbFieldType, width, precision := mapPostGISTypeToGDB(record.NewFieldType)
				fieldDef := Gogeo.FieldDefinition{
					Name:      record.NewFieldName,
					Type:      gdbFieldType,
					Width:     width,
					Precision: precision,
					Nullable:  true,
					Default:   nil,
				}
				if err := Gogeo.AddField(Soucepath, SouceLayer, fieldDef); err != nil {
					syncErrors = append(syncErrors, fmt.Sprintf("添加字段失败(%s): %v", record.NewFieldName, err))
				}

			case "modify":
				oldFieldName := mapField(record.OldFieldName, AttMap)
				if err := Gogeo.DeleteField(Soucepath, SouceLayer, oldFieldName); err != nil {
					syncErrors = append(syncErrors, fmt.Sprintf("删除旧字段失败(%s): %v", oldFieldName, err))
					continue
				}
				gdbFieldType, width, precision := mapPostGISTypeToGDB(record.NewFieldType)
				fieldDef := Gogeo.FieldDefinition{
					Name:      record.NewFieldName,
					Type:      gdbFieldType,
					Width:     width,
					Precision: precision,
					Nullable:  true,
					Default:   nil,
				}
				if err := Gogeo.AddField(Soucepath, SouceLayer, fieldDef); err != nil {
					syncErrors = append(syncErrors, fmt.Sprintf("创建新字段失败(%s): %v", record.NewFieldName, err))
				}

			case "delete":
				if err := Gogeo.DeleteField(Soucepath, SouceLayer, mapField(record.OldFieldName, AttMap)); err != nil {
					syncErrors = append(syncErrors, fmt.Sprintf("删除字段失败(%s): %v", record.OldFieldName, err))
				}
			}
		}
	}

	// ==================== 5. 清理并重建映射表 ====================
	DB.Where("table_name = ?", TableName).Delete(&models.OriginMapping{})
	DB.Where("table_name = ?", TableName).Delete(&models.GeoRecord{})
	DB.Where("table_name = ?", TableName).Delete(&models.FieldRecord{})
	DB.Where("table_name = ?", TableName).Delete(&models.EditSession{})

	// 重新初始化映射
	mappingKeyField := "objectid"
	if ext == ".gdb" {
		mappingKeyField = "fid"
	}
	if err := InitOriginMapping(DB, TableName, mappingKeyField); err != nil {
		log.Printf("重建映射表失败: %v", err)
		syncErrors = append(syncErrors, fmt.Sprintf("重建映射表失败: %v", err))
	}

	// ==================== 6. 构建响应 ====================
	response := gin.H{
		"code":             200,
		"message":          "同步完成",
		"total_deleted":    totalDeleted,
		"total_inserted":   totalInserted,
		"total_updated":    totalUpdated,
		"mapping_deleted":  len(toDeleteMappings),
		"mapping_inserted": len(toInsertMappings),
	}

	if len(syncErrors) > 0 {
		response["code"] = 207
		response["message"] = "同步完成,但有部分错误"
		response["errors"] = syncErrors
	}

	c.JSON(http.StatusOK, response)
}
