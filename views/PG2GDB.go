package views

import (
	"encoding/json"
	"fmt"
	"github.com/GrainArc/Gogeo"
	"github.com/GrainArc/SouceMap/config"
	"github.com/GrainArc/SouceMap/models"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"net/http"
	"path/filepath"
	"strings"
)

// SourceConfig 源配置结构
type SourceConfig struct {
	SourcePath      string               `json:"source_path"`
	SourceLayerName string               `json:"source_layer_name"`
	SourceLayerSRS  int                  `json:"source_layer_srs"`
	KeyAttribute    string               `json:"key_attribute"`
	AttMap          []ProcessedFieldInfo `json:"att_map"`
}

// ProcessedFieldInfo 字段映射信息
type ProcessedFieldInfo struct {
	OriginalName  string `json:"OriginalName"`
	ProcessedName string `json:"ProcessedName"`
	DBType        string `json:"DBType"`
}

// ImportPGToGDBRequest 导入请求参数
type ImportPGToGDBRequest struct {
	TargetMain string   `json:"target_main"` // 目标Main参数，用于查询目标GDB路径
	TableNames []string `json:"table_names"` // PG表名列表（查询MySchema的EN字段）
}

// LayerImportResult 单个图层导入结果
type LayerImportResult struct {
	TableName     string `json:"table_name"`
	LayerName     string `json:"layer_name"`
	Success       bool   `json:"success"`
	Message       string `json:"message"`
	TotalCount    int    `json:"total_count"`
	InsertedCount int    `json:"inserted_count"`
	FailedCount   int    `json:"failed_count"`
	SkippedCount  int    `json:"skipped_count"`
}

// ImportPGToGDBResponse 导入响应
type ImportPGToGDBResponse struct {
	Success      bool                 `json:"success"`
	Message      string               `json:"message"`
	GDBPath      string               `json:"gdb_path"`
	TotalLayers  int                  `json:"total_layers"`
	SuccessCount int                  `json:"success_count"`
	FailedCount  int                  `json:"failed_count"`
	Results      []*LayerImportResult `json:"results"`
}

// ImportPGToGDB 将多个PG表导入到GDB
func ImportPGToGDB(req *ImportPGToGDBRequest) (*ImportPGToGDBResponse, error) {
	DB := models.DB

	// 参数校验
	if len(req.TableNames) == 0 {
		return nil, fmt.Errorf("必须提供table_names")
	}
	if req.TargetMain == "" {
		return nil, fmt.Errorf("必须提供target_main")
	}

	// 1. 根据TargetMain查询目标GDB的配置
	var targetSchema models.MySchema
	err := DB.Where("main LIKE ?", req.TargetMain+"%").First(&targetSchema).Error
	if err != nil {
		return nil, fmt.Errorf("查询目标GDB配置失败: %v", err)
	}

	// 2. 解析目标Schema的Source字段获取GDB路径
	targetSourceConfigs, err := parseSourceConfig(targetSchema.Source)
	if err != nil {
		return nil, fmt.Errorf("解析目标Source配置失败: %v", err)
	}
	if len(targetSourceConfigs) == 0 {
		return nil, fmt.Errorf("目标Source配置为空")
	}

	// 从目标source_path中提取GDB路径
	gdbPath := extractGDBPath(targetSourceConfigs[0].SourcePath)
	if gdbPath == "" {
		return nil, fmt.Errorf("无法从目标Source中解析GDB路径")
	}

	// 获取目标坐标系EPSG
	targetEPSG := targetSourceConfigs[0].SourceLayerSRS
	if targetEPSG == 0 {
		// 如果目标配置中没有EPSG，使用默认值
		targetEPSG = 4490 // CGCS2000
	}

	// 3. 构建PostGIS基础配置
	basePostGISConfig := &Gogeo.PostGISConfig{
		Host:     getEnvOrDefault("PG_HOST", config.MainConfig.Host),
		Port:     getEnvOrDefault("PG_PORT", config.MainConfig.Port),
		Database: getEnvOrDefault("PG_DATABASE", config.MainConfig.Dbname),
		User:     getEnvOrDefault("PG_USER", config.MainConfig.Username),
		Password: getEnvOrDefault("PG_PASSWORD", config.MainConfig.Password),
		Schema:   "public",
	}

	// 4. 遍历导入每个表
	response := &ImportPGToGDBResponse{
		GDBPath:     gdbPath,
		TotalLayers: len(req.TableNames),
		Results:     make([]*LayerImportResult, 0, len(req.TableNames)),
	}

	for _, tableName := range req.TableNames {
		// 传递目标信息给单表导入函数
		result := importSingleTable(DB, basePostGISConfig, gdbPath, tableName, req.TargetMain,
			targetSourceConfigs[0].SourcePath, targetEPSG)
		response.Results = append(response.Results, result)

		if result.Success {
			response.SuccessCount++
		} else {
			response.FailedCount++
		}
	}

	// 设置总体结果
	response.Success = response.FailedCount == 0
	if response.Success {
		response.Message = fmt.Sprintf("全部导入成功，共 %d 个图层", response.SuccessCount)
	} else {
		response.Message = fmt.Sprintf("导入完成，成功 %d 个，失败 %d 个", response.SuccessCount, response.FailedCount)
	}

	return response, nil
}

// importSingleTable 导入单个表
func importSingleTable(DB *gorm.DB, baseConfig *Gogeo.PostGISConfig, gdbPath, tableName,
	targetMain, targetSourcePath string, targetEPSG int) *LayerImportResult {
	result := &LayerImportResult{
		TableName: tableName,
	}

	// 1. 查询源表的MySchema配置
	var sourceSchema models.MySchema
	err := DB.Where("LOWER(en) = ?", strings.ToLower(tableName)).First(&sourceSchema).Error
	if err != nil {
		result.Success = false
		result.Message = fmt.Sprintf("查询源表Schema配置失败: %v", err)
		return result
	}

	// 2. 解析源表的Source字段
	sourceConfigs, err := parseSourceConfig(sourceSchema.Source)
	if err != nil {
		result.Success = false
		result.Message = fmt.Sprintf("解析源表Source配置失败: %v", err)
		return result
	}
	if len(sourceConfigs) == 0 {
		result.Success = false
		result.Message = "源表Source配置为空"
		return result
	}
	sourceConfig := sourceConfigs[0]

	// 3. 目标图层名使用source_layer_name
	layerName := sourceConfig.SourceLayerName
	result.LayerName = layerName

	// 4. 计算LayerPath - 使用目标的Main和SourcePath
	layerPath := calculateLayerPath(targetMain, targetSourcePath)

	// 5. 构建字段映射
	fieldMapping := buildFieldMapping(sourceConfig.AttMap)

	// 6. 获取字段别名映射
	fieldAliases := GetCEMap(sourceSchema.EN)
	if fieldAliases == nil {
		fieldAliases = make(map[string]string)
	}

	// 7. 确定PG表名
	pgTableName := strings.ToLower(sourceSchema.EN)

	// 8. 构建PostGIS配置（复制基础配置并设置表名）
	postGISConfig := &Gogeo.PostGISConfig{
		Host:     baseConfig.Host,
		Port:     baseConfig.Port,
		Database: baseConfig.Database,
		User:     baseConfig.User,
		Password: baseConfig.Password,
		Schema:   baseConfig.Schema,
		Table:    pgTableName,
	}

	// 9. 构建导入选项
	options := Gogeo.NewImportToGDBOptionsV3().
		WithLayerAlias(sourceSchema.CN).
		WithLayerPath(layerPath).
		WithFieldAliases(fieldAliases)

	options.FieldMapping = fieldMapping

	// 10. 设置目标坐标系（使用目标配置的EPSG，而不是源配置的EPSG）
	if targetEPSG > 0 {
		options.TargetSRS = Gogeo.NewGDBSpatialReferenceFromEPSG(targetEPSG)
	} else {
		options.TargetSRS = Gogeo.SRS_CGCS2000
	}

	// 11. 执行导入
	importResult, err := Gogeo.ImportPostGISToGDBV3Auto(postGISConfig, gdbPath, layerName, options)
	if err != nil {
		result.Success = false
		result.Message = fmt.Sprintf("导入失败: %v", err)
		return result
	}

	// 12. 更新源表的Main字段
	err = DB.Model(&models.MySchema{}).
		Where("LOWER(en) = ?", strings.ToLower(tableName)).
		Update("main", targetMain).Error
	if err != nil {
		result.Success = false
		result.Message = fmt.Sprintf("导入成功但更新Main字段失败: %v", err)
		return result
	}

	result.Success = true
	result.Message = "导入成功"
	result.TotalCount = importResult.TotalCount
	result.InsertedCount = importResult.InsertedCount
	result.FailedCount = importResult.FailedCount
	result.SkippedCount = importResult.SkippedCount

	return result
}

// parseSourceConfig 解析Source JSON字段
func parseSourceConfig(sourceJSON interface{}) ([]SourceConfig, error) {
	var configs []SourceConfig

	var jsonBytes []byte
	var err error

	switch v := sourceJSON.(type) {
	case []byte:
		jsonBytes = v
	case string:
		jsonBytes = []byte(v)
	case json.RawMessage:
		jsonBytes = v
	default:
		jsonBytes, err = json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("无法序列化Source字段: %v", err)
		}
	}

	err = json.Unmarshal(jsonBytes, &configs)
	if err != nil {
		var singleConfig SourceConfig
		if err2 := json.Unmarshal(jsonBytes, &singleConfig); err2 == nil {
			configs = append(configs, singleConfig)
		} else {
			return nil, fmt.Errorf("解析Source JSON失败: %v", err)
		}
	}

	return configs, nil
}

// extractGDBPath 从source_path中提取GDB路径
func extractGDBPath(sourcePath string) string {
	if sourcePath == "" {
		return ""
	}

	normalizedPath := filepath.ToSlash(sourcePath)
	gdbIndex := strings.Index(strings.ToLower(normalizedPath), ".gdb")
	if gdbIndex == -1 {
		return ""
	}

	gdbPath := normalizedPath[:gdbIndex+4]
	return filepath.FromSlash(gdbPath)
}

// calculateLayerPath 计算LayerPath - 使用目标的Main和SourcePath
func calculateLayerPath(main string, sourcePath string) string {
	if main == "" || sourcePath == "" {
		return ""
	}
	fmt.Println("计算路径", main, sourcePath)

	baseName := filepath.Base(sourcePath)
	gdbName := strings.TrimSuffix(baseName, filepath.Ext(baseName))
	if gdbName == "" {
		return ""
	}
	// 统一路径分隔符为 /
	normalizedMain := strings.ReplaceAll(main, "\\", "/")
	// 查找 gdbName 在 main 中的位置
	idx := strings.Index(normalizedMain, gdbName)
	if idx == -1 {
		return ""
	}
	// 截取 gdbName 之后的部分
	// 例如: aaaa/510300自贡市_CSJC2024/XHDataset -> XHDataset
	remaining := normalizedMain[idx+len(gdbName):]

	// 去除开头的分隔符
	remaining = strings.TrimPrefix(remaining, "/")
	fmt.Println(remaining)
	return remaining
}

// buildFieldMapping 构建字段映射（PG字段名 -> GDB字段名）
func buildFieldMapping(attMap []ProcessedFieldInfo) map[string]string {
	mapping := make(map[string]string)

	for _, field := range attMap {
		if field.ProcessedName != "" && field.OriginalName != "" {
			mapping[field.ProcessedName] = field.OriginalName
		}
	}

	return mapping
}

// getEnvOrDefault 获取环境变量或默认值
func getEnvOrDefault(key, defaultValue string) string {
	return defaultValue
}

// ImportPGToGDBHandler 导入处理器
func (uc *UserController) ImportPGToGDBHandler(c *gin.Context) {
	var req ImportPGToGDBRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": fmt.Sprintf("参数解析失败: %v", err),
		})
		return
	}

	resp, err := ImportPGToGDB(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, resp)
}
