package services

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/GrainArc/Gogeo"
	"github.com/GrainArc/SouceMap/config"
	"github.com/GrainArc/SouceMap/models"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"os"
	"path/filepath"
	"regexp"
)

// ClipRequest 裁剪请求参数
type ClipRequest struct {
	SourcePath  string  `json:"source_path" binding:"required"`
	NameField   string  `json:"name_field"`
	JPEGQuality int     `json:"jpeg_quality"`
	BufferDist  float64 `json:"buffer_dist"`
	ImageFormat string  `json:"image_format"`
	LayerName   string  `json:"layer_name" binding:"required"`
}

// ClipResponse 裁剪响应
type ClipResponse struct {
	TaskID     string `json:"task_id"`
	OutputPath string `json:"output_path"`
	Message    string `json:"message"`
}

// RasterService 栅格裁剪服务
type RasterService struct {
}

// StartClipTask 启动异步裁剪任务
func (s *RasterService) StartClipTask(req *ClipRequest) (*ClipResponse, error) {
	// 生成TaskID
	taskID := uuid.New().String()
	// 构建输出路径
	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("创建输出目录失败: %w", err)
	}
	// 设置默认值
	if req.NameField == "" {
		req.NameField = "NAME"
	}
	if req.JPEGQuality <= 0 || req.JPEGQuality > 100 {
		req.JPEGQuality = 85
	}
	if req.ImageFormat == "" {
		req.ImageFormat = "JPEG"
	}
	// 序列化参数
	argsJSON, _ := json.Marshal(req)
	// 创建记录
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputDir,
		Status:     0, // 运行中
		TypeName:   "clip",
		Args:       datatypes.JSON(argsJSON),
	}
	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}
	// 启动异步任务
	go s.executeClipTask(taskID, req, outputDir)
	return &ClipResponse{
		TaskID:     taskID,
		OutputPath: outputDir,
		Message:    "任务已提交",
	}, nil
}

// executeClipTask 执行裁剪任务
func (s *RasterService) executeClipTask(taskID string, req *ClipRequest, outputDir string) {
	var finalStatus int = 1 // 默认成功
	defer func() {
		if r := recover(); r != nil {
			finalStatus = 2 // 执行失败
		}
		// 更新任务状态
		models.DB.Model(&models.RasterRecord{}).Where("task_id = ?", taskID).Update("status", finalStatus)
	}()
	// 打开栅格数据集
	rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
	if err != nil {
		finalStatus = 2
		return
	}
	defer rd.Close()
	// 读取PostGIS图层
	reader := Gogeo.MakePGReader(req.LayerName)
	layer, err := reader.ReadGeometryTable()
	if err != nil {
		finalStatus = 2
		return
	}
	epsg := rd.GetEPSGCode()
	if epsg == 0 {
		epsg = 4490
	}
	reprojectLayer, err := layer.ReprojectLayer(epsg)
	if err != nil {
		finalStatus = 2
		return
	}

	defer layer.Close()

	// 构建裁剪选项
	options := &Gogeo.ClipOptions{
		OutputDir:         outputDir,
		NameField:         req.NameField,
		JPEGQuality:       req.JPEGQuality,
		BufferDist:        req.BufferDist,
		ImageFormat:       req.ImageFormat,
		OverwriteExisting: true,
	}
	// 执行裁剪
	_, err = rd.ClipRasterByLayer(reprojectLayer, options)
	if err != nil {
		finalStatus = 2
		return
	}

}

// GetTaskStatus 查询任务状态
func (s *RasterService) GetTaskStatus(taskID string) (*models.RasterRecord, error) {
	var record models.RasterRecord
	if err := models.DB.Where("task_id = ?", taskID).First(&record).Error; err != nil {
		return nil, err
	}
	return &record, nil
}

// MosaicRequest 镶嵌请求参数
type MosaicRequest struct {
	SourcePaths    []string `json:"source_paths" binding:"required,min=2"` // 输入文件路径列表
	OutputName     string   `json:"output_name"`                           // 输出文件名
	OutputFormat   string   `json:"output_format"`                         // 输出格式: GTiff, JPEG, PNG
	ResampleMethod int      `json:"resample_method"`                       // 重采样方法: 0-最近邻,1-双线性,2-三次卷积
	ForceBandMatch bool     `json:"force_band_match"`                      // 强制波段匹配
	NoDataValue    float64  `json:"nodata_value"`                          // NoData值
	HasNoData      bool     `json:"has_nodata"`                            // 是否设置NoData
	Priorities     []int    `json:"priorities"`                            // 优先级（可选）
}

// MosaicResponse 镶嵌响应
type MosaicResponse struct {
	TaskID     string `json:"task_id"`
	OutputPath string `json:"output_path"`
	Message    string `json:"message"`
}

// MosaicPreviewRequest 镶嵌预览请求
type MosaicPreviewRequest struct {
	SourcePaths    []string `json:"source_paths" binding:"required,min=2"`
	ForceBandMatch bool     `json:"force_band_match"`
}

// MosaicPreviewResponse 镶嵌预览响应
type MosaicPreviewResponse struct {
	MinX          float64 `json:"min_x"`
	MinY          float64 `json:"min_y"`
	MaxX          float64 `json:"max_x"`
	MaxY          float64 `json:"max_y"`
	ResX          float64 `json:"res_x"`
	ResY          float64 `json:"res_y"`
	Width         int     `json:"width"`
	Height        int     `json:"height"`
	BandCount     int     `json:"band_count"`
	DataType      string  `json:"data_type"`
	Projection    string  `json:"projection"`
	EstimatedSize int64   `json:"estimated_size"` // 预估大小(字节)
}

// 分页查询请求参数
type QueryRasterTasksRequest struct {
	Page     int    `json:"page"`
	PageSize int    `json:"pageSize"`
	Status   *int   `json:"status"` // 可选，按状态筛选
	TaskID   string `json:"taskId"` // 可选，按任务ID筛选
}

// 分页查询响应数据
type QueryRasterTasksResponse struct {
	Total    int64                 `json:"total"`
	Page     int                   `json:"page"`
	PageSize int                   `json:"page_size"`
	List     []models.RasterRecord `json:"list"`
}

// Service 层方法
func (s *RasterService) GetTaskList(page, pageSize int, status *int, taskID string) (*QueryRasterTasksResponse, error) {
	var total int64
	var records []models.RasterRecord
	db := models.DB
	query := db.Model(&models.RasterRecord{})

	// 条件筛选
	if status != nil {
		query = query.Where("status = ?", *status)
	}
	if taskID != "" {
		query = query.Where("task_id LIKE ?", "%"+taskID+"%")
	}

	// 获取总数
	if err := query.Count(&total).Error; err != nil {
		return nil, err
	}

	// 分页查询
	offset := (page - 1) * pageSize
	if err := query.Offset(offset).Limit(pageSize).
		Order("id DESC").
		Find(&records).Error; err != nil {
		return nil, err
	}

	return &QueryRasterTasksResponse{
		Total:    total,
		Page:     page,
		PageSize: pageSize,
		List:     records,
	}, nil
}

// StartMosaicTask 启动异步镶嵌任务
func (s *RasterService) StartMosaicTask(req *MosaicRequest) (*MosaicResponse, error) {
	// 验证输入文件
	for _, path := range req.SourcePaths {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return nil, fmt.Errorf("文件不存在: %s", path)
		}
	}
	// 生成TaskID
	taskID := uuid.New().String()
	// 构建输出路径
	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("创建输出目录失败: %w", err)
	}
	// 设置默认值
	if req.OutputFormat == "" {
		req.OutputFormat = "GTiff"
	}
	if req.OutputName == "" {
		req.OutputName = "mosaic"
	}
	// 确定输出文件扩展名
	ext := getFormatExtension(req.OutputFormat)
	outputPath := filepath.Join(outputDir, req.OutputName+ext)
	// 序列化参数
	argsJSON, _ := json.Marshal(req)
	// 创建记录
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePaths[0], // 存储第一个源文件
		OutputPath: outputPath,
		Status:     0, // 运行中
		TypeName:   "mosaic",
		Args:       datatypes.JSON(argsJSON),
	}
	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}
	// 启动异步任务
	go s.executeMosaicTask(taskID, req, outputPath)
	return &MosaicResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "镶嵌任务已提交",
	}, nil
}

// executeMosaicTask 执行镶嵌任务
func (s *RasterService) executeMosaicTask(taskID string, req *MosaicRequest, outputPath string) {
	var finalStatus int = 1 // 默认成功
	defer func() {
		if r := recover(); r != nil {
			finalStatus = 2
		}
		models.DB.Model(&models.RasterRecord{}).Where("task_id = ?", taskID).Update("status", finalStatus)
	}()
	// 构建镶嵌选项
	options := &Gogeo.MosaicOptions{
		ForceBandMatch: req.ForceBandMatch,
		ResampleMethod: Gogeo.ResampleMethod(req.ResampleMethod),
		NoDataValue:    req.NoDataValue,
		HasNoData:      req.HasNoData,
		NumThreads:     0, // 自动
	}
	// 判断是否使用优先级镶嵌
	if len(req.Priorities) == len(req.SourcePaths) {
		err := s.mosaicWithPriority(req, options, outputPath)
		if err != nil {
			finalStatus = 2
		}
		return
	}
	// 普通镶嵌
	err := Gogeo.MosaicFilesToFile(req.SourcePaths, outputPath, req.OutputFormat, options)
	if err != nil {
		finalStatus = 2
	}
}

// mosaicWithPriority 带优先级的镶嵌
func (s *RasterService) mosaicWithPriority(req *MosaicRequest, baseOptions *Gogeo.MosaicOptions, outputPath string) error {
	// 打开所有数据集
	datasets := make([]*Gogeo.RasterDataset, 0, len(req.SourcePaths))
	for _, path := range req.SourcePaths {
		ds, err := Gogeo.OpenRasterDataset(path, false)
		if err != nil {
			for _, d := range datasets {
				d.Close()
			}
			return err
		}
		datasets = append(datasets, ds)
	}
	defer func() {
		for _, ds := range datasets {
			ds.Close()
		}
	}()
	// 构建优先级选项
	priorityOptions := &Gogeo.PriorityMosaicOptions{
		MosaicOptions: *baseOptions,
		Priorities:    req.Priorities,
	}
	// 执行优先级镶嵌
	result, err := Gogeo.MosaicDatasetsWithPriority(datasets, priorityOptions)
	if err != nil {
		return err
	}
	defer result.Close()
	return result.ExportToFile(outputPath, req.OutputFormat, nil)
}

// GetMosaicPreview 获取镶嵌预览信息
func (s *RasterService) GetMosaicPreview(req *MosaicPreviewRequest) (*MosaicPreviewResponse, error) {
	// 打开所有数据集
	datasets := make([]*Gogeo.RasterDataset, 0, len(req.SourcePaths))
	for _, path := range req.SourcePaths {
		ds, err := Gogeo.OpenRasterDataset(path, false)
		if err != nil {
			for _, d := range datasets {
				d.Close()
			}
			return nil, fmt.Errorf("打开文件失败 %s: %w", path, err)
		}
		datasets = append(datasets, ds)
	}
	defer func() {
		for _, ds := range datasets {
			ds.Close()
		}
	}()
	// 构建选项
	options := &Gogeo.MosaicOptions{
		ForceBandMatch: req.ForceBandMatch,
	}
	// 验证输入
	if err := Gogeo.ValidateMosaicInputs(datasets, options); err != nil {
		return nil, fmt.Errorf("输入验证失败: %w", err)
	}
	// 获取镶嵌信息
	info, err := Gogeo.GetMosaicInfo(datasets, options)
	if err != nil {
		return nil, fmt.Errorf("获取镶嵌信息失败: %w", err)
	}
	// 估算大小
	estimatedSize, _ := Gogeo.EstimateMosaicSize(datasets, options)
	return &MosaicPreviewResponse{
		MinX:          info.MinX,
		MinY:          info.MinY,
		MaxX:          info.MaxX,
		MaxY:          info.MaxY,
		ResX:          info.ResX,
		ResY:          info.ResY,
		Width:         info.Width,
		Height:        info.Height,
		BandCount:     info.BandCount,
		DataType:      info.DataType,
		Projection:    info.Projection,
		EstimatedSize: estimatedSize,
	}, nil
}

// getFormatExtension 获取格式对应的扩展名
func getFormatExtension(format string) string {
	switch format {
	case "GTiff":
		return ".tif"
	case "JPEG":
		return ".jpg"
	case "PNG":
		return ".png"
	case "HFA":
		return ".img"
	default:
		return ".tif"
	}
}

//投影相关

// DefineProjectionRequest 定义投影请求
type DefineProjectionRequest struct {
	SourcePath string `json:"source_path" binding:"required"` // 源文件路径
	EPSG       int    `json:"epsg" binding:"required"`        // EPSG代码
	InPlace    bool   `json:"in_place"`                       // 是否直接修改原文件，默认false生成新文件
	OutputName string `json:"output_name"`                    // 输出文件名（不含扩展名）
}

// DefineProjectionWithGeoTransformRequest 定义投影并设置地理变换请求
type DefineProjectionWithGeoTransformRequest struct {
	SourcePath   string     `json:"source_path" binding:"required"`   // 源文件路径
	EPSG         int        `json:"epsg" binding:"required"`          // EPSG代码
	GeoTransform [6]float64 `json:"geo_transform" binding:"required"` // 地理变换参数
	InPlace      bool       `json:"in_place"`                         // 是否直接修改原文件
	OutputName   string     `json:"output_name"`                      // 输出文件名
}

// ReprojectRequest 重投影请求
type ReprojectRequest struct {
	SourcePath     string `json:"source_path" binding:"required"` // 源文件路径
	SrcEPSG        int    `json:"src_epsg"`                       // 源EPSG代码（0表示自动检测）
	DstEPSG        int    `json:"dst_epsg" binding:"required"`    // 目标EPSG代码
	OutputName     string `json:"output_name"`                    // 输出文件名
	OutputFormat   string `json:"output_format"`                  // 输出格式：GTiff, JP2OpenJPEG等
	ResampleMethod int    `json:"resample_method"`                // 重采样方法：0-最近邻,1-双线性,2-三次卷积,3-三次样条,4-Lanczos
}

// ProjectionResponse 投影操作响应
type ProjectionResponse struct {
	TaskID     string `json:"task_id"`
	OutputPath string `json:"output_path"`
	Message    string `json:"message"`
}

// ProjectionInfo 投影信息
type ProjectionInfo struct {
	Width        int        `json:"width"`
	Height       int        `json:"height"`
	BandCount    int        `json:"band_count"`
	Projection   string     `json:"projection"`
	GeoTransform [6]float64 `json:"geo_transform"`
	HasGeoInfo   bool       `json:"has_geo_info"`
	Bounds       [4]float64 `json:"bounds"` // minX, minY, maxX, maxY
}

// ==================== 服务方法 ====================
// 解析 srtext 提取坐标系名称
func parseSRTextName(srtext string) string {
	// 匹配 PROJCS["名称",...] 或 GEOGCS["名称",...]
	re := regexp.MustCompile(`^(?:PROJCS|GEOGCS)\["([^"]+)"`)
	matches := re.FindStringSubmatch(srtext)
	if len(matches) > 1 {
		return matches[1]
	}
	return ""
}

// GetSpatialRefByEPSG 根据EPSG代码查询坐标系名称
// 参数: epsg - EPSG代码 (例如: 4326)

func GetSpatialRefByEPSG(epsg int) (string, error) {
	DB := models.DB
	type rawSpatialRef struct {
		SRID   int    `gorm:"column:srid"` // 或试试 auth_srid
		SRText string `gorm:"column:srtext"`
	}
	var raw rawSpatialRef

	// 根据 SRID 查询
	if err := DB.Table("spatial_ref_sys").
		Select("srid, srtext").
		Where("srid = ?", epsg).
		First(&raw).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", fmt.Errorf("EPSG代码 %d 不存在", epsg)
		}
		return "", err
	}

	// 解析坐标系名称
	name := parseSRTextName(raw.SRText)
	if name == "" {
		return "", fmt.Errorf("无法解析EPSG代码 %d 的坐标系名称", epsg)
	}

	return name, nil
}

// GetProjectionInfo 获取栅格投影信息
func (s *RasterService) GetProjectionInfo(sourcePath string) (*ProjectionInfo, error) {
	rd, err := Gogeo.OpenRasterDataset(sourcePath, false)
	if err != nil {
		return nil, fmt.Errorf("打开栅格文件失败: %w", err)
	}
	defer rd.Close()

	info := rd.GetInfo()
	Projection := ""
	epsg := rd.GetEPSGCode()
	if epsg != 0 {
		Projection, _ = GetSpatialRefByEPSG(epsg)
	}

	minX, minY, maxX, maxY := rd.GetBounds()

	return &ProjectionInfo{
		Width:        info.Width,
		Height:       info.Height,
		BandCount:    info.BandCount,
		Projection:   Projection,
		GeoTransform: info.GeoTransform,
		HasGeoInfo:   info.HasGeoInfo,
		Bounds:       [4]float64{minX, minY, maxX, maxY},
	}, nil
}

// StartDefineProjectionTask 启动定义投影任务
func (s *RasterService) StartDefineProjectionTask(req *DefineProjectionRequest) (*ProjectionResponse, error) {
	taskID := uuid.New().String()

	var outputPath string
	if req.InPlace {
		outputPath = req.SourcePath
	} else {
		outputDir := filepath.Join(config.MainConfig.Download, taskID)
		if err := createDirIfNotExist(outputDir); err != nil {
			return nil, err
		}
		outputName := req.OutputName
		if outputName == "" {
			outputName = "defined_projection"
		}
		outputPath = filepath.Join(outputDir, outputName+".tif")
	}

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "define_projection",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeDefineProjectionTask(taskID, req, outputPath)

	return &ProjectionResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

// executeDefineProjectionTask 执行定义投影任务
func (s *RasterService) executeDefineProjectionTask(taskID string, req *DefineProjectionRequest, outputPath string) {
	var finalStatus int = 1
	defer func() {
		if r := recover(); r != nil {
			finalStatus = 2
		}
		models.DB.Model(&models.RasterRecord{}).Where("task_id = ?", taskID).Update("status", finalStatus)
	}()

	rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
	if err != nil {
		finalStatus = 2
		return
	}
	defer rd.Close()

	if req.InPlace {
		// 直接修改原文件
		if err := rd.DefineProjection(req.EPSG); err != nil {
			finalStatus = 2
			return
		}
	} else {
		// 创建内存副本后导出
		newRD, err := rd.DefineProjectionToMemory(req.EPSG)
		if err != nil {
			finalStatus = 2
			return
		}
		defer newRD.Close()

		if err := newRD.ExportToFile(outputPath, "GTiff", nil); err != nil {
			finalStatus = 2
			return
		}
	}
}

// StartDefineProjectionWithGeoTransformTask 启动定义投影+地理变换任务
func (s *RasterService) StartDefineProjectionWithGeoTransformTask(req *DefineProjectionWithGeoTransformRequest) (*ProjectionResponse, error) {
	taskID := uuid.New().String()

	var outputPath string
	if req.InPlace {
		outputPath = req.SourcePath
	} else {
		outputDir := filepath.Join(config.MainConfig.Download, taskID)
		if err := createDirIfNotExist(outputDir); err != nil {
			return nil, err
		}
		outputName := req.OutputName
		if outputName == "" {
			outputName = "defined_geotransform"
		}
		outputPath = filepath.Join(outputDir, outputName+".tif")
	}

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "define_projection_geotransform",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeDefineProjectionWithGeoTransformTask(taskID, req, outputPath)

	return &ProjectionResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

// executeDefineProjectionWithGeoTransformTask 执行定义投影+地理变换任务
func (s *RasterService) executeDefineProjectionWithGeoTransformTask(taskID string, req *DefineProjectionWithGeoTransformRequest, outputPath string) {
	var finalStatus int = 1
	defer func() {
		if r := recover(); r != nil {
			finalStatus = 2
		}
		models.DB.Model(&models.RasterRecord{}).Where("task_id = ?", taskID).Update("status", finalStatus)
	}()

	rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
	if err != nil {
		finalStatus = 2
		return
	}
	defer rd.Close()

	if req.InPlace {
		if err := rd.DefineProjectionWithGeoTransform(req.EPSG, req.GeoTransform); err != nil {
			finalStatus = 2
			return
		}
	} else {
		// 先定义投影到内存
		newRD, err := rd.DefineProjectionToMemory(req.EPSG)
		if err != nil {
			finalStatus = 2
			return
		}
		defer newRD.Close()

		// 导出文件
		if err := newRD.ExportToFile(outputPath, "GTiff", nil); err != nil {
			finalStatus = 2
			return
		}
	}
}

// StartReprojectTask 启动重投影任务
func (s *RasterService) StartReprojectTask(req *ReprojectRequest) (*ProjectionResponse, error) {
	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}

	// 设置默认值
	if req.OutputFormat == "" {
		req.OutputFormat = "GTiff"
	}
	outputName := req.OutputName
	if outputName == "" {
		outputName = "reprojected"
	}

	ext := getFormatExtension(req.OutputFormat)
	outputPath := filepath.Join(outputDir, outputName+ext)

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "reproject",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeReprojectTask(taskID, req, outputPath)

	return &ProjectionResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

// executeReprojectTask 执行重投影任务
func (s *RasterService) executeReprojectTask(taskID string, req *ReprojectRequest, outputPath string) {
	var finalStatus int = 1
	defer func() {
		if r := recover(); r != nil {
			finalStatus = 2
		}
		models.DB.Model(&models.RasterRecord{}).Where("task_id = ?", taskID).Update("status", finalStatus)
	}()

	rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
	if err != nil {
		finalStatus = 2
		return
	}
	defer rd.Close()

	// 转换重采样方法
	resampleMethod := Gogeo.ResampleMethod(req.ResampleMethod)

	// 执行重投影
	if err := rd.ReprojectToEPSG(req.SrcEPSG, req.DstEPSG, outputPath, req.OutputFormat, resampleMethod); err != nil {
		finalStatus = 2
		return
	}
}

// ==================== 辅助函数 ====================

func createDirIfNotExist(dir string) error {
	return os.MkdirAll(dir, 0755)
}

// ==================== 栅格重采样相关 ====================

// ResampleRequest 重采样请求参数
type ResampleRequest struct {
	SourcePath     string  `json:"source_path" binding:"required"` // 源文件路径
	OutputName     string  `json:"output_name"`                    // 输出文件名
	OutputFormat   string  `json:"output_format"`                  // 输出格式: GTiff, JPEG, PNG
	ResampleMethod int     `json:"resample_method"`                // 重采样方法: 0-最近邻,1-双线性,2-三次卷积,3-三次样条,4-Lanczos
	TargetResX     float64 `json:"target_res_x"`                   // 目标X分辨率（与ScaleFactor/TargetSize三选一）
	TargetResY     float64 `json:"target_res_y"`                   // 目标Y分辨率
	ScaleFactor    float64 `json:"scale_factor"`                   // 缩放因子（>1放大，<1缩小）
	TargetWidth    int     `json:"target_width"`                   // 目标宽度
	TargetHeight   int     `json:"target_height"`                  // 目标高度
	NoDataValue    float64 `json:"nodata_value"`                   // NoData值
	HasNoData      bool    `json:"has_nodata"`                     // 是否设置NoData
}

// ResampleResponse 重采样响应
type ResampleResponse struct {
	TaskID     string `json:"task_id"`
	OutputPath string `json:"output_path"`
	Message    string `json:"message"`
}

// ResamplePreviewRequest 重采样预览请求
type ResamplePreviewRequest struct {
	SourcePath     string  `json:"source_path" binding:"required"`
	ResampleMethod int     `json:"resample_method"`
	TargetResX     float64 `json:"target_res_x"`
	TargetResY     float64 `json:"target_res_y"`
	ScaleFactor    float64 `json:"scale_factor"`
	TargetWidth    int     `json:"target_width"`
	TargetHeight   int     `json:"target_height"`
}

// ResamplePreviewResponse 重采样预览响应
type ResamplePreviewResponse struct {
	OriginalWidth  int     `json:"original_width"`
	OriginalHeight int     `json:"original_height"`
	OriginalResX   float64 `json:"original_res_x"`
	OriginalResY   float64 `json:"original_res_y"`
	TargetWidth    int     `json:"target_width"`
	TargetHeight   int     `json:"target_height"`
	TargetResX     float64 `json:"target_res_x"`
	TargetResY     float64 `json:"target_res_y"`
	BandCount      int     `json:"band_count"`
	EstimatedSize  int64   `json:"estimated_size"` // 预估大小(字节)
}

// StartResampleTask 启动重采样任务
func (s *RasterService) StartResampleTask(req *ResampleRequest) (*ResampleResponse, error) {
	if _, err := os.Stat(req.SourcePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("文件不存在: %s", req.SourcePath)
	}

	taskID := uuid.New().String()
	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}

	if req.OutputFormat == "" {
		req.OutputFormat = "GTiff"
	}
	if req.OutputName == "" {
		req.OutputName = "resampled"
	}

	ext := getFormatExtension(req.OutputFormat)
	outputPath := filepath.Join(outputDir, req.OutputName+ext)

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "resample",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeResampleTask(taskID, req, outputPath)

	return &ResampleResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "重采样任务已提交",
	}, nil
}

// executeResampleTask 执行重采样任务
func (s *RasterService) executeResampleTask(taskID string, req *ResampleRequest, outputPath string) {
	var finalStatus int = 1
	defer func() {
		if r := recover(); r != nil {
			finalStatus = 2
		}
		models.DB.Model(&models.RasterRecord{}).Where("task_id = ?", taskID).Update("status", finalStatus)
	}()

	rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
	if err != nil {
		finalStatus = 2
		return
	}
	defer rd.Close()

	options := &Gogeo.ResampleOptions{
		Method:       Gogeo.ResampleMethod(req.ResampleMethod),
		TargetResX:   req.TargetResX,
		TargetResY:   req.TargetResY,
		ScaleFactor:  req.ScaleFactor,
		TargetWidth:  req.TargetWidth,
		TargetHeight: req.TargetHeight,
		NoDataValue:  req.NoDataValue,
		HasNoData:    req.HasNoData,
	}

	if err := rd.ResampleToFile(outputPath, req.OutputFormat, options); err != nil {
		finalStatus = 2
		return
	}
}

// GetResamplePreview 获取重采样预览信息
func (s *RasterService) GetResamplePreview(req *ResamplePreviewRequest) (*ResamplePreviewResponse, error) {
	rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
	if err != nil {
		return nil, fmt.Errorf("打开文件失败: %w", err)
	}
	defer rd.Close()

	options := &Gogeo.ResampleOptions{
		Method:       Gogeo.ResampleMethod(req.ResampleMethod),
		TargetResX:   req.TargetResX,
		TargetResY:   req.TargetResY,
		ScaleFactor:  req.ScaleFactor,
		TargetWidth:  req.TargetWidth,
		TargetHeight: req.TargetHeight,
	}

	info, err := rd.GetResampleInfo(options)
	if err != nil {
		return nil, fmt.Errorf("获取重采样信息失败: %w", err)
	}

	estimatedSize, _ := rd.EstimateResampleSize(options)

	return &ResamplePreviewResponse{
		OriginalWidth:  info.OriginalWidth,
		OriginalHeight: info.OriginalHeight,
		OriginalResX:   info.OriginalResX,
		OriginalResY:   info.OriginalResY,
		TargetWidth:    info.TargetWidth,
		TargetHeight:   info.TargetHeight,
		TargetResX:     info.TargetResX,
		TargetResY:     info.TargetResY,
		BandCount:      info.BandCount,
		EstimatedSize:  estimatedSize,
	}, nil
}

// ==================== 金字塔构建相关 ====================

// BuildOverviewsRequest 构建金字塔请求
type BuildOverviewsRequest struct {
	SourcePath string `json:"source_path" binding:"required"` // 源文件路径
	Levels     []int  `json:"levels"`                         // 缩放因子，如 [2,4,8,16,32]，空则自动计算
	Resampling string `json:"resampling"`                     // 重采样方法: NEAREST,BILINEAR,CUBIC,AVERAGE,GAUSS,LANCZOS,MODE
}

// RemoveOverviewsRequest 删除金字塔请求
type RemoveOverviewsRequest struct {
	SourcePath string `json:"source_path" binding:"required"`
}

// OverviewInfoResponse 金字塔信息响应
type OverviewInfoResponse struct {
	HasOverviews  bool `json:"has_overviews"`
	OverviewCount int  `json:"overview_count"`
}

// StartBuildOverviewsTask 启动构建金字塔任务
func (s *RasterService) StartBuildOverviewsTask(req *BuildOverviewsRequest) (*ProjectionResponse, error) {
	if _, err := os.Stat(req.SourcePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("文件不存在: %s", req.SourcePath)
	}

	if req.Resampling == "" {
		req.Resampling = "NEAREST"
	}

	taskID := uuid.New().String()

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: req.SourcePath, // 金字塔直接写入源文件(.ovr)
		Status:     0,
		TypeName:   "build_overviews",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeBuildOverviewsTask(taskID, req)

	return &ProjectionResponse{
		TaskID:     taskID,
		OutputPath: req.SourcePath,
		Message:    "金字塔构建任务已提交",
	}, nil
}

// executeBuildOverviewsTask 执行构建金字塔任务
func (s *RasterService) executeBuildOverviewsTask(taskID string, req *BuildOverviewsRequest) {
	var finalStatus int = 1
	defer func() {
		if r := recover(); r != nil {
			finalStatus = 2
		}
		models.DB.Model(&models.RasterRecord{}).Where("task_id = ?", taskID).Update("status", finalStatus)
	}()

	rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
	if err != nil {
		finalStatus = 2
		return
	}
	defer rd.Close()

	resampling := Gogeo.ResampleOverview(req.Resampling)

	if len(req.Levels) > 0 {
		if err := rd.BuildOverviews(req.Levels, resampling); err != nil {
			finalStatus = 2
			return
		}
	} else {
		if err := rd.BuildOverviewsAuto(resampling); err != nil {
			finalStatus = 2
			return
		}
	}
}

// StartRemoveOverviewsTask 启动删除金字塔任务
func (s *RasterService) StartRemoveOverviewsTask(req *RemoveOverviewsRequest) (*ProjectionResponse, error) {
	if _, err := os.Stat(req.SourcePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("文件不存在: %s", req.SourcePath)
	}

	taskID := uuid.New().String()

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: req.SourcePath,
		Status:     0,
		TypeName:   "remove_overviews",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeRemoveOverviewsTask(taskID, req)

	return &ProjectionResponse{
		TaskID:     taskID,
		OutputPath: req.SourcePath,
		Message:    "金字塔删除任务已提交",
	}, nil
}

// executeRemoveOverviewsTask 执行删除金字塔任务
func (s *RasterService) executeRemoveOverviewsTask(taskID string, req *RemoveOverviewsRequest) {
	var finalStatus int = 1
	defer func() {
		if r := recover(); r != nil {
			finalStatus = 2
		}
		models.DB.Model(&models.RasterRecord{}).Where("task_id = ?", taskID).Update("status", finalStatus)
	}()

	rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
	if err != nil {
		finalStatus = 2
		return
	}
	defer rd.Close()

	if err := rd.RemoveOverviews(); err != nil {
		finalStatus = 2
		return
	}
}

// GetOverviewInfo 获取金字塔信息（同步，无需异步）
func (s *RasterService) GetOverviewInfo(sourcePath string) (*OverviewInfoResponse, error) {
	rd, err := Gogeo.OpenRasterDataset(sourcePath, false)
	if err != nil {
		return nil, fmt.Errorf("打开文件失败: %w", err)
	}
	defer rd.Close()

	return &OverviewInfoResponse{
		HasOverviews:  rd.HasOverviews(),
		OverviewCount: rd.GetOverviewCount(),
	}, nil
}
