package services

import (
	"encoding/json"
	"fmt"
	"github.com/GrainArc/Gogeo"
	"github.com/GrainArc/SouceMap/config"
	"github.com/GrainArc/SouceMap/models"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"path/filepath"
)

// ==================== 请求/响应结构体 ====================

// BandInfoResponse 波段信息响应
type BandInfoResponse struct {
	BandIndex   int     `json:"band_index"`
	DataType    string  `json:"data_type"`
	ColorInterp string  `json:"color_interp"`
	NoDataValue float64 `json:"nodata_value"`
	HasNoData   bool    `json:"has_nodata"`
	MinValue    float64 `json:"min_value"`
	MaxValue    float64 `json:"max_value"`
	HasStats    bool    `json:"has_stats"`
}

// AllBandsInfoResponse 所有波段信息响应
type AllBandsInfoResponse struct {
	BandCount int                `json:"band_count"`
	Bands     []BandInfoResponse `json:"bands"`
}

// BandHistogramResponse 波段直方图响应
type BandHistogramResponse struct {
	BandIndex int      `json:"band_index"`
	Buckets   int      `json:"buckets"`
	Min       float64  `json:"min"`
	Max       float64  `json:"max"`
	Histogram []uint64 `json:"histogram"`
}

// PaletteEntryResponse 调色板条目响应
type PaletteEntryResponse struct {
	Index int   `json:"index"`
	R     int16 `json:"r"`
	G     int16 `json:"g"`
	B     int16 `json:"b"`
	A     int16 `json:"a"`
}

// PaletteInfoResponse 调色板信息响应
type PaletteInfoResponse struct {
	BandIndex  int                    `json:"band_index"`
	EntryCount int                    `json:"entry_count"`
	InterpType string                 `json:"interp_type"`
	Entries    []PaletteEntryResponse `json:"entries"`
}

// ==================== 波段操作请求 ====================

// SetColorInterpRequest 设置颜色解释请求
type SetColorInterpRequest struct {
	SourcePath  string `json:"source_path" binding:"required"`
	BandIndex   int    `json:"band_index" binding:"required,min=1"`
	ColorInterp int    `json:"color_interp" binding:"min=0,max=13"`
	InPlace     bool   `json:"in_place"`
	OutputName  string `json:"output_name"`
}

// SetNoDataRequest 设置NoData请求
type SetNoDataRequest struct {
	SourcePath  string  `json:"source_path" binding:"required"`
	BandIndex   int     `json:"band_index" binding:"required,min=1"`
	NoDataValue float64 `json:"nodata_value"`
	InPlace     bool    `json:"in_place"`
	OutputName  string  `json:"output_name"`
}

// DeleteNoDataRequest 删除NoData请求
type DeleteNoDataRequest struct {
	SourcePath string `json:"source_path" binding:"required"`
	BandIndex  int    `json:"band_index" binding:"required,min=1"`
	InPlace    bool   `json:"in_place"`
	OutputName string `json:"output_name"`
}

// AddBandRequest 添加波段请求
type AddBandRequest struct {
	SourcePath  string  `json:"source_path" binding:"required"`
	DataType    int     `json:"data_type"`
	ColorInterp int     `json:"color_interp"`
	NoDataValue float64 `json:"nodata_value"`
	OutputName  string  `json:"output_name"`
}

// RemoveBandRequest 删除波段请求
type RemoveBandRequest struct {
	SourcePath string `json:"source_path" binding:"required"`
	BandIndex  int    `json:"band_index" binding:"required,min=1"`
	OutputName string `json:"output_name"`
}

// ReorderBandsRequest 重排波段请求
type ReorderBandsRequest struct {
	SourcePath string `json:"source_path" binding:"required"`
	BandOrder  []int  `json:"band_order" binding:"required,min=1"`
	OutputName string `json:"output_name"`
}

// ConvertBandTypeRequest 转换波段数据类型请求
type ConvertBandTypeRequest struct {
	SourcePath string `json:"source_path" binding:"required"`
	BandIndex  int    `json:"band_index" binding:"required,min=1"`
	NewType    int    `json:"new_type" binding:"required"`
	OutputName string `json:"output_name"`
}

// BandMathRequest 波段运算请求
type BandMathRequest struct {
	SourcePath string  `json:"source_path" binding:"required"`
	Band1      int     `json:"band1" binding:"required,min=1"`
	Band2      int     `json:"band2"`     // 为0时使用标量运算
	Scalar     float64 `json:"scalar"`    // 标量值
	Operation  int     `json:"operation"` // 0-加,1-减,2-乘,3-除,4-最小,5-最大,6-幂
	OutputName string  `json:"output_name"`
}

// CalculateIndexRequest 计算指数请求
type CalculateIndexRequest struct {
	SourcePath string `json:"source_path" binding:"required"`
	IndexType  string `json:"index_type" binding:"required"` // NDVI, NDWI, EVI
	NIRBand    int    `json:"nir_band"`
	RedBand    int    `json:"red_band"`
	GreenBand  int    `json:"green_band"`
	BlueBand   int    `json:"blue_band"`
	OutputName string `json:"output_name"`
}

// NormalizeBandRequest 归一化波段请求
type NormalizeBandRequest struct {
	SourcePath string  `json:"source_path" binding:"required"`
	BandIndex  int     `json:"band_index" binding:"required,min=1"`
	NewMin     float64 `json:"new_min"`
	NewMax     float64 `json:"new_max"`
	OutputName string  `json:"output_name"`
}

// ApplyFilterRequest 应用滤波请求
type ApplyFilterRequest struct {
	SourcePath string `json:"source_path" binding:"required"`
	BandIndex  int    `json:"band_index" binding:"required,min=1"`
	FilterType int    `json:"filter_type"` // 0-均值,1-中值,2-高斯,3-Sobel,4-拉普拉斯,5-最小,6-最大
	KernelSize int    `json:"kernel_size"` // 必须为奇数
	OutputName string `json:"output_name"`
}

// ReclassifyRequest 重分类请求
type ReclassifyRequest struct {
	SourcePath   string           `json:"source_path" binding:"required"`
	BandIndex    int              `json:"band_index" binding:"required,min=1"`
	Rules        []ReclassifyRule `json:"rules" binding:"required,min=1"`
	DefaultValue float64          `json:"default_value"`
	OutputName   string           `json:"output_name"`
}

// ReclassifyRule 重分类规则
type ReclassifyRule struct {
	MinValue float64 `json:"min_value"`
	MaxValue float64 `json:"max_value"`
	NewValue float64 `json:"new_value"`
}

// SetPaletteRequest 设置调色板请求
type SetPaletteRequest struct {
	SourcePath   string                 `json:"source_path" binding:"required"`
	BandIndex    int                    `json:"band_index" binding:"required,min=1"`
	PaletteType  string                 `json:"palette_type"`  // grayscale, rainbow, heatmap, custom
	CustomColors []PaletteEntryResponse `json:"custom_colors"` // 自定义颜色
	InPlace      bool                   `json:"in_place"`
	OutputName   string                 `json:"output_name"`
}

// PaletteToRGBRequest 调色板转RGB请求
type PaletteToRGBRequest struct {
	SourcePath string `json:"source_path" binding:"required"`
	OutputName string `json:"output_name"`
}

// RGBToPaletteRequest RGB转调色板请求
type RGBToPaletteRequest struct {
	SourcePath string `json:"source_path" binding:"required"`
	ColorCount int    `json:"color_count" binding:"required,min=1,max=256"`
	OutputName string `json:"output_name"`
}

// MergeBandsRequest 合并波段请求
type MergeBandsRequest struct {
	SourcePath  string `json:"source_path" binding:"required"`
	BandIndices []int  `json:"band_indices" binding:"required,min=1"`
	OutputName  string `json:"output_name"`
}

// SetBandMetadataRequest 设置波段元数据请求
type SetBandMetadataRequest struct {
	SourcePath string `json:"source_path" binding:"required"`
	BandIndex  int    `json:"band_index" binding:"required,min=1"`
	Key        string `json:"key" binding:"required"`
	Value      string `json:"value" binding:"required"`
	InPlace    bool   `json:"in_place"`
	OutputName string `json:"output_name"`
}

// SetBandDescriptionRequest 设置波段描述请求
type SetBandDescriptionRequest struct {
	SourcePath  string `json:"source_path" binding:"required"`
	BandIndex   int    `json:"band_index" binding:"required,min=1"`
	Description string `json:"description" binding:"required"`
	InPlace     bool   `json:"in_place"`
	OutputName  string `json:"output_name"`
}

// GetBandHistogramRequest 获取直方图请求
type GetBandHistogramRequest struct {
	SourcePath string  `json:"source_path" binding:"required"`
	BandIndex  int     `json:"band_index" binding:"required,min=1"`
	Buckets    int     `json:"buckets"`
	Min        float64 `json:"min"`
	Max        float64 `json:"max"`
}

// BandOperationResponse 波段操作响应
type BandOperationResponse struct {
	TaskID     string `json:"task_id"`
	OutputPath string `json:"output_path"`
	Message    string `json:"message"`
}

// ==================== 服务方法实现 ====================

// GetBandInfo 获取单个波段信息
func (s *RasterService) GetBandInfo(sourcePath string, bandIndex int) (*BandInfoResponse, error) {
	rd, err := Gogeo.OpenRasterDataset(sourcePath, false)
	if err != nil {
		return nil, fmt.Errorf("打开栅格文件失败: %w", err)
	}
	defer rd.Close()

	info, err := rd.GetBandInfo(bandIndex)
	if err != nil {
		return nil, fmt.Errorf("获取波段信息失败: %w", err)
	}

	return &BandInfoResponse{
		BandIndex:   info.BandIndex,
		DataType:    Gogeo.BandDataType(info.DataType).String(),
		ColorInterp: Gogeo.ColorInterpretation(info.ColorInterp).String(),
		NoDataValue: info.NoDataValue,
		HasNoData:   info.HasNoData,
		MinValue:    info.MinValue,
		MaxValue:    info.MaxValue,
		HasStats:    info.HasStats,
	}, nil
}

// GetAllBandsInfo 获取所有波段信息
func (s *RasterService) GetAllBandsInfo(sourcePath string) (*AllBandsInfoResponse, error) {
	rd, err := Gogeo.OpenRasterDataset(sourcePath, false)
	if err != nil {
		return nil, fmt.Errorf("打开栅格文件失败: %w", err)
	}
	defer rd.Close()

	infos, err := rd.GetAllBandsInfo()
	if err != nil {
		return nil, fmt.Errorf("获取波段信息失败: %w", err)
	}

	bands := make([]BandInfoResponse, len(infos))
	for i, info := range infos {
		bands[i] = BandInfoResponse{
			BandIndex:   info.BandIndex,
			DataType:    Gogeo.BandDataType(info.DataType).String(),
			ColorInterp: Gogeo.ColorInterpretation(info.ColorInterp).String(),
			NoDataValue: info.NoDataValue,
			HasNoData:   info.HasNoData,
			MinValue:    info.MinValue,
			MaxValue:    info.MaxValue,
			HasStats:    info.HasStats,
		}
	}

	return &AllBandsInfoResponse{
		BandCount: rd.GetBandCount(),
		Bands:     bands,
	}, nil
}

// GetBandHistogram 获取波段直方图
func (s *RasterService) GetBandHistogram(req *GetBandHistogramRequest) (*BandHistogramResponse, error) {
	rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
	if err != nil {
		return nil, fmt.Errorf("打开栅格文件失败: %w", err)
	}
	defer rd.Close()

	buckets := req.Buckets
	if buckets <= 0 {
		buckets = 256
	}

	histogram, err := rd.GetBandHistogram(req.BandIndex, buckets, req.Min, req.Max)
	if err != nil {
		return nil, fmt.Errorf("获取直方图失败: %w", err)
	}

	return &BandHistogramResponse{
		BandIndex: req.BandIndex,
		Buckets:   buckets,
		Min:       req.Min,
		Max:       req.Max,
		Histogram: histogram,
	}, nil
}

// GetPaletteInfo 获取调色板信息
func (s *RasterService) GetPaletteInfo(sourcePath string, bandIndex int) (*PaletteInfoResponse, error) {
	rd, err := Gogeo.OpenRasterDataset(sourcePath, false)
	if err != nil {
		return nil, fmt.Errorf("打开栅格文件失败: %w", err)
	}
	defer rd.Close()

	info, err := rd.GetPaletteInfo(bandIndex)
	if err != nil {
		return nil, fmt.Errorf("获取调色板信息失败: %w", err)
	}

	entries := make([]PaletteEntryResponse, len(info.Entries))
	for i, entry := range info.Entries {
		entries[i] = PaletteEntryResponse{
			Index: i,
			R:     entry.C1,
			G:     entry.C2,
			B:     entry.C3,
			A:     entry.C4,
		}
	}

	interpType := "Unknown"
	switch info.InterpType {
	case Gogeo.PaletteGray:
		interpType = "Gray"
	case Gogeo.PaletteRGB:
		interpType = "RGB"
	case Gogeo.PaletteCMYK:
		interpType = "CMYK"
	case Gogeo.PaletteHLS:
		interpType = "HLS"
	}

	return &PaletteInfoResponse{
		BandIndex:  bandIndex,
		EntryCount: info.EntryCount,
		InterpType: interpType,
		Entries:    entries,
	}, nil
}

// StartSetColorInterpTask 启动设置颜色解释任务
func (s *RasterService) StartSetColorInterpTask(req *SetColorInterpRequest) (*BandOperationResponse, error) {
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
			outputName = "color_interp_modified"
		}
		outputPath = filepath.Join(outputDir, outputName+".tif")
	}

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "set_color_interp",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeSetColorInterpTask(taskID, req, outputPath)

	return &BandOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeSetColorInterpTask(taskID string, req *SetColorInterpRequest, outputPath string) {
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

	if err := rd.SetBandColorInterpretation(req.BandIndex, Gogeo.ColorInterpretation(req.ColorInterp)); err != nil {
		finalStatus = 2
		return
	}

	if !req.InPlace {
		if err := rd.ExportToFile(outputPath, "GTiff", nil); err != nil {
			finalStatus = 2
			return
		}
	}
}

// StartSetNoDataTask 启动设置NoData任务
func (s *RasterService) StartSetNoDataTask(req *SetNoDataRequest) (*BandOperationResponse, error) {
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
			outputName = "nodata_modified"
		}
		outputPath = filepath.Join(outputDir, outputName+".tif")
	}

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "set_nodata",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeSetNoDataTask(taskID, req, outputPath)

	return &BandOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeSetNoDataTask(taskID string, req *SetNoDataRequest, outputPath string) {
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

	if err := rd.SetBandNoDataValue(req.BandIndex, req.NoDataValue); err != nil {
		finalStatus = 2
		return
	}

	if !req.InPlace {
		if err := rd.ExportToFile(outputPath, "GTiff", nil); err != nil {
			finalStatus = 2
			return
		}
	}
}

// StartAddBandTask 启动添加波段任务
func (s *RasterService) StartAddBandTask(req *AddBandRequest) (*BandOperationResponse, error) {
	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}
	outputName := req.OutputName
	if outputName == "" {
		outputName = "band_added"
	}
	outputPath := filepath.Join(outputDir, outputName+".tif")

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "add_band",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeAddBandTask(taskID, req, outputPath)

	return &BandOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeAddBandTask(taskID string, req *AddBandRequest, outputPath string) {
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

	if err := rd.AddBand(Gogeo.BandDataType(req.DataType), Gogeo.ColorInterpretation(req.ColorInterp), req.NoDataValue); err != nil {
		finalStatus = 2
		return
	}

	if err := rd.ExportToFile(outputPath, "GTiff", nil); err != nil {
		finalStatus = 2
		return
	}
}

// StartRemoveBandTask 启动删除波段任务
func (s *RasterService) StartRemoveBandTask(req *RemoveBandRequest) (*BandOperationResponse, error) {
	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}
	outputName := req.OutputName
	if outputName == "" {
		outputName = "band_removed"
	}
	outputPath := filepath.Join(outputDir, outputName+".tif")

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "remove_band",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeRemoveBandTask(taskID, req, outputPath)

	return &BandOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeRemoveBandTask(taskID string, req *RemoveBandRequest, outputPath string) {
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

	if err := rd.RemoveBand(req.BandIndex); err != nil {
		finalStatus = 2
		return
	}

	if err := rd.ExportToFile(outputPath, "GTiff", nil); err != nil {
		finalStatus = 2
		return
	}
}

// StartReorderBandsTask 启动重排波段任务
func (s *RasterService) StartReorderBandsTask(req *ReorderBandsRequest) (*BandOperationResponse, error) {
	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}
	outputName := req.OutputName
	if outputName == "" {
		outputName = "bands_reordered"
	}
	outputPath := filepath.Join(outputDir, outputName+".tif")

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "reorder_bands",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeReorderBandsTask(taskID, req, outputPath)

	return &BandOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeReorderBandsTask(taskID string, req *ReorderBandsRequest, outputPath string) {
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

	if err := rd.ReorderBands(req.BandOrder); err != nil {
		finalStatus = 2
		return
	}

	if err := rd.ExportToFile(outputPath, "GTiff", nil); err != nil {
		finalStatus = 2
		return
	}
}

// StartBandMathTask 启动波段运算任务
func (s *RasterService) StartBandMathTask(req *BandMathRequest) (*BandOperationResponse, error) {
	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}
	outputName := req.OutputName
	if outputName == "" {
		outputName = "band_math_result"
	}
	outputPath := filepath.Join(outputDir, outputName+".tif")

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "band_math",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeBandMathTask(taskID, req, outputPath)

	return &BandOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeBandMathTask(taskID string, req *BandMathRequest, outputPath string) {
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

	var result []float64
	op := Gogeo.BandMathOp(req.Operation)

	if req.Band2 > 0 {
		// 双波段运算
		result, err = rd.BandMath(req.Band1, req.Band2, op)
	} else {
		// 标量运算
		result, err = rd.BandMathScalar(req.Band1, req.Scalar, op)
	}

	if err != nil {
		finalStatus = 2
		return
	}

	// 创建新数据集并写入结果
	newRD, err := rd.MergeBandsToNewDataset([]int{1})
	if err != nil {
		finalStatus = 2
		return
	}
	defer newRD.Close()

	if err := newRD.WriteBandData(1, result); err != nil {
		finalStatus = 2
		return
	}

	if err := newRD.ExportToFile(outputPath, "GTiff", nil); err != nil {
		finalStatus = 2
		return
	}
}

// StartCalculateIndexTask 启动计算指数任务
func (s *RasterService) StartCalculateIndexTask(req *CalculateIndexRequest) (*BandOperationResponse, error) {
	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}
	outputName := req.OutputName
	if outputName == "" {
		outputName = req.IndexType + "_result"
	}
	outputPath := filepath.Join(outputDir, outputName+".tif")

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "calculate_index",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeCalculateIndexTask(taskID, req, outputPath)

	return &BandOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeCalculateIndexTask(taskID string, req *CalculateIndexRequest, outputPath string) {
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

	var result []float64

	switch req.IndexType {
	case "NDVI":
		result, err = rd.CalculateNDVI(req.NIRBand, req.RedBand)
	case "NDWI":
		result, err = rd.CalculateNDWI(req.GreenBand, req.NIRBand)
	case "EVI":
		result, err = rd.CalculateEVI(req.NIRBand, req.RedBand, req.BlueBand)
	default:
		finalStatus = 2
		return
	}

	if err != nil {
		finalStatus = 2
		return
	}

	// 创建单波段数据集保存结果
	newRD, err := rd.MergeBandsToNewDataset([]int{1})
	if err != nil {
		finalStatus = 2
		return
	}
	defer newRD.Close()

	if err := newRD.WriteBandData(1, result); err != nil {
		finalStatus = 2
		return
	}

	if err := newRD.ExportToFile(outputPath, "GTiff", nil); err != nil {
		finalStatus = 2
		return
	}
}

// StartNormalizeBandTask 启动归一化波段任务
func (s *RasterService) StartNormalizeBandTask(req *NormalizeBandRequest) (*BandOperationResponse, error) {
	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}
	outputName := req.OutputName
	if outputName == "" {
		outputName = "normalized"
	}
	outputPath := filepath.Join(outputDir, outputName+".tif")

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "normalize_band",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeNormalizeBandTask(taskID, req, outputPath)

	return &BandOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeNormalizeBandTask(taskID string, req *NormalizeBandRequest, outputPath string) {
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

	result, err := rd.NormalizeBand(req.BandIndex, req.NewMin, req.NewMax)
	if err != nil {
		finalStatus = 2
		return
	}

	newRD, err := rd.MergeBandsToNewDataset([]int{req.BandIndex})
	if err != nil {
		finalStatus = 2
		return
	}
	defer newRD.Close()

	if err := newRD.WriteBandData(1, result); err != nil {
		finalStatus = 2
		return
	}

	if err := newRD.ExportToFile(outputPath, "GTiff", nil); err != nil {
		finalStatus = 2
		return
	}
}

// StartApplyFilterTask 启动应用滤波任务
func (s *RasterService) StartApplyFilterTask(req *ApplyFilterRequest) (*BandOperationResponse, error) {
	// 验证核大小
	if req.KernelSize%2 == 0 {
		return nil, fmt.Errorf("核大小必须为奇数")
	}
	if req.KernelSize <= 0 {
		req.KernelSize = 3
	}

	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}
	outputName := req.OutputName
	if outputName == "" {
		outputName = "filtered"
	}
	outputPath := filepath.Join(outputDir, outputName+".tif")

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "apply_filter",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeApplyFilterTask(taskID, req, outputPath)

	return &BandOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeApplyFilterTask(taskID string, req *ApplyFilterRequest, outputPath string) {
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

	result, err := rd.ApplyFilter(req.BandIndex, Gogeo.FilterType(req.FilterType), req.KernelSize)
	if err != nil {
		finalStatus = 2
		return
	}

	newRD, err := rd.MergeBandsToNewDataset([]int{req.BandIndex})
	if err != nil {
		finalStatus = 2
		return
	}
	defer newRD.Close()

	if err := newRD.WriteBandData(1, result); err != nil {
		finalStatus = 2
		return
	}

	if err := newRD.ExportToFile(outputPath, "GTiff", nil); err != nil {
		finalStatus = 2
		return
	}
}

// StartReclassifyTask 启动重分类任务
func (s *RasterService) StartReclassifyTask(req *ReclassifyRequest) (*BandOperationResponse, error) {
	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}
	outputName := req.OutputName
	if outputName == "" {
		outputName = "reclassified"
	}
	outputPath := filepath.Join(outputDir, outputName+".tif")

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "reclassify",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeReclassifyTask(taskID, req, outputPath)

	return &BandOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeReclassifyTask(taskID string, req *ReclassifyRequest, outputPath string) {
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

	// 转换规则
	rules := make([]Gogeo.ReclassifyRule, len(req.Rules))
	for i, r := range req.Rules {
		rules[i] = Gogeo.ReclassifyRule{
			MinValue: r.MinValue,
			MaxValue: r.MaxValue,
			NewValue: r.NewValue,
		}
	}

	result, err := rd.ReclassifyBand(req.BandIndex, rules, req.DefaultValue)
	if err != nil {
		finalStatus = 2
		return
	}

	newRD, err := rd.MergeBandsToNewDataset([]int{req.BandIndex})
	if err != nil {
		finalStatus = 2
		return
	}
	defer newRD.Close()

	if err := newRD.WriteBandData(1, result); err != nil {
		finalStatus = 2
		return
	}

	if err := newRD.ExportToFile(outputPath, "GTiff", nil); err != nil {
		finalStatus = 2
		return
	}
}

// StartSetPaletteTask 启动设置调色板任务
func (s *RasterService) StartSetPaletteTask(req *SetPaletteRequest) (*BandOperationResponse, error) {
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
			outputName = "palette_set"
		}
		outputPath = filepath.Join(outputDir, outputName+".tif")
	}

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "set_palette",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeSetPaletteTask(taskID, req, outputPath)

	return &BandOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeSetPaletteTask(taskID string, req *SetPaletteRequest, outputPath string) {
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

	var ct *Gogeo.ColorTable

	switch req.PaletteType {
	case "grayscale":
		ct = Gogeo.CreateGrayscalePalette()
	case "rainbow":
		ct = Gogeo.CreateRainbowPalette()
	case "heatmap":
		ct = Gogeo.CreateHeatmapPalette()
	case "custom":
		if len(req.CustomColors) == 0 {
			finalStatus = 2
			return
		}
		entries := make([]Gogeo.PaletteEntry, len(req.CustomColors))
		for i, c := range req.CustomColors {
			entries[i] = Gogeo.PaletteEntry{
				C1: c.R,
				C2: c.G,
				C3: c.B,
				C4: c.A,
			}
		}
		ct = Gogeo.CreateCustomPalette(entries)
	default:
		ct = Gogeo.CreateGrayscalePalette()
	}

	if ct == nil {
		finalStatus = 2
		return
	}
	defer ct.Destroy()

	if err := rd.SetBandColorTable(req.BandIndex, ct); err != nil {
		finalStatus = 2
		return
	}

	if !req.InPlace {
		if err := rd.ExportToFile(outputPath, "GTiff", nil); err != nil {
			finalStatus = 2
			return
		}
	}
}

// StartPaletteToRGBTask 启动调色板转RGB任务
func (s *RasterService) StartPaletteToRGBTask(req *PaletteToRGBRequest) (*BandOperationResponse, error) {
	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}
	outputName := req.OutputName
	if outputName == "" {
		outputName = "palette_to_rgb"
	}
	outputPath := filepath.Join(outputDir, outputName+".tif")

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "palette_to_rgb",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executePaletteToRGBTask(taskID, req, outputPath)

	return &BandOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executePaletteToRGBTask(taskID string, req *PaletteToRGBRequest, outputPath string) {
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

	newRD, err := rd.PaletteToRGB()
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

// StartRGBToPaletteTask 启动RGB转调色板任务
func (s *RasterService) StartRGBToPaletteTask(req *RGBToPaletteRequest) (*BandOperationResponse, error) {
	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}
	outputName := req.OutputName
	if outputName == "" {
		outputName = "rgb_to_palette"
	}
	outputPath := filepath.Join(outputDir, outputName+".tif")

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "rgb_to_palette",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeRGBToPaletteTask(taskID, req, outputPath)

	return &BandOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeRGBToPaletteTask(taskID string, req *RGBToPaletteRequest, outputPath string) {
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

	newRD, err := rd.RGBToPalette(req.ColorCount)
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

// StartMergeBandsTask 启动合并波段任务
func (s *RasterService) StartMergeBandsTask(req *MergeBandsRequest) (*BandOperationResponse, error) {
	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}
	outputName := req.OutputName
	if outputName == "" {
		outputName = "merged_bands"
	}
	outputPath := filepath.Join(outputDir, outputName+".tif")

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "merge_bands",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeMergeBandsTask(taskID, req, outputPath)

	return &BandOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeMergeBandsTask(taskID string, req *MergeBandsRequest, outputPath string) {
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

	newRD, err := rd.MergeBandsToNewDataset(req.BandIndices)
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

// StartSetBandMetadataTask 启动设置波段元数据任务
func (s *RasterService) StartSetBandMetadataTask(req *SetBandMetadataRequest) (*BandOperationResponse, error) {
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
			outputName = "metadata_set"
		}
		outputPath = filepath.Join(outputDir, outputName+".tif")
	}

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "set_band_metadata",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeSetBandMetadataTask(taskID, req, outputPath)

	return &BandOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeSetBandMetadataTask(taskID string, req *SetBandMetadataRequest, outputPath string) {
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

	if err := rd.SetBandMetadata(req.BandIndex, req.Key, req.Value); err != nil {
		finalStatus = 2
		return
	}

	if !req.InPlace {
		if err := rd.ExportToFile(outputPath, "GTiff", nil); err != nil {
			finalStatus = 2
			return
		}
	}
}

// GetBandMetadata 获取波段元数据
func (s *RasterService) GetBandMetadata(sourcePath string, bandIndex int, key string) (string, error) {
	rd, err := Gogeo.OpenRasterDataset(sourcePath, false)
	if err != nil {
		return "", fmt.Errorf("打开栅格文件失败: %w", err)
	}
	defer rd.Close()

	return rd.GetBandMetadata(bandIndex, key)
}

// GetAllBandMetadata 获取波段所有元数据
func (s *RasterService) GetAllBandMetadata(sourcePath string, bandIndex int) (map[string]string, error) {
	rd, err := Gogeo.OpenRasterDataset(sourcePath, false)
	if err != nil {
		return nil, fmt.Errorf("打开栅格文件失败: %w", err)
	}
	defer rd.Close()

	return rd.GetAllBandMetadata(bandIndex)
}

// StartSetBandDescriptionTask 启动设置波段描述任务
func (s *RasterService) StartSetBandDescriptionTask(req *SetBandDescriptionRequest) (*BandOperationResponse, error) {
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
			outputName = "description_set"
		}
		outputPath = filepath.Join(outputDir, outputName+".tif")
	}

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "set_band_description",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeSetBandDescriptionTask(taskID, req, outputPath)

	return &BandOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeSetBandDescriptionTask(taskID string, req *SetBandDescriptionRequest, outputPath string) {
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

	if err := rd.SetBandDescription(req.BandIndex, req.Description); err != nil {
		finalStatus = 2
		return
	}

	if !req.InPlace {
		if err := rd.ExportToFile(outputPath, "GTiff", nil); err != nil {
			finalStatus = 2
			return
		}
	}
}

// GetBandDescription 获取波段描述
func (s *RasterService) GetBandDescription(sourcePath string, bandIndex int) (string, error) {
	rd, err := Gogeo.OpenRasterDataset(sourcePath, false)
	if err != nil {
		return "", fmt.Errorf("打开栅格文件失败: %w", err)
	}
	defer rd.Close()

	return rd.GetBandDescription(bandIndex)
}

// ConvertBandDataType 转换波段数据类型
func (s *RasterService) StartConvertBandTypeTask(req *ConvertBandTypeRequest) (*BandOperationResponse, error) {
	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}
	outputName := req.OutputName
	if outputName == "" {
		outputName = "type_converted"
	}
	outputPath := filepath.Join(outputDir, outputName+".tif")

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "convert_band_type",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeConvertBandTypeTask(taskID, req, outputPath)

	return &BandOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeConvertBandTypeTask(taskID string, req *ConvertBandTypeRequest, outputPath string) {
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

	if err := rd.ConvertBandDataType(req.BandIndex, Gogeo.BandDataType(req.NewType)); err != nil {
		finalStatus = 2
		return
	}

	if err := rd.ExportToFile(outputPath, "GTiff", nil); err != nil {
		finalStatus = 2
		return
	}
}

// GetBandScaleOffset 获取波段缩放和偏移
func (s *RasterService) GetBandScaleOffset(sourcePath string, bandIndex int) (scale, offset float64, err error) {
	rd, err := Gogeo.OpenRasterDataset(sourcePath, false)
	if err != nil {
		return 0, 0, fmt.Errorf("打开栅格文件失败: %w", err)
	}
	defer rd.Close()

	scale, err = rd.GetBandScale(bandIndex)
	if err != nil {
		return 0, 0, err
	}

	offset, err = rd.GetBandOffset(bandIndex)
	if err != nil {
		return 0, 0, err
	}

	return scale, offset, nil
}

// GetBandUnitType 获取波段单位类型
func (s *RasterService) GetBandUnitType(sourcePath string, bandIndex int) (string, error) {
	rd, err := Gogeo.OpenRasterDataset(sourcePath, false)
	if err != nil {
		return "", fmt.Errorf("打开栅格文件失败: %w", err)
	}
	defer rd.Close()

	return rd.GetBandUnitType(bandIndex)
}
