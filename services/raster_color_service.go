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

// ==================== 调色请求结构 ====================

// ColorAdjustRequest 综合调色请求
type ColorAdjustRequest struct {
	SourcePath   string  `json:"source_path" binding:"required"`
	Brightness   float64 `json:"brightness"` // [-1.0, 1.0]
	Contrast     float64 `json:"contrast"`   // [-1.0, 1.0]
	Saturation   float64 `json:"saturation"` // [-1.0, 1.0]
	Gamma        float64 `json:"gamma"`      // [0.1, 10.0]
	Hue          float64 `json:"hue"`        // [-180, 180]
	OutputName   string  `json:"output_name"`
	OutputFormat string  `json:"output_format"`
}

// SingleColorAdjustRequest 单项调色请求
type SingleColorAdjustRequest struct {
	SourcePath   string  `json:"source_path" binding:"required"`
	Value        float64 `json:"value" binding:"required"`
	OutputName   string  `json:"output_name"`
	OutputFormat string  `json:"output_format"`
}

// LevelsAdjustRequest 色阶调整请求
type LevelsAdjustRequest struct {
	SourcePath   string  `json:"source_path" binding:"required"`
	InputMin     float64 `json:"input_min"`
	InputMax     float64 `json:"input_max"`
	OutputMin    float64 `json:"output_min"`
	OutputMax    float64 `json:"output_max"`
	Midtone      float64 `json:"midtone"`    // [0.1, 9.9], 1.0为不变
	BandIndex    int     `json:"band_index"` // 0=全部波段
	OutputName   string  `json:"output_name"`
	OutputFormat string  `json:"output_format"`
}

// CurvePointDTO 曲线控制点
type CurvePointDTO struct {
	Input  float64 `json:"input"`  // [0, 255]
	Output float64 `json:"output"` // [0, 255]
}

// CurvesAdjustRequest 曲线调整请求
type CurvesAdjustRequest struct {
	SourcePath   string          `json:"source_path" binding:"required"`
	Points       []CurvePointDTO `json:"points" binding:"required,min=2"`
	Channel      int             `json:"channel"` // 0=全部, 1=R, 2=G, 3=B
	OutputName   string          `json:"output_name"`
	OutputFormat string          `json:"output_format"`
}

// AutoAdjustRequest 自动调整请求
type AutoAdjustRequest struct {
	SourcePath   string  `json:"source_path" binding:"required"`
	ClipPercent  float64 `json:"clip_percent"` // 用于自动色阶
	OutputName   string  `json:"output_name"`
	OutputFormat string  `json:"output_format"`
}

// CLAHERequest CLAHE均衡化请求
type CLAHERequest struct {
	SourcePath   string  `json:"source_path" binding:"required"`
	TileSize     int     `json:"tile_size"`  // 默认8
	ClipLimit    float64 `json:"clip_limit"` // 默认2.0
	OutputName   string  `json:"output_name"`
	OutputFormat string  `json:"output_format"`
}

// HistogramEqualizeRequest 直方图均衡化请求
type HistogramEqualizeRequest struct {
	SourcePath   string `json:"source_path" binding:"required"`
	BandIndex    int    `json:"band_index"` // 0=全部波段
	OutputName   string `json:"output_name"`
	OutputFormat string `json:"output_format"`
}

// PresetColorRequest 预设调色请求
type PresetColorRequest struct {
	SourcePath   string `json:"source_path" binding:"required"`
	PresetName   string `json:"preset_name" binding:"required"` // vivid, soft, high_contrast, warm, cool, black_white, sepia
	OutputName   string `json:"output_name"`
	OutputFormat string `json:"output_format"`
}

// SCurveRequest S曲线对比度请求
type SCurveRequest struct {
	SourcePath   string  `json:"source_path" binding:"required"`
	Strength     float64 `json:"strength"` // 强度
	OutputName   string  `json:"output_name"`
	OutputFormat string  `json:"output_format"`
}

// ==================== 匀色请求结构 ====================

// ReferenceRegionDTO 参考区域
type ReferenceRegionDTO struct {
	X      int `json:"x"`
	Y      int `json:"y"`
	Width  int `json:"width"`
	Height int `json:"height"`
}

// ColorStatisticsResponse 颜色统计响应
type ColorStatisticsResponse struct {
	MeanR float64 `json:"mean_r"`
	MeanG float64 `json:"mean_g"`
	MeanB float64 `json:"mean_b"`
	StdR  float64 `json:"std_r"`
	StdG  float64 `json:"std_g"`
	StdB  float64 `json:"std_b"`
	MinR  float64 `json:"min_r"`
	MinG  float64 `json:"min_g"`
	MinB  float64 `json:"min_b"`
	MaxR  float64 `json:"max_r"`
	MaxG  float64 `json:"max_g"`
	MaxB  float64 `json:"max_b"`
}

// BandStatisticsResponse 波段统计响应
type BandStatisticsResponse struct {
	Min       float64 `json:"min"`
	Max       float64 `json:"max"`
	Mean      float64 `json:"mean"`
	Stddev    float64 `json:"stddev"`
	Histogram []int   `json:"histogram"`
}

// GetColorStatisticsRequest 获取颜色统计请求
type GetColorStatisticsRequest struct {
	SourcePath string              `json:"source_path" binding:"required"`
	Region     *ReferenceRegionDTO `json:"region"` // 可选区域
}

// GetBandStatisticsRequest 获取波段统计请求
type GetBandStatisticsRequest struct {
	SourcePath string              `json:"source_path" binding:"required"`
	BandIndex  int                 `json:"band_index" binding:"required"`
	Region     *ReferenceRegionDTO `json:"region"`
}

// HistogramMatchRequest 直方图匹配请求
type HistogramMatchRequest struct {
	SourcePath    string              `json:"source_path" binding:"required"`
	ReferencePath string              `json:"reference_path" binding:"required"`
	SrcRegion     *ReferenceRegionDTO `json:"src_region"`
	RefRegion     *ReferenceRegionDTO `json:"ref_region"`
	OutputName    string              `json:"output_name"`
	OutputFormat  string              `json:"output_format"`
}

// MeanStdMatchRequest 均值-标准差匹配请求
type MeanStdMatchRequest struct {
	SourcePath   string              `json:"source_path" binding:"required"`
	TargetMeanR  float64             `json:"target_mean_r"`
	TargetMeanG  float64             `json:"target_mean_g"`
	TargetMeanB  float64             `json:"target_mean_b"`
	TargetStdR   float64             `json:"target_std_r"`
	TargetStdG   float64             `json:"target_std_g"`
	TargetStdB   float64             `json:"target_std_b"`
	Region       *ReferenceRegionDTO `json:"region"`
	Strength     float64             `json:"strength"` // [0, 1]
	OutputName   string              `json:"output_name"`
	OutputFormat string              `json:"output_format"`
}

// WallisFilterRequest Wallis滤波请求
type WallisFilterRequest struct {
	SourcePath   string  `json:"source_path" binding:"required"`
	TargetMean   float64 `json:"target_mean"`
	TargetStd    float64 `json:"target_std"`
	C            float64 `json:"c"`           // 对比度参数 [0, 1]
	B            float64 `json:"b"`           // 亮度参数 [0, 1]
	WindowSize   int     `json:"window_size"` // 默认31
	OutputName   string  `json:"output_name"`
	OutputFormat string  `json:"output_format"`
}

// MomentMatchRequest 矩匹配请求
type MomentMatchRequest struct {
	SourcePath    string              `json:"source_path" binding:"required"`
	ReferencePath string              `json:"reference_path" binding:"required"`
	SrcRegion     *ReferenceRegionDTO `json:"src_region"`
	RefRegion     *ReferenceRegionDTO `json:"ref_region"`
	OutputName    string              `json:"output_name"`
	OutputFormat  string              `json:"output_format"`
}

// LinearRegressionBalanceRequest 线性回归匀色请求
type LinearRegressionBalanceRequest struct {
	SourcePath    string             `json:"source_path" binding:"required"`
	ReferencePath string             `json:"reference_path" binding:"required"`
	OverlapRegion ReferenceRegionDTO `json:"overlap_region" binding:"required"`
	OutputName    string             `json:"output_name"`
	OutputFormat  string             `json:"output_format"`
}

// DodgingBalanceRequest Dodging匀光请求
type DodgingBalanceRequest struct {
	SourcePath   string  `json:"source_path" binding:"required"`
	BlockSize    int     `json:"block_size"` // 默认128
	Strength     float64 `json:"strength"`   // [0, 1]
	OutputName   string  `json:"output_name"`
	OutputFormat string  `json:"output_format"`
}

// GradientBlendRequest 渐变融合请求
type GradientBlendRequest struct {
	SourcePath1   string             `json:"source_path1" binding:"required"`
	SourcePath2   string             `json:"source_path2" binding:"required"`
	OverlapRegion ReferenceRegionDTO `json:"overlap_region" binding:"required"`
	BlendWidth    int                `json:"blend_width"`
	OutputName    string             `json:"output_name"`
	OutputFormat  string             `json:"output_format"`
}

// ColorBalanceRequest 通用匀色请求
type ColorBalanceRequest struct {
	SourcePath    string              `json:"source_path" binding:"required"`
	ReferencePath string              `json:"reference_path"`
	Method        int                 `json:"method" binding:"required"` // 0-直方图匹配,1-均值标准差,2-Wallis,3-矩匹配,4-线性回归,5-Dodging
	Strength      float64             `json:"strength"`
	OverlapRegion *ReferenceRegionDTO `json:"overlap_region"`
	WallisC       float64             `json:"wallis_c"`
	WallisB       float64             `json:"wallis_b"`
	TargetMean    float64             `json:"target_mean"`
	TargetStd     float64             `json:"target_std"`
	OutputName    string              `json:"output_name"`
	OutputFormat  string              `json:"output_format"`
}

// AutoColorBalanceRequest 自动匀色请求
type AutoColorBalanceRequest struct {
	SourcePath    string              `json:"source_path" binding:"required"`
	ReferencePath string              `json:"reference_path" binding:"required"`
	OverlapRegion *ReferenceRegionDTO `json:"overlap_region"`
	OutputName    string              `json:"output_name"`
	OutputFormat  string              `json:"output_format"`
}

// BatchColorBalanceRequest 批量匀色请求
type BatchColorBalanceRequest struct {
	SourcePaths   []string            `json:"source_paths" binding:"required,min=1"`
	ReferencePath string              `json:"reference_path" binding:"required"`
	Method        int                 `json:"method"`
	Strength      float64             `json:"strength"`
	OverlapRegion *ReferenceRegionDTO `json:"overlap_region"`
	OutputFormat  string              `json:"output_format"`
}

// PipelineStep 管道步骤
type PipelineStep struct {
	Operation string                 `json:"operation"` // brightness, contrast, saturation, gamma, hue, auto_levels, auto_white_balance, clahe
	Params    map[string]interface{} `json:"params"`
}

// ColorPipelineRequest 调色管道请求
type ColorPipelineRequest struct {
	SourcePath   string         `json:"source_path" binding:"required"`
	Steps        []PipelineStep `json:"steps" binding:"required,min=1"`
	OutputName   string         `json:"output_name"`
	OutputFormat string         `json:"output_format"`
}

// ==================== 响应结构 ====================

// ColorTaskResponse 调色任务响应
type ColorTaskResponse struct {
	TaskID     string `json:"task_id"`
	OutputPath string `json:"output_path"`
	Message    string `json:"message"`
}

// BatchColorTaskResponse 批量任务响应
type BatchColorTaskResponse struct {
	TaskID      string   `json:"task_id"`
	OutputPaths []string `json:"output_paths"`
	Message     string   `json:"message"`
}

// ==================== 调色服务方法 ====================

// StartColorAdjustTask 启动综合调色任务
func (s *RasterService) StartColorAdjustTask(req *ColorAdjustRequest) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, "color_adjust")
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath, outputPath, "color_adjust", argsJSON); err != nil {
		return nil, err
	}

	go s.executeColorAdjustTask(taskID, req, outputPath)

	return &ColorTaskResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "调色任务已提交",
	}, nil
}

func (s *RasterService) executeColorAdjustTask(taskID string, req *ColorAdjustRequest, outputPath string) {
	s.executeWithRecover(taskID, func() error {
		rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
		if err != nil {
			return err
		}
		defer rd.Close()

		params := &Gogeo.ColorAdjustParams{
			Brightness: req.Brightness,
			Contrast:   req.Contrast,
			Saturation: req.Saturation,
			Gamma:      req.Gamma,
			Hue:        req.Hue,
		}

		result, err := rd.AdjustColors(params)
		if err != nil {
			return err
		}
		defer result.Close()

		return result.ExportToFile(outputPath, s.getFormat(req.OutputFormat), nil)
	})
}

// StartBrightnessTask 启动亮度调整任务
func (s *RasterService) StartBrightnessTask(req *SingleColorAdjustRequest) (*ColorTaskResponse, error) {
	return s.startSingleAdjustTask(req, "brightness", func(rd *Gogeo.RasterDataset) (*Gogeo.RasterDataset, error) {
		return rd.AdjustBrightness(req.Value)
	})
}

// StartContrastTask 启动对比度调整任务
func (s *RasterService) StartContrastTask(req *SingleColorAdjustRequest) (*ColorTaskResponse, error) {
	return s.startSingleAdjustTask(req, "contrast", func(rd *Gogeo.RasterDataset) (*Gogeo.RasterDataset, error) {
		return rd.AdjustContrast(req.Value)
	})
}

// StartSaturationTask 启动饱和度调整任务
func (s *RasterService) StartSaturationTask(req *SingleColorAdjustRequest) (*ColorTaskResponse, error) {
	return s.startSingleAdjustTask(req, "saturation", func(rd *Gogeo.RasterDataset) (*Gogeo.RasterDataset, error) {
		return rd.AdjustSaturation(req.Value)
	})
}

// StartGammaTask 启动Gamma校正任务
func (s *RasterService) StartGammaTask(req *SingleColorAdjustRequest) (*ColorTaskResponse, error) {
	return s.startSingleAdjustTask(req, "gamma", func(rd *Gogeo.RasterDataset) (*Gogeo.RasterDataset, error) {
		return rd.AdjustGamma(req.Value)
	})
}

// StartHueTask 启动色相调整任务
func (s *RasterService) StartHueTask(req *SingleColorAdjustRequest) (*ColorTaskResponse, error) {
	return s.startSingleAdjustTask(req, "hue", func(rd *Gogeo.RasterDataset) (*Gogeo.RasterDataset, error) {
		return rd.AdjustHue(req.Value)
	})
}

// StartLevelsTask 启动色阶调整任务
func (s *RasterService) StartLevelsTask(req *LevelsAdjustRequest) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, "levels")
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath, outputPath, "levels", argsJSON); err != nil {
		return nil, err
	}

	go s.executeWithRecover(taskID, func() error {
		rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
		if err != nil {
			return err
		}
		defer rd.Close()

		params := &Gogeo.LevelsParams{
			InputMin:  req.InputMin,
			InputMax:  req.InputMax,
			OutputMin: req.OutputMin,
			OutputMax: req.OutputMax,
			Midtone:   req.Midtone,
		}

		result, err := rd.AdjustLevels(params, req.BandIndex)
		if err != nil {
			return err
		}
		defer result.Close()

		return result.ExportToFile(outputPath, s.getFormat(req.OutputFormat), nil)
	})

	return &ColorTaskResponse{TaskID: taskID, OutputPath: outputPath, Message: "色阶调整任务已提交"}, nil
}

// StartCurvesTask 启动曲线调整任务
func (s *RasterService) StartCurvesTask(req *CurvesAdjustRequest) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, "curves")
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath, outputPath, "curves", argsJSON); err != nil {
		return nil, err
	}

	go s.executeWithRecover(taskID, func() error {
		rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
		if err != nil {
			return err
		}
		defer rd.Close()

		points := make([]Gogeo.CurvePoint, len(req.Points))
		for i, p := range req.Points {
			points[i] = Gogeo.CurvePoint{Input: p.Input, Output: p.Output}
		}

		params := &Gogeo.CurveParams{
			Points:  points,
			Channel: req.Channel,
		}

		result, err := rd.AdjustCurves(params)
		if err != nil {
			return err
		}
		defer result.Close()

		return result.ExportToFile(outputPath, s.getFormat(req.OutputFormat), nil)
	})

	return &ColorTaskResponse{TaskID: taskID, OutputPath: outputPath, Message: "曲线调整任务已提交"}, nil
}

// StartAutoLevelsTask 启动自动色阶任务
func (s *RasterService) StartAutoLevelsTask(req *AutoAdjustRequest) (*ColorTaskResponse, error) {
	return s.startAutoAdjustTask(req, "auto_levels", func(rd *Gogeo.RasterDataset) (*Gogeo.RasterDataset, error) {
		clipPercent := req.ClipPercent
		if clipPercent <= 0 {
			clipPercent = 0.5
		}
		return rd.AutoLevels(clipPercent)
	})
}

// StartAutoContrastTask 启动自动对比度任务
func (s *RasterService) StartAutoContrastTask(req *AutoAdjustRequest) (*ColorTaskResponse, error) {
	return s.startAutoAdjustTask(req, "auto_contrast", func(rd *Gogeo.RasterDataset) (*Gogeo.RasterDataset, error) {
		return rd.AutoContrast()
	})
}

// StartAutoWhiteBalanceTask 启动自动白平衡任务
func (s *RasterService) StartAutoWhiteBalanceTask(req *AutoAdjustRequest) (*ColorTaskResponse, error) {
	return s.startAutoAdjustTask(req, "auto_white_balance", func(rd *Gogeo.RasterDataset) (*Gogeo.RasterDataset, error) {
		return rd.AutoWhiteBalance()
	})
}

// StartHistogramEqualizeTask 启动直方图均衡化任务
func (s *RasterService) StartHistogramEqualizeTask(req *HistogramEqualizeRequest) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, "histogram_equalize")
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath, outputPath, "histogram_equalize", argsJSON); err != nil {
		return nil, err
	}

	go s.executeWithRecover(taskID, func() error {
		rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
		if err != nil {
			return err
		}
		defer rd.Close()

		result, err := rd.HistogramEqualization(req.BandIndex)
		if err != nil {
			return err
		}
		defer result.Close()

		return result.ExportToFile(outputPath, s.getFormat(req.OutputFormat), nil)
	})

	return &ColorTaskResponse{TaskID: taskID, OutputPath: outputPath, Message: "直方图均衡化任务已提交"}, nil
}

// StartCLAHETask 启动CLAHE均衡化任务
func (s *RasterService) StartCLAHETask(req *CLAHERequest) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, "clahe")
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath, outputPath, "clahe", argsJSON); err != nil {
		return nil, err
	}

	go s.executeWithRecover(taskID, func() error {
		rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
		if err != nil {
			return err
		}
		defer rd.Close()

		tileSize := req.TileSize
		if tileSize <= 0 {
			tileSize = 8
		}
		clipLimit := req.ClipLimit
		if clipLimit <= 0 {
			clipLimit = 2.0
		}

		result, err := rd.CLAHEEqualization(tileSize, clipLimit)
		if err != nil {
			return err
		}
		defer result.Close()

		return result.ExportToFile(outputPath, s.getFormat(req.OutputFormat), nil)
	})

	return &ColorTaskResponse{TaskID: taskID, OutputPath: outputPath, Message: "CLAHE均衡化任务已提交"}, nil
}

// StartPresetColorTask 启动预设调色任务
func (s *RasterService) StartPresetColorTask(req *PresetColorRequest) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, "preset_"+req.PresetName)
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath, outputPath, "preset_color", argsJSON); err != nil {
		return nil, err
	}

	go s.executeWithRecover(taskID, func() error {
		rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
		if err != nil {
			return err
		}
		defer rd.Close()

		var result *Gogeo.RasterDataset
		switch req.PresetName {
		case "vivid":
			result, err = rd.PresetVivid()
		case "soft":
			result, err = rd.PresetSoft()
		case "high_contrast":
			result, err = rd.PresetHighContrast()
		case "warm":
			result, err = rd.PresetWarm()
		case "cool":
			result, err = rd.PresetCool()
		case "black_white":
			result, err = rd.PresetBlackWhite()
		case "sepia":
			result, err = rd.PresetSepia()
		default:
			return fmt.Errorf("未知预设: %s", req.PresetName)
		}

		if err != nil {
			return err
		}
		defer result.Close()

		return result.ExportToFile(outputPath, s.getFormat(req.OutputFormat), nil)
	})

	return &ColorTaskResponse{TaskID: taskID, OutputPath: outputPath, Message: "预设调色任务已提交"}, nil
}

// StartSCurveTask 启动S曲线对比度任务
func (s *RasterService) StartSCurveTask(req *SCurveRequest) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, "scurve")
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath, outputPath, "scurve", argsJSON); err != nil {
		return nil, err
	}

	go s.executeWithRecover(taskID, func() error {
		rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
		if err != nil {
			return err
		}
		defer rd.Close()

		result, err := rd.SCurveContrast(req.Strength)
		if err != nil {
			return err
		}
		defer result.Close()

		return result.ExportToFile(outputPath, s.getFormat(req.OutputFormat), nil)
	})

	return &ColorTaskResponse{TaskID: taskID, OutputPath: outputPath, Message: "S曲线对比度任务已提交"}, nil
}

// ==================== 匀色服务方法 ====================

// GetColorStatistics 获取颜色统计信息（同步）
func (s *RasterService) GetColorStatistics(req *GetColorStatisticsRequest) (*ColorStatisticsResponse, error) {
	rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
	if err != nil {
		return nil, fmt.Errorf("打开栅格文件失败: %w", err)
	}
	defer rd.Close()

	var region *Gogeo.ReferenceRegion
	if req.Region != nil {
		region = &Gogeo.ReferenceRegion{
			X:      req.Region.X,
			Y:      req.Region.Y,
			Width:  req.Region.Width,
			Height: req.Region.Height,
		}
	}

	stats, err := rd.GetColorStatistics(region)
	if err != nil {
		return nil, err
	}

	return &ColorStatisticsResponse{
		MeanR: stats.MeanR, MeanG: stats.MeanG, MeanB: stats.MeanB,
		StdR: stats.StdR, StdG: stats.StdG, StdB: stats.StdB,
		MinR: stats.MinR, MinG: stats.MinG, MinB: stats.MinB,
		MaxR: stats.MaxR, MaxG: stats.MaxG, MaxB: stats.MaxB,
	}, nil
}

// 继续 services/raster_color_service.go

// GetBandStatistics 获取波段统计信息（同步）- 续
func (s *RasterService) GetBandStatistics(req *GetBandStatisticsRequest) (*BandStatisticsResponse, error) {
	rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
	if err != nil {
		return nil, fmt.Errorf("打开栅格文件失败: %w", err)
	}
	defer rd.Close()

	var region *Gogeo.ReferenceRegion
	if req.Region != nil {
		region = &Gogeo.ReferenceRegion{
			X:      req.Region.X,
			Y:      req.Region.Y,
			Width:  req.Region.Width,
			Height: req.Region.Height,
		}
	}

	stats, err := rd.GetBandStatistics(req.BandIndex, region)
	if err != nil {
		return nil, err
	}

	return &BandStatisticsResponse{
		Min:       stats.Min,
		Max:       stats.Max,
		Mean:      stats.Mean,
		Stddev:    stats.Stddev,
		Histogram: stats.Histogram,
	}, nil
}

// StartHistogramMatchTask 启动直方图匹配任务
func (s *RasterService) StartHistogramMatchTask(req *HistogramMatchRequest) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, "histogram_match")
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath, outputPath, "histogram_match", argsJSON); err != nil {
		return nil, err
	}

	go s.executeWithRecover(taskID, func() error {
		srcRD, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
		if err != nil {
			return err
		}
		defer srcRD.Close()

		refRD, err := Gogeo.OpenRasterDataset(req.ReferencePath, false)
		if err != nil {
			return err
		}
		defer refRD.Close()

		srcRegion := s.convertRegion(req.SrcRegion)
		refRegion := s.convertRegion(req.RefRegion)

		result, err := srcRD.HistogramMatch(refRD, srcRegion, refRegion)
		if err != nil {
			return err
		}
		defer result.Close()

		return result.ExportToFile(outputPath, s.getFormat(req.OutputFormat), nil)
	})

	return &ColorTaskResponse{TaskID: taskID, OutputPath: outputPath, Message: "直方图匹配任务已提交"}, nil
}

// StartMeanStdMatchTask 启动均值-标准差匹配任务
func (s *RasterService) StartMeanStdMatchTask(req *MeanStdMatchRequest) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, "mean_std_match")
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath, outputPath, "mean_std_match", argsJSON); err != nil {
		return nil, err
	}

	go s.executeWithRecover(taskID, func() error {
		rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
		if err != nil {
			return err
		}
		defer rd.Close()

		targetStats := &Gogeo.ColorStatistics{
			MeanR: req.TargetMeanR,
			MeanG: req.TargetMeanG,
			MeanB: req.TargetMeanB,
			StdR:  req.TargetStdR,
			StdG:  req.TargetStdG,
			StdB:  req.TargetStdB,
		}

		region := s.convertRegion(req.Region)
		strength := req.Strength
		if strength <= 0 {
			strength = 1.0
		}

		result, err := rd.MeanStdMatch(targetStats, region, strength)
		if err != nil {
			return err
		}
		defer result.Close()

		return result.ExportToFile(outputPath, s.getFormat(req.OutputFormat), nil)
	})

	return &ColorTaskResponse{TaskID: taskID, OutputPath: outputPath, Message: "均值-标准差匹配任务已提交"}, nil
}

// StartWallisFilterTask 启动Wallis滤波任务
func (s *RasterService) StartWallisFilterTask(req *WallisFilterRequest) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, "wallis_filter")
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath, outputPath, "wallis_filter", argsJSON); err != nil {
		return nil, err
	}

	go s.executeWithRecover(taskID, func() error {
		rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
		if err != nil {
			return err
		}
		defer rd.Close()

		windowSize := req.WindowSize
		if windowSize <= 0 {
			windowSize = 31
		}

		result, err := rd.WallisFilter(req.TargetMean, req.TargetStd, req.C, req.B, windowSize)
		if err != nil {
			return err
		}
		defer result.Close()

		return result.ExportToFile(outputPath, s.getFormat(req.OutputFormat), nil)
	})

	return &ColorTaskResponse{TaskID: taskID, OutputPath: outputPath, Message: "Wallis滤波任务已提交"}, nil
}

// StartMomentMatchTask 启动矩匹配任务
func (s *RasterService) StartMomentMatchTask(req *MomentMatchRequest) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, "moment_match")
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath, outputPath, "moment_match", argsJSON); err != nil {
		return nil, err
	}

	go s.executeWithRecover(taskID, func() error {
		srcRD, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
		if err != nil {
			return err
		}
		defer srcRD.Close()

		refRD, err := Gogeo.OpenRasterDataset(req.ReferencePath, false)
		if err != nil {
			return err
		}
		defer refRD.Close()

		srcRegion := s.convertRegion(req.SrcRegion)
		refRegion := s.convertRegion(req.RefRegion)

		result, err := srcRD.MomentMatch(refRD, srcRegion, refRegion)
		if err != nil {
			return err
		}
		defer result.Close()

		return result.ExportToFile(outputPath, s.getFormat(req.OutputFormat), nil)
	})

	return &ColorTaskResponse{TaskID: taskID, OutputPath: outputPath, Message: "矩匹配任务已提交"}, nil
}

// StartLinearRegressionBalanceTask 启动线性回归匀色任务
func (s *RasterService) StartLinearRegressionBalanceTask(req *LinearRegressionBalanceRequest) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, "linear_regression")
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath, outputPath, "linear_regression", argsJSON); err != nil {
		return nil, err
	}

	go s.executeWithRecover(taskID, func() error {
		srcRD, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
		if err != nil {
			return err
		}
		defer srcRD.Close()

		refRD, err := Gogeo.OpenRasterDataset(req.ReferencePath, false)
		if err != nil {
			return err
		}
		defer refRD.Close()

		overlapRegion := &Gogeo.ReferenceRegion{
			X:      req.OverlapRegion.X,
			Y:      req.OverlapRegion.Y,
			Width:  req.OverlapRegion.Width,
			Height: req.OverlapRegion.Height,
		}

		result, err := srcRD.LinearRegressionBalance(refRD, overlapRegion)
		if err != nil {
			return err
		}
		defer result.Close()

		return result.ExportToFile(outputPath, s.getFormat(req.OutputFormat), nil)
	})

	return &ColorTaskResponse{TaskID: taskID, OutputPath: outputPath, Message: "线性回归匀色任务已提交"}, nil
}

// 继续 services/raster_color_service.go

// StartDodgingBalanceTask - 续
func (s *RasterService) StartDodgingBalanceTask(req *DodgingBalanceRequest) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, "dodging")
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath, outputPath, "dodging", argsJSON); err != nil {
		return nil, err
	}

	go s.executeWithRecover(taskID, func() error {
		rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
		if err != nil {
			return err
		}
		defer rd.Close()

		blockSize := req.BlockSize
		if blockSize <= 0 {
			blockSize = 128
		}
		strength := req.Strength
		if strength <= 0 {
			strength = 0.8
		}

		result, err := rd.DodgingBalance(blockSize, strength)
		if err != nil {
			return err
		}
		defer result.Close()

		return result.ExportToFile(outputPath, s.getFormat(req.OutputFormat), nil)
	})

	return &ColorTaskResponse{TaskID: taskID, OutputPath: outputPath, Message: "Dodging匀光任务已提交"}, nil
}

// StartGradientBlendTask 启动渐变融合任务
func (s *RasterService) StartGradientBlendTask(req *GradientBlendRequest) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, "gradient_blend")
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath1, outputPath, "gradient_blend", argsJSON); err != nil {
		return nil, err
	}

	go s.executeWithRecover(taskID, func() error {
		rd1, err := Gogeo.OpenRasterDataset(req.SourcePath1, false)
		if err != nil {
			return err
		}
		defer rd1.Close()

		rd2, err := Gogeo.OpenRasterDataset(req.SourcePath2, false)
		if err != nil {
			return err
		}
		defer rd2.Close()

		overlapRegion := &Gogeo.ReferenceRegion{
			X:      req.OverlapRegion.X,
			Y:      req.OverlapRegion.Y,
			Width:  req.OverlapRegion.Width,
			Height: req.OverlapRegion.Height,
		}

		blendWidth := req.BlendWidth
		if blendWidth <= 0 {
			blendWidth = 50
		}

		result, err := rd1.GradientBlend(rd2, overlapRegion, blendWidth)
		if err != nil {
			return err
		}
		defer result.Close()

		return result.ExportToFile(outputPath, s.getFormat(req.OutputFormat), nil)
	})

	return &ColorTaskResponse{TaskID: taskID, OutputPath: outputPath, Message: "渐变融合任务已提交"}, nil
}

// StartColorBalanceTask 启动通用匀色任务
func (s *RasterService) StartColorBalanceTask(req *ColorBalanceRequest) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, "color_balance")
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath, outputPath, "color_balance", argsJSON); err != nil {
		return nil, err
	}

	go s.executeWithRecover(taskID, func() error {
		srcRD, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
		if err != nil {
			return err
		}
		defer srcRD.Close()

		var refRD *Gogeo.RasterDataset
		if req.ReferencePath != "" {
			refRD, err = Gogeo.OpenRasterDataset(req.ReferencePath, false)
			if err != nil {
				return err
			}
			defer refRD.Close()
		}

		params := &Gogeo.ColorBalanceParams{
			Method:     Gogeo.ColorBalanceMethod(req.Method),
			Strength:   req.Strength,
			WallisC:    req.WallisC,
			WallisB:    req.WallisB,
			TargetMean: req.TargetMean,
			TargetStd:  req.TargetStd,
		}

		if req.OverlapRegion != nil {
			params.OverlapRegion = &Gogeo.ReferenceRegion{
				X:      req.OverlapRegion.X,
				Y:      req.OverlapRegion.Y,
				Width:  req.OverlapRegion.Width,
				Height: req.OverlapRegion.Height,
			}
		}

		result, err := srcRD.ColorBalance(refRD, params)
		if err != nil {
			return err
		}
		defer result.Close()

		return result.ExportToFile(outputPath, s.getFormat(req.OutputFormat), nil)
	})

	return &ColorTaskResponse{TaskID: taskID, OutputPath: outputPath, Message: "匀色任务已提交"}, nil
}

// StartAutoColorBalanceTask 启动自动匀色任务
// 继续 services/raster_color_service.go

// StartAutoColorBalanceTask - 续
func (s *RasterService) StartAutoColorBalanceTask(req *AutoColorBalanceRequest) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, "auto_color_balance")
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath, outputPath, "auto_color_balance", argsJSON); err != nil {
		return nil, err
	}

	go s.executeWithRecover(taskID, func() error {
		srcRD, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
		if err != nil {
			return err
		}
		defer srcRD.Close()

		refRD, err := Gogeo.OpenRasterDataset(req.ReferencePath, false)
		if err != nil {
			return err
		}
		defer refRD.Close()

		var overlapRegion *Gogeo.ReferenceRegion
		if req.OverlapRegion != nil {
			overlapRegion = &Gogeo.ReferenceRegion{
				X:      req.OverlapRegion.X,
				Y:      req.OverlapRegion.Y,
				Width:  req.OverlapRegion.Width,
				Height: req.OverlapRegion.Height,
			}
		}

		result, err := srcRD.SmartColorBalance(refRD, overlapRegion)
		if err != nil {
			return err
		}
		defer result.Close()

		return result.ExportToFile(outputPath, s.getFormat(req.OutputFormat), nil)
	})

	return &ColorTaskResponse{TaskID: taskID, OutputPath: outputPath, Message: "自动匀色任务已提交"}, nil
}

// StartBatchColorBalanceTask 启动批量匀色任务
func (s *RasterService) StartBatchColorBalanceTask(req *BatchColorBalanceRequest) (*BatchColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}

	outputPaths := make([]string, len(req.SourcePaths))
	for i, srcPath := range req.SourcePaths {
		baseName := filepath.Base(srcPath)
		ext := filepath.Ext(baseName)
		name := baseName[:len(baseName)-len(ext)]
		outputPaths[i] = filepath.Join(outputDir, name+"_balanced"+getFormatExtension(s.getFormat(req.OutputFormat)))
	}

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePaths[0],
		OutputPath: outputDir,
		Status:     0,
		TypeName:   "batch_color_balance",
		Args:       datatypes.JSON(argsJSON),
	}
	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeBatchColorBalanceTask(taskID, req, outputPaths)

	return &BatchColorTaskResponse{
		TaskID:      taskID,
		OutputPaths: outputPaths,
		Message:     "批量匀色任务已提交",
	}, nil
}

func (s *RasterService) executeBatchColorBalanceTask(taskID string, req *BatchColorBalanceRequest, outputPaths []string) {
	var finalStatus int = 1
	defer func() {
		if r := recover(); r != nil {
			finalStatus = 2
		}
		models.DB.Model(&models.RasterRecord{}).Where("task_id = ?", taskID).Update("status", finalStatus)
	}()

	// 打开参考数据集
	refRD, err := Gogeo.OpenRasterDataset(req.ReferencePath, false)
	if err != nil {
		finalStatus = 2
		return
	}
	defer refRD.Close()

	// 构建匀色参数
	params := &Gogeo.ColorBalanceParams{
		Method:   Gogeo.ColorBalanceMethod(req.Method),
		Strength: req.Strength,
	}
	if req.OverlapRegion != nil {
		params.OverlapRegion = &Gogeo.ReferenceRegion{
			X:      req.OverlapRegion.X,
			Y:      req.OverlapRegion.Y,
			Width:  req.OverlapRegion.Width,
			Height: req.OverlapRegion.Height,
		}
	}

	// 处理每个源文件
	for i, srcPath := range req.SourcePaths {
		srcRD, err := Gogeo.OpenRasterDataset(srcPath, false)
		if err != nil {
			continue
		}

		result, err := srcRD.ColorBalance(refRD, params)
		if err != nil {
			srcRD.Close()
			continue
		}

		err = result.ExportToFile(outputPaths[i], s.getFormat(req.OutputFormat), nil)
		result.Close()
		srcRD.Close()

		if err != nil {
			continue
		}
	}
}

// StartColorPipelineTask 启动调色管道任务
func (s *RasterService) StartColorPipelineTask(req *ColorPipelineRequest) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, "color_pipeline")
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath, outputPath, "color_pipeline", argsJSON); err != nil {
		return nil, err
	}

	go s.executeColorPipelineTask(taskID, req, outputPath)

	return &ColorTaskResponse{TaskID: taskID, OutputPath: outputPath, Message: "调色管道任务已提交"}, nil
}

func (s *RasterService) executeColorPipelineTask(taskID string, req *ColorPipelineRequest, outputPath string) {
	s.executeWithRecover(taskID, func() error {
		rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
		if err != nil {
			return err
		}
		defer rd.Close()

		pipeline := rd.NewColorPipeline()

		for _, step := range req.Steps {
			switch step.Operation {
			case "brightness":
				if v, ok := step.Params["value"].(float64); ok {
					pipeline = pipeline.Brightness(v)
				}
			case "contrast":
				if v, ok := step.Params["value"].(float64); ok {
					pipeline = pipeline.Contrast(v)
				}
			case "saturation":
				if v, ok := step.Params["value"].(float64); ok {
					pipeline = pipeline.Saturation(v)
				}
			case "gamma":
				if v, ok := step.Params["value"].(float64); ok {
					pipeline = pipeline.Gamma(v)
				}
			case "hue":
				if v, ok := step.Params["value"].(float64); ok {
					pipeline = pipeline.Hue(v)
				}
			case "auto_levels":
				clipPercent := 0.5
				if v, ok := step.Params["clip_percent"].(float64); ok {
					clipPercent = v
				}
				pipeline = pipeline.AutoLevels(clipPercent)
			case "auto_white_balance":
				pipeline = pipeline.AutoWhiteBalance()
			case "clahe":
				tileSize := 8
				clipLimit := 2.0
				if v, ok := step.Params["tile_size"].(float64); ok {
					tileSize = int(v)
				}
				if v, ok := step.Params["clip_limit"].(float64); ok {
					clipLimit = v
				}
				pipeline = pipeline.CLAHE(tileSize, clipLimit)
			}
		}

		return pipeline.Export(outputPath, s.getFormat(req.OutputFormat))
	})
}

// ==================== 辅助方法 ====================

// prepareOutputPath 准备输出路径
func (s *RasterService) prepareOutputPath(taskID, outputName, outputFormat, defaultName string) (string, error) {
	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return "", fmt.Errorf("创建输出目录失败: %w", err)
	}

	if outputName == "" {
		outputName = defaultName
	}
	format := s.getFormat(outputFormat)
	ext := getFormatExtension(format)

	return filepath.Join(outputDir, outputName+ext), nil
}

// createTaskRecord 创建任务记录
func (s *RasterService) createTaskRecord(taskID, sourcePath, outputPath, typeName string, argsJSON []byte) error {
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: sourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   typeName,
		Args:       datatypes.JSON(argsJSON),
	}
	return models.DB.Create(record).Error
}

// executeWithRecover 带恢复的执行
func (s *RasterService) executeWithRecover(taskID string, fn func() error) {
	var finalStatus int = 1
	defer func() {
		if r := recover(); r != nil {
			finalStatus = 2
		}
		models.DB.Model(&models.RasterRecord{}).Where("task_id = ?", taskID).Update("status", finalStatus)
	}()

	if err := fn(); err != nil {
		finalStatus = 2
	}
}

// getFormat 获取格式
func (s *RasterService) getFormat(format string) string {
	if format == "" {
		return "GTiff"
	}
	return format
}

// convertRegion 转换区域
func (s *RasterService) convertRegion(region *ReferenceRegionDTO) *Gogeo.ReferenceRegion {
	if region == nil {
		return nil
	}
	return &Gogeo.ReferenceRegion{
		X:      region.X,
		Y:      region.Y,
		Width:  region.Width,
		Height: region.Height,
	}
}

// startSingleAdjustTask 启动单项调整任务的通用方法
func (s *RasterService) startSingleAdjustTask(req *SingleColorAdjustRequest, typeName string, adjustFn func(*Gogeo.RasterDataset) (*Gogeo.RasterDataset, error)) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, typeName)
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath, outputPath, typeName, argsJSON); err != nil {
		return nil, err
	}

	go s.executeWithRecover(taskID, func() error {
		rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
		if err != nil {
			return err
		}
		defer rd.Close()

		result, err := adjustFn(rd)
		if err != nil {
			return err
		}
		defer result.Close()

		return result.ExportToFile(outputPath, s.getFormat(req.OutputFormat), nil)
	})

	return &ColorTaskResponse{TaskID: taskID, OutputPath: outputPath, Message: typeName + "任务已提交"}, nil
}

// startAutoAdjustTask 启动自动调整任务的通用方法
func (s *RasterService) startAutoAdjustTask(req *AutoAdjustRequest, typeName string, adjustFn func(*Gogeo.RasterDataset) (*Gogeo.RasterDataset, error)) (*ColorTaskResponse, error) {
	taskID := uuid.New().String()
	outputPath, err := s.prepareOutputPath(taskID, req.OutputName, req.OutputFormat, typeName)
	if err != nil {
		return nil, err
	}

	argsJSON, _ := json.Marshal(req)
	if err := s.createTaskRecord(taskID, req.SourcePath, outputPath, typeName, argsJSON); err != nil {
		return nil, err
	}

	go s.executeWithRecover(taskID, func() error {
		rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
		if err != nil {
			return err
		}
		defer rd.Close()

		result, err := adjustFn(rd)
		if err != nil {
			return err
		}
		defer result.Close()

		return result.ExportToFile(outputPath, s.getFormat(req.OutputFormat), nil)
	})

	return &ColorTaskResponse{TaskID: taskID, OutputPath: outputPath, Message: typeName + "任务已提交"}, nil
}
