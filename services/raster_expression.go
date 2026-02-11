package services

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/GrainArc/Gogeo"
	"github.com/GrainArc/SouceMap/config"
	"github.com/GrainArc/SouceMap/models"
	"github.com/google/uuid"
	"gorm.io/datatypes"
)

// ==================== 请求结构体 ====================

// ExpressionRequest 表达式计算请求
type ExpressionRequest struct {
	SourcePath string `json:"source_path" binding:"required"`
	Expression string `json:"expression" binding:"required"`
	OutputName string `json:"output_name"`
	TargetBand int    `json:"target_band"` // 写入目标波段（0表示创建新文件）
}

// ExpressionWithConditionRequest 条件表达式计算请求

type ExpressionWithConditionRequest struct {
	SourcePath  string  `json:"source_path" binding:"required"`
	Expression  string  `json:"expression" binding:"required"`
	Condition   string  `json:"condition"`
	NoDataValue float64 `json:"nodata_value"`
	OutputName  string  `json:"output_name"`
	TargetBand  int     `json:"target_band"` // >0: 写入源数据集的指定波段后导出; 0: 创建单波段新文件
	SetNoData   bool    `json:"set_nodata"`  // 是否将nodata_value设为波段NoData元数据
}

// ConditionalReplaceRequest 条件替换请求
type ConditionalReplaceRequest struct {
	SourcePath string             `json:"source_path" binding:"required"`
	BandIndex  int                `json:"band_index" binding:"required,min=1"`
	Conditions []ReplaceCondition `json:"conditions" binding:"required,min=1"`
	OutputName string             `json:"output_name"`
}

// ReplaceCondition 替换条件
type ReplaceCondition struct {
	MinValue   float64 `json:"min_value"`
	MaxValue   float64 `json:"max_value"`
	NewValue   float64 `json:"new_value"`
	IncludeMin bool    `json:"include_min"`
	IncludeMax bool    `json:"include_max"`
}

// BatchExpressionRequest 批量表达式请求
type BatchExpressionRequest struct {
	SourcePath  string   `json:"source_path" binding:"required"`
	Expressions []string `json:"expressions" binding:"required,min=1"`
	OutputName  string   `json:"output_name"`
}

// BlockCalculateRequest 分块计算请求
type BlockCalculateRequest struct {
	SourcePath  string `json:"source_path" binding:"required"`
	Expression  string `json:"expression" binding:"required"`
	BlockWidth  int    `json:"block_width"`
	BlockHeight int    `json:"block_height"`
	OutputName  string `json:"output_name"`
}

// ValidateExpressionRequest 验证表达式请求
type ValidateExpressionRequest struct {
	SourcePath string `json:"source_path" binding:"required"`
	Expression string `json:"expression" binding:"required"`
}

// IndexCalculateRequest 遥感指数计算通用请求
type IndexCalculateRequest struct {
	SourcePath string  `json:"source_path" binding:"required"`
	NIRBand    int     `json:"nir_band"`
	RedBand    int     `json:"red_band"`
	GreenBand  int     `json:"green_band"`
	BlueBand   int     `json:"blue_band"`
	SWIRBand   int     `json:"swir_band"`
	LFactor    float64 `json:"l_factor"` // SAVI的L参数
	OutputName string  `json:"output_name"`
}

// ==================== 响应结构体 ====================

// CalcOperationResponse 计算操作响应
type CalcOperationResponse struct {
	TaskID     string `json:"task_id"`
	OutputPath string `json:"output_path"`
	Message    string `json:"message"`
}

// ValidateExpressionResponse 验证表达式响应
type ValidateExpressionResponse struct {
	Valid   bool   `json:"valid"`
	Message string `json:"message"`
}

// BatchExpressionResponse 批量表达式响应
type BatchExpressionResponse struct {
	TaskID      string   `json:"task_id"`
	OutputPaths []string `json:"output_paths"`
	Message     string   `json:"message"`
}

// ==================== 服务方法实现 ====================

// ValidateExpression 验证表达式
func (s *RasterService) ValidateExpression(req *ValidateExpressionRequest) (*ValidateExpressionResponse, error) {
	rd, err := Gogeo.OpenRasterDataset(req.SourcePath, false)
	if err != nil {
		return nil, fmt.Errorf("打开栅格文件失败: %w", err)
	}
	defer rd.Close()

	calc := rd.NewBandCalculator()
	err = calc.ValidateExpression(req.Expression)
	if err != nil {
		return &ValidateExpressionResponse{
			Valid:   false,
			Message: err.Error(),
		}, nil
	}

	return &ValidateExpressionResponse{
		Valid:   true,
		Message: "表达式有效",
	}, nil
}

// StartExpressionTask 启动表达式计算任务
func (s *RasterService) StartExpressionTask(req *ExpressionRequest) (*CalcOperationResponse, error) {
	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}
	outputName := req.OutputName
	if outputName == "" {
		outputName = "calc_result"
	}
	outputPath := filepath.Join(outputDir, outputName+".tif")

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "栅格表达式计算",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeExpressionTask(taskID, req, outputPath)

	return &CalcOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeExpressionTask(taskID string, req *ExpressionRequest, outputPath string) {
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

	calc := rd.NewBandCalculator()

	if req.TargetBand > 0 {
		// 写入指定波段
		if err := calc.CalculateAndWrite(req.Expression, req.TargetBand); err != nil {
			finalStatus = 2
			return
		}
		if err := rd.ExportToFile(outputPath, "GTiff", nil); err != nil {
			finalStatus = 2
			return
		}
	} else {
		// 创建新文件
		result, err := calc.Calculate(req.Expression)
		if err != nil {
			finalStatus = 2
			return
		}

		newRD, err := rd.CreateSingleBandDataset(result, Gogeo.BandReal64)
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

// StartExpressionWithConditionTask 启动条件表达式计算任务
func (s *RasterService) StartExpressionWithConditionTask(req *ExpressionWithConditionRequest) (*CalcOperationResponse, error) {
	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}
	outputName := req.OutputName
	if outputName == "" {
		outputName = "cond_calc_result"
	}
	outputPath := filepath.Join(outputDir, outputName+".tif")

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "expression_with_condition",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeExpressionWithConditionTask(taskID, req, outputPath)

	return &CalcOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeExpressionWithConditionTask(taskID string, req *ExpressionWithConditionRequest, outputPath string) {
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

	calc := rd.NewBandCalculator()

	if req.TargetBand > 0 {
		// ━━━━━━━━━━━━━━━━━━
		// 模式一: 写入源数据集的指定波段，然后导出完整数据集
		// 使用 CalculateWithConditionAndWrite 零拷贝直写，C层一次完成
		// ━━━━━━━━━━
		if err := calc.CalculateWithConditionAndWrite(
			req.Expression,
			req.Condition,
			req.NoDataValue,
			req.TargetBand,
			req.SetNoData,
		); err != nil {
			finalStatus = 2
			return
		}

		// 导出整个数据集（包含所有原始波段，目标波段已被覆写）
		if err := rd.ExportToFile(outputPath, "GTiff", nil); err != nil {
			finalStatus = 2
			return
		}
	} else {
		// ━━━━━━━━━━
		// 模式二: 创建单波段新文件输出
		// ━━━━━━━━━━
		result, err := calc.CalculateWithCondition(req.Expression, req.Condition, req.NoDataValue)
		if err != nil {
			finalStatus = 2
			return
		}

		newRD, err := rd.CreateSingleBandDataset(result, Gogeo.BandReal64)
		if err != nil {
			finalStatus = 2
			return
		}
		defer newRD.Close()

		// 设置NoData元数据
		if req.SetNoData {
			if err := newRD.SetBandNoDataValue(1, req.NoDataValue, false); err != nil {
				finalStatus = 2
				return
			}
		}

		if err := newRD.ExportToFile(outputPath, "GTiff", nil); err != nil {
			finalStatus = 2
			return
		}
	}
}

// StartConditionalReplaceTask 启动条件替换任务
func (s *RasterService) StartConditionalReplaceTask(req *ConditionalReplaceRequest) (*CalcOperationResponse, error) {
	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}
	outputName := req.OutputName
	if outputName == "" {
		outputName = "replace_result"
	}
	outputPath := filepath.Join(outputDir, outputName+".tif")

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "conditional_replace",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeConditionalReplaceTask(taskID, req, outputPath)

	return &CalcOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeConditionalReplaceTask(taskID string, req *ConditionalReplaceRequest, outputPath string) {
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

	calc := rd.NewBandCalculator()

	// 转换条件格式
	conditions := make([]Gogeo.ReplaceCondition, len(req.Conditions))
	for i, c := range req.Conditions {
		conditions[i] = Gogeo.ReplaceCondition{
			MinValue:   c.MinValue,
			MaxValue:   c.MaxValue,
			NewValue:   c.NewValue,
			IncludeMin: c.IncludeMin,
			IncludeMax: c.IncludeMax,
		}
	}

	result, err := calc.ConditionalReplaceMulti(req.BandIndex, conditions)
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

// StartBatchExpressionTask 启动批量表达式计算任务
func (s *RasterService) StartBatchExpressionTask(req *BatchExpressionRequest) (*BatchExpressionResponse, error) {
	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}

	outputPaths := make([]string, len(req.Expressions))
	baseName := req.OutputName
	if baseName == "" {
		baseName = "batch_result"
	}
	for i := range req.Expressions {
		outputPaths[i] = filepath.Join(outputDir, fmt.Sprintf("%s_%d.tif", baseName, i+1))
	}

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputDir,
		Status:     0,
		TypeName:   "batch_expression",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeBatchExpressionTask(taskID, req, outputPaths)

	return &BatchExpressionResponse{
		TaskID:      taskID,
		OutputPaths: outputPaths,
		Message:     "任务已提交",
	}, nil
}

func (s *RasterService) executeBatchExpressionTask(taskID string, req *BatchExpressionRequest, outputPaths []string) {
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

	calc := rd.NewBandCalculator()
	results := calc.CalculateBatch(req.Expressions)

	for i, res := range results {
		if res.Error != nil {
			finalStatus = 2
			return
		}

		newRD, err := rd.CreateSingleBandDataset(res.Data, Gogeo.BandReal64)
		if err != nil {
			finalStatus = 2
			return
		}

		if err := newRD.ExportToFile(outputPaths[i], "GTiff", nil); err != nil {
			newRD.Close()
			finalStatus = 2
			return
		}
		newRD.Close()
	}
}

// StartBlockCalculateTask 启动分块计算任务（大影像）
func (s *RasterService) StartBlockCalculateTask(req *BlockCalculateRequest) (*CalcOperationResponse, error) {
	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}
	outputName := req.OutputName
	if outputName == "" {
		outputName = "block_calc_result"
	}
	outputPath := filepath.Join(outputDir, outputName+".tif")

	// 默认块大小
	if req.BlockWidth <= 0 {
		req.BlockWidth = 512
	}
	if req.BlockHeight <= 0 {
		req.BlockHeight = 512
	}

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "block_calculate",
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeBlockCalculateTask(taskID, req, outputPath)

	return &CalcOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeBlockCalculateTask(taskID string, req *BlockCalculateRequest, outputPath string) {
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

	blockCalc, err := rd.NewBlockCalculator(req.Expression, req.BlockWidth, req.BlockHeight)
	if err != nil {
		finalStatus = 2
		return
	}
	defer blockCalc.Close()

	result, err := blockCalc.CalculateAllBlocks()
	if err != nil {
		finalStatus = 2
		return
	}

	newRD, err := rd.CreateSingleBandDataset(result, Gogeo.BandReal64)
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

// ==================== 遥感指数计算 ====================

// StartNDVITask 启动NDVI计算任务
func (s *RasterService) StartNDVITask(req *IndexCalculateRequest) (*CalcOperationResponse, error) {
	return s.startIndexTask(req, "NDVI", func(calc *Gogeo.BandCalculator) ([]float64, error) {
		return calc.CalculateNDVI(req.NIRBand, req.RedBand)
	})
}

// StartNDWITask 启动NDWI计算任务
func (s *RasterService) StartNDWITask(req *IndexCalculateRequest) (*CalcOperationResponse, error) {
	return s.startIndexTask(req, "NDWI", func(calc *Gogeo.BandCalculator) ([]float64, error) {
		return calc.CalculateNDWI(req.GreenBand, req.NIRBand)
	})
}

// StartEVITask 启动EVI计算任务
func (s *RasterService) StartEVITask(req *IndexCalculateRequest) (*CalcOperationResponse, error) {
	return s.startIndexTask(req, "EVI", func(calc *Gogeo.BandCalculator) ([]float64, error) {
		return calc.CalculateEVI(req.NIRBand, req.RedBand, req.BlueBand)
	})
}

// StartSAVITask 启动SAVI计算任务
func (s *RasterService) StartSAVITask(req *IndexCalculateRequest) (*CalcOperationResponse, error) {
	L := req.LFactor
	if L == 0 {
		L = 0.5 // 默认值
	}
	return s.startIndexTask(req, "SAVI", func(calc *Gogeo.BandCalculator) ([]float64, error) {
		return calc.CalculateSAVI(req.NIRBand, req.RedBand, L)
	})
}

// StartMNDWITask 启动MNDWI计算任务
func (s *RasterService) StartMNDWITask(req *IndexCalculateRequest) (*CalcOperationResponse, error) {
	return s.startIndexTask(req, "MNDWI", func(calc *Gogeo.BandCalculator) ([]float64, error) {
		return calc.CalculateMNDWI(req.GreenBand, req.SWIRBand)
	})
}

// StartNDBITask 启动NDBI计算任务
func (s *RasterService) StartNDBITask(req *IndexCalculateRequest) (*CalcOperationResponse, error) {
	return s.startIndexTask(req, "NDBI", func(calc *Gogeo.BandCalculator) ([]float64, error) {
		return calc.CalculateNDBI(req.SWIRBand, req.NIRBand)
	})
}

// StartNDSITask 启动NDSI计算任务
func (s *RasterService) StartNDSITask(req *IndexCalculateRequest) (*CalcOperationResponse, error) {
	return s.startIndexTask(req, "NDSI", func(calc *Gogeo.BandCalculator) ([]float64, error) {
		return calc.CalculateNDSI(req.GreenBand, req.SWIRBand)
	})
}

// StartLAITask 启动LAI计算任务
func (s *RasterService) StartLAITask(req *IndexCalculateRequest) (*CalcOperationResponse, error) {
	return s.startIndexTask(req, "LAI", func(calc *Gogeo.BandCalculator) ([]float64, error) {
		return calc.CalculateLAI(req.NIRBand, req.RedBand)
	})
}

// startIndexTask 通用遥感指数任务启动器
func (s *RasterService) startIndexTask(req *IndexCalculateRequest, indexType string, calcFunc func(*Gogeo.BandCalculator) ([]float64, error)) (*CalcOperationResponse, error) {
	taskID := uuid.New().String()

	outputDir := filepath.Join(config.MainConfig.Download, taskID)
	if err := createDirIfNotExist(outputDir); err != nil {
		return nil, err
	}
	outputName := req.OutputName
	if outputName == "" {
		outputName = indexType + "_result"
	}
	outputPath := filepath.Join(outputDir, outputName+".tif")

	argsJSON, _ := json.Marshal(req)
	record := &models.RasterRecord{
		TaskID:     taskID,
		SourcePath: req.SourcePath,
		OutputPath: outputPath,
		Status:     0,
		TypeName:   "index_" + indexType,
		Args:       datatypes.JSON(argsJSON),
	}

	if err := models.DB.Create(record).Error; err != nil {
		return nil, fmt.Errorf("创建任务记录失败: %w", err)
	}

	go s.executeIndexTask(taskID, req.SourcePath, outputPath, calcFunc)

	return &CalcOperationResponse{
		TaskID:     taskID,
		OutputPath: outputPath,
		Message:    "任务已提交",
	}, nil
}

func (s *RasterService) executeIndexTask(taskID, sourcePath, outputPath string, calcFunc func(*Gogeo.BandCalculator) ([]float64, error)) {
	var finalStatus int = 1
	defer func() {
		if r := recover(); r != nil {
			finalStatus = 2
		}
		models.DB.Model(&models.RasterRecord{}).Where("task_id = ?", taskID).Update("status", finalStatus)
	}()

	rd, err := Gogeo.OpenRasterDataset(sourcePath, false)
	if err != nil {
		finalStatus = 2
		return
	}
	defer rd.Close()

	calc := rd.NewBandCalculator()
	result, err := calcFunc(calc)
	if err != nil {
		finalStatus = 2
		return
	}

	newRD, err := rd.CreateSingleBandDataset(result, Gogeo.BandReal64)
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
