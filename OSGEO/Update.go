package OSGEO

import (
	"fmt"
	"github.com/GrainArc/Gogeo"
)

// SpatialUpdateAnalysisParallel 执行并行空间更新分析
func SpatialUpdateAnalysisParallel(inputTable, updateTable string, config *Gogeo.ParallelGeosConfig) (*Gogeo.GeosAnalysisResult, error) {
	// 读取输入几何表
	inputReader := Gogeo.MakePGReader(inputTable)
	inputLayer, err := inputReader.ReadGeometryTable()
	if err != nil {
		return nil, fmt.Errorf("读取输入表 %s 失败: %v", inputTable, err)
	}
	defer inputLayer.Close()

	// 读取更新几何表
	updateReader := Gogeo.MakePGReader(updateTable)
	updateLayer, err := updateReader.ReadGeometryTable()
	if err != nil {
		return nil, fmt.Errorf("读取更新表 %s 失败: %v", updateTable, err)
	}
	defer updateLayer.Close()
	resultLayer, err := Gogeo.SpatialUpdateAnalysis(inputLayer, updateLayer, config)
	if err != nil {
		return nil, fmt.Errorf("执行并行裁剪分析失败: %v", err)
	}

	return resultLayer, nil
}
