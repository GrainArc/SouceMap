package OSGEO

import (
	"fmt"
	"github.com/fmecool/Gogeo"
)

// SpatialIdentityAnalysisParallel 执行并行空间Identity分析
func SpatialIdentityAnalysisParallel(inputTable, methodTable string, config *Gogeo.ParallelGeosConfig) (*Gogeo.GeosAnalysisResult, error) {
	// 读取输入几何表
	inputReader := Gogeo.MakePGReader(inputTable)
	inputLayer, err := inputReader.ReadGeometryTable()
	if err != nil {
		return nil, fmt.Errorf("读取输入表 %s 失败: %v", inputTable, err)
	}
	defer inputLayer.Close()

	// 读取方法几何表
	methodReader := Gogeo.MakePGReader(methodTable)
	methodLayer, err := methodReader.ReadGeometryTable()
	if err != nil {
		return nil, fmt.Errorf("读取方法表 %s 失败: %v", methodTable, err)
	}
	defer methodLayer.Close()
	resultLayer, err := Gogeo.SpatialIdentityAnalysis(inputLayer, methodLayer, config)
	if err != nil {
		return nil, fmt.Errorf("执行并行裁剪分析失败: %v", err)
	}

	return resultLayer, nil

}
