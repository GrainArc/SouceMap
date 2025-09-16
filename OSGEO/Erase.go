package OSGEO

import (
	"fmt"
	"github.com/GrainArc/Gogeo"
)

// SpatialEraseAnalysisParallel 执行并行空间擦除分析
func SpatialEraseAnalysisParallel(inputTable, eraseTable string, config *Gogeo.ParallelGeosConfig) (*Gogeo.GeosAnalysisResult, error) {
	// 读取输入图层
	inputReader := Gogeo.MakePGReader(inputTable)
	inputLayer, err := inputReader.ReadGeometryTable()
	if err != nil {
		return nil, fmt.Errorf("读取输入表 %s 失败: %v", inputTable, err)
	}
	defer inputLayer.Close()

	// 读取擦除图层
	eraseReader := Gogeo.MakePGReader(eraseTable)
	eraseLayer, err := eraseReader.ReadGeometryTable()
	if err != nil {
		return nil, fmt.Errorf("读取擦除表 %s 失败: %v", eraseTable, err)
	}
	defer eraseLayer.Close()

	resultLayer, err := Gogeo.SpatialEraseAnalysis(inputLayer, eraseLayer, config)
	if err != nil {
		return nil, fmt.Errorf("执行并行裁剪分析失败: %v", err)
	}

	return resultLayer, nil
}
