package OSGEO

import (
	"fmt"
	"github.com/fmecool/Gogeo"
)

// SpatialClipAnalysisParallel 并行空间裁剪分析
func SpatialClipAnalysisParallel(table1, table2 string, config *Gogeo.ParallelGeosConfig) (*Gogeo.GeosAnalysisResult, error) {
	// 读取两个几何表
	reader1 := Gogeo.MakePGReader(table1)
	layer1, err := reader1.ReadGeometryTable()
	if err != nil {
		return nil, fmt.Errorf("读取输入表 %s 失败: %v", table1, err)
	}
	defer layer1.Close()

	reader2 := Gogeo.MakePGReader(table2)
	layer2, err := reader2.ReadGeometryTable()
	if err != nil {
		return nil, fmt.Errorf("读取裁剪表 %s 失败: %v", table2, err)
	}
	defer layer2.Close()

	// 执行并行裁剪分析
	resultLayer, err := Gogeo.SpatialClipAnalysis(layer1, layer2, config)
	if err != nil {
		return nil, fmt.Errorf("执行并行裁剪分析失败: %v", err)
	}

	return resultLayer, nil
}
