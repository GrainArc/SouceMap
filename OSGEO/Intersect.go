package OSGEO

import (
	"fmt"
	"github.com/fmecool/Gogeo"
)

// SpatialIntersectionAnalysisParallel 并行空间相交分析
func SpatialIntersectionAnalysisParallel(table1, table2 string, strategy Gogeo.FieldMergeStrategy, config *Gogeo.ParallelGeosConfig) (*Gogeo.GeosAnalysisResult, error) {
	// 读取两个几何表
	reader1 := Gogeo.MakePGReader(table1)
	layer1, err := reader1.ReadGeometryTable()
	if err != nil {
		return nil, fmt.Errorf("读取表 %s 失败: %v", table1, err)
	}
	defer layer1.Close()

	reader2 := Gogeo.MakePGReader(table2)
	layer2, err := reader2.ReadGeometryTable()
	if err != nil {
		return nil, fmt.Errorf("读取表 %s 失败: %v", table2, err)
	}
	defer layer2.Close()

	// 执行并行相交分析
	resultLayer, err := Gogeo.SpatialIntersectionAnalysis(layer1, layer2, config, strategy)
	if err != nil {
		return nil, fmt.Errorf("执行并行裁剪分析失败: %v", err)
	}

	return resultLayer, nil
}
