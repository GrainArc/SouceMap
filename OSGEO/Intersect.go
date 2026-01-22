package OSGEO

import (
	"fmt"
	"github.com/GrainArc/Gogeo"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"log"
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

func SpatialIntersectionAnalysisParallelPG(
	db *gorm.DB,
	table1, table2 string,
	strategy Gogeo.FieldMergeStrategy,
	config *Gogeo.ParallelGeosConfig,
) (*Gogeo.GeosAnalysisResult, error) {

	taskid := uuid.New().String()
	// 1. 直接从PG生成瓦片bin文件（优化版本）
	log.Printf("开始从PostgreSQL生成瓦片...")
	err := Gogeo.GenerateTilesFromPG(db, table1, table2, config.TileCount, taskid)
	if err != nil {
		return nil, fmt.Errorf("生成瓦片失败: %v", err)
	}
	// 2. 读取bin文件分组
	log.Printf("读取瓦片分组...")
	GPbins, err := Gogeo.ReadAndGroupBinFiles(taskid)
	if err != nil {
		return nil, fmt.Errorf("读取分组文件失败: %v", err)
	}
	log.Printf("共有 %d 个瓦片组需要处理", len(GPbins))
	// 3. 创建结果图层（需要从第一个bin文件获取schema信息）
	log.Printf("创建结果图层...")
	resultLayer, err := createResultLayerFromBin(GPbins, table1, table2, strategy)
	if err != nil {
		return nil, fmt.Errorf("创建结果图层失败: %v", err)
	}
	// 4. 并发执行分析（精度设置在反序列化后应用）
	log.Printf("开始并发执行空间分析...")
	err = Gogeo.ExecuteConcurrentIntersectionAnalysisOptimized(GPbins, resultLayer, config, strategy)
	if err != nil {
		resultLayer.Close()
		return nil, fmt.Errorf("并发分析失败: %v", err)
	}
	// 5. 清理临时文件
	defer func() {
		err := Gogeo.CleanupTileFiles(taskid)
		if err != nil {
			log.Printf("清理临时文件失败: %v", err)
		}
	}()
	// 6. 计算结果数量
	resultCount := resultLayer.GetFeatureCount()
	log.Printf("空间分析完成，共生成 %d 个要素", resultCount)
	// 7. 如果需要合并瓦片
	if config.IsMergeTile {
		log.Printf("开始合并瓦片...")
		unionResult, err := Gogeo.PerformUnionByFields(resultLayer, config.PrecisionConfig, config.ProgressCallback)
		if err != nil {
			return nil, fmt.Errorf("执行融合操作失败: %v", err)
		}
		// 删除临时标识字段
		err = Gogeo.DeleteFieldFromLayerFuzzy(unionResult.OutputLayer, "gogeo_analysis_id")
		if err != nil {
			log.Printf("警告: 删除临时标识字段失败: %v", err)
		}
		log.Printf("合并完成，最终结果: %d 个要素", unionResult.ResultCount)
		return unionResult, nil
	}
	return &Gogeo.GeosAnalysisResult{
		OutputLayer: resultLayer,
		ResultCount: resultCount,
	}, nil
}

// createResultLayerFromBin 从bin文件创建结果图层
func createResultLayerFromBin(GPbins []Gogeo.GroupTileFiles, table1, table2 string, strategy Gogeo.FieldMergeStrategy) (*Gogeo.GDALLayer, error) {
	// 找到第一个非空的bin文件
	var layer1Path, layer2Path string

	for _, group := range GPbins {
		// 检查文件是否存在且非空
		if Gogeo.IsValidBinFile(group.GPBin.Layer1) {
			layer1Path = group.GPBin.Layer1
		}
		if Gogeo.IsValidBinFile(group.GPBin.Layer2) {
			layer2Path = group.GPBin.Layer2
		}

		if layer1Path != "" && layer2Path != "" {
			break
		}
	}
	if layer1Path == "" || layer2Path == "" {
		return nil, fmt.Errorf("未找到有效的bin文件")
	}
	// 反序列化第一个bin文件以获取schema信息
	tempLayer1, err := Gogeo.DeserializeLayerFromFile(layer1Path)
	if err != nil {
		return nil, fmt.Errorf("反序列化layer1失败: %v", err)
	}
	defer tempLayer1.Close()
	tempLayer2, err := Gogeo.DeserializeLayerFromFile(layer2Path)
	if err != nil {
		return nil, fmt.Errorf("反序列化layer2失败: %v", err)
	}
	defer tempLayer2.Close()
	// 创建结果图层（复用现有函数）
	return Gogeo.CreateIntersectionResultLayer(tempLayer1, tempLayer2, strategy)
}
