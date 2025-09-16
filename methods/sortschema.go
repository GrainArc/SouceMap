package methods

import (
	"github.com/GrainArc/SouceMap/models"
	"sort"
)

func SortSchema(TableSchema []models.MySchema) []models.MySchema {
	typeOrder := map[string]int{
		"point":   0,
		"line":    1,
		"polygon": 2,
	}
	// 按Main字段分组
	grouped := make(map[string][]models.MySchema)
	for _, schema := range TableSchema {
		grouped[schema.Main] = append(grouped[schema.Main], schema)
	}
	// 保留Main字段的原始顺序
	var mainOrder []string
	for _, schema := range TableSchema {
		if len(mainOrder) == 0 || mainOrder[len(mainOrder)-1] != schema.Main {
			mainOrder = append(mainOrder, schema.Main)
		}
	}
	// 排序每个分组内的数据
	for _, group := range grouped {
		sort.Slice(group, func(i, j int) bool {
			return typeOrder[group[i].Type] < typeOrder[group[j].Type]
		})
	}
	// 拼接排序后的结果
	var sortedSchemas []models.MySchema
	for _, mainKey := range mainOrder {
		sortedSchemas = append(sortedSchemas, grouped[mainKey]...)
	}

	return sortedSchemas
}
