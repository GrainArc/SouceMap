package ImgHandler

import (
	"fmt"
	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/models"
)

// TLImgMake 根据表名生成图例图片
type CMap struct {
	Property string
	Color    string
}
type ColorData struct {
	LayerName string
	AttName   string
	ColorMap  []CMap
}

func GetColor(LayerName string) []ColorData {
	DB := models.DB

	var searchData []models.AttColor
	DB.Where("layer_name = ?", LayerName).Find(&searchData)

	if len(searchData) == 0 {
		return []ColorData{}
	}

	// 使用 map 按 AttName 分组，同时对 Property 去重
	attColorMap := make(map[string]map[string]CMap) // attName -> property -> CMap

	for _, item := range searchData {
		if attColorMap[item.AttName] == nil {
			attColorMap[item.AttName] = make(map[string]CMap)
		}

		// 使用 Property 作为 key 实现去重
		// 如果相同 Property 已存在，会被覆盖（保留最后一个）
		attColorMap[item.AttName][item.Property] = CMap{
			Property: item.Property,
			Color:    item.Color,
		}
	}

	// 转换为 ColorData 切片
	result := make([]ColorData, 0, len(attColorMap))
	for attName, propertyMap := range attColorMap {
		colorMaps := make([]CMap, 0, len(propertyMap))
		for _, cmap := range propertyMap {
			colorMaps = append(colorMaps, cmap)
		}

		result = append(result, ColorData{
			LayerName: LayerName, // 直接使用参数，因为查询条件已经限定了 LayerName
			AttName:   attName,
			ColorMap:  colorMaps,
		})
	}

	return result
}

// error: 错误信息，如果执行成功则为 nil
func TLImgMake(tableName string) ([]byte, error) {
	// 验证输入参数，确保表名不为空
	if tableName == "" {
		return nil, fmt.Errorf("tableName cannot be empty")
	}

	// 从视图层获取指定表名对应的颜色配置信息
	colors := GetColor(tableName)

	// 检查颜色配置是否为空，避免索引越界导致 panic
	if len(colors) == 0 {
		return nil, fmt.Errorf("no color configuration found for table: %s", tableName)
	}

	// 获取第一个颜色配置的映射关系
	colorMap := colors[0].ColorMap

	// 检查颜色映射是否为空
	if len(colorMap) == 0 {
		return nil, fmt.Errorf("color map is empty for table: %s", tableName)
	}

	// 预分配切片容量，避免动态扩容带来的性能损耗
	items := make([]LegendItem, 0, len(colorMap))

	// 遍历颜色映射，构建图例项列表
	for _, item := range colorMap {
		// 将每个颜色映射项转换为图例项并添加到列表中
		items = append(items, LegendItem{
			Property: item.Property, // 属性名称
			Color:    item.Color,    // 对应的颜色值
			GeoType:  "Polygon",     // 几何类型，默认为多边形
		})
	}

	// 调用图例创建函数，生成图例图片的字节数据
	img, err := CreateLegend(items)
	if err != nil {
		// 如果创建失败，包装错误信息并返回
		return nil, fmt.Errorf("failed to create legend for table %s: %w", tableName, err)
	}

	// 返回成功生成的图片数据
	return img, nil
}

func TLImgMakeByUser(tableName string, Property string) ([]byte, error) {
	// 验证输入参数，确保表名不为空
	if tableName == "" {
		return nil, fmt.Errorf("tableName cannot be empty")
	}

	// 从视图层获取指定表名对应的颜色配置信息
	colors := GetColor(tableName)

	// 检查颜色配置是否为空，避免索引越界导致 panic
	if len(colors) == 0 {
		return nil, fmt.Errorf("no color configuration found for table: %s", tableName)
	}

	// 获取第一个颜色配置的映射关系
	colorMap := colors[0].ColorMap

	// 检查颜色映射是否为空
	if len(colorMap) == 0 {
		return nil, fmt.Errorf("color map is empty for table: %s", tableName)
	}

	// 预分配切片容量，避免动态扩容带来的性能损耗
	items := make([]LegendItem, 0, len(colorMap))

	// 遍历颜色映射，构建图例项列表
	for _, item := range colorMap {
		// 将每个颜色映射项转换为图例项并添加到列表中
		items = append(items, LegendItem{
			Property: Property,   // 属性名称
			Color:    item.Color, // 对应的颜色值
			GeoType:  "Polygon",  // 几何类型，默认为多边形
		})
	}

	// 调用图例创建函数，生成图例图片的字节数据
	img, err := CreateLegend(items)
	if err != nil {
		// 如果创建失败，包装错误信息并返回
		return nil, fmt.Errorf("failed to create legend for table %s: %w", tableName, err)
	}

	// 返回成功生成的图片数据
	return img, nil
}

func TLImgMakeFilter(tableName string, groupedResult []methods.Result) ([]byte, error) {
	// 验证输入参数，确保表名不为空
	if tableName == "" {
		return nil, fmt.Errorf("tableName cannot be empty")
	}

	// 从视图层获取指定表名对应的颜色配置信息
	colors := GetColor(tableName)

	// 检查颜色配置是否为空，避免索引越界导致 panic
	if len(colors) == 0 {
		return nil, fmt.Errorf("no color configuration found for table: %s", tableName)
	}
	var colorMap []CMap
	for _, result := range colors[0].ColorMap {
		if ContainsProperty(groupedResult, result.Property) {
			colorMap = append(colorMap, result)
		}
	}

	// 检查颜色映射是否为空
	if len(colorMap) == 0 {
		return nil, fmt.Errorf("color map is empty for table: %s", tableName)
	}

	// 预分配切片容量，避免动态扩容带来的性能损耗
	items := make([]LegendItem, 0, len(colorMap))
	items = append(items, LegendItem{
		Property: "用地范围",         // 属性名称
		Color:    "RGB(254,0,0)", // 对应的颜色值
		GeoType:  "LineString",   // 几何类型，默认为多边形
	})
	// 遍历颜色映射，构建图例项列表
	for _, item := range colorMap {
		// 将每个颜色映射项转换为图例项并添加到列表中
		items = append(items, LegendItem{
			Property: item.Property, // 属性名称
			Color:    item.Color,    // 对应的颜色值
			GeoType:  "Polygon",     // 几何类型，默认为多边形
		})
	}

	// 调用图例创建函数，生成图例图片的字节数据
	img, err := CreateLegend(items)
	if err != nil {
		// 如果创建失败，包装错误信息并返回
		return nil, fmt.Errorf("failed to create legend for table %s: %w", tableName, err)
	}

	// 返回成功生成的图片数据
	return img, nil
}

// ContainsProperty 判断指定的 property 值是否存在于 CMap 切片中
func ContainsProperty(cmaps []methods.Result, property string) bool {
	for _, cm := range cmaps {
		if cm.Dlmc == property {
			return true
		}
	}
	return false
}
