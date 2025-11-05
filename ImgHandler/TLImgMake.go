package ImgHandler

import (
	"fmt"
	"github.com/GrainArc/SouceMap/views"
)

// TLImgMake 根据表名生成图例图片
// 参数:
//
//	tableName: 数据表名称，用于获取颜色配置
//
// 返回:
//
//	[]byte: 生成的图例图片字节数据
//	error: 错误信息，如果执行成功则为 nil
func TLImgMake(tableName string) ([]byte, error) {
	// 验证输入参数，确保表名不为空
	if tableName == "" {
		return nil, fmt.Errorf("tableName cannot be empty")
	}

	// 从视图层获取指定表名对应的颜色配置信息
	colors := views.GetColor(tableName)

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
	items = append(items, LegendItem{
		Property: "用地范围",     // 属性名称
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

func TLImgMakeByUser(tableName string, Property string) ([]byte, error) {
	// 验证输入参数，确保表名不为空
	if tableName == "" {
		return nil, fmt.Errorf("tableName cannot be empty")
	}

	// 从视图层获取指定表名对应的颜色配置信息
	colors := views.GetColor(tableName)

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
