package ImgHandler

import (
	"fmt"
	"github.com/GrainArc/SouceMap/views"
)

// 查询图例
func TLImgMake(tableName string) ([]byte, error) {
	colors := views.GetColor(tableName)
	var items []LegendItem
	for _, item := range colors[0].ColorMap {
		items = append(items, LegendItem{
			Property: item.Property,
			Color:    item.Color,
		})
	}
	IMG, err := CreateLegend(items)
	if err != nil {
		fmt.Println(err)
	}
	return IMG, err
}
