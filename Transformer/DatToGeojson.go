package Transformer

import (
	"bufio"
	"fmt"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"os"
	"strconv"
	"strings"
)

func DatToGeojson(FilePath string) (*geojson.FeatureCollection, string) {
	var isTransform string
	featureCollection := geojson.NewFeatureCollection()
	file, err := os.Open(FilePath) // 替换为实际文件路径
	if err != nil {
		fmt.Println("Error opening file:", err)
	}
	var lines []string
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()      // 获取一行文本
		lines = append(lines, line) // 将每行内容添加到切片中
	}
	for _, item := range lines {
		Coord := strings.Split(item, ",")
		if len(Coord) < 4 {
			fmt.Println("Error: not enough coordinates in line:", item)
			continue
		}
		x, _ := strconv.ParseFloat(Coord[2], 64)
		y, _ := strconv.ParseFloat(Coord[3], 64)
		if x >= 100000 && x <= 10000000 {
			isTransform = "4544"
		} else if x <= 1000 {
			isTransform = "4326"
		} else if x >= 33000000 && x <= 34000000 {
			isTransform = "4521"
		} else if x >= 34000000 && x <= 35000000 {
			isTransform = "4522"
		} else if x >= 35000000 && x <= 36000000 {
			isTransform = "4523"
		} else if x >= 36000000 && x <= 37000000 {
			isTransform = "4524"
		}
		attrs := make(map[string]interface{})
		attrs["name"] = GbkToUtf8(Coord[0])
		geometry := orb.Point{x, y}
		feature := geojson.NewFeature(geometry)
		feature.Properties = attrs
		featureCollection.Append(feature)
	}
	return featureCollection, isTransform
}
