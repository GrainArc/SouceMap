package Transformer

import (
	"bufio"
	"fmt"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/saintfish/chardet"
	"log"
	"os"
	"strconv"
	"strings"
)

func TxtToGeojson(FilePath string) (*geojson.FeatureCollection, string) {
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

	//获取几何文件
	var Propertie []string
	var currentPlot []string
	var GeoText [][]string
	for _, line := range lines {
		if strings.HasSuffix(line, "@") {
			// 去掉末尾的@并添加到当前地块
			line = strings.TrimSuffix(line, "@")
			Propertie = append(Propertie, line)
			GeoText = append(GeoText, currentPlot) // 保存当前地块
			currentPlot = []string{}               // 重置当前地块
		} else {
			// 添加当前行到当前地块坐标
			currentPlot = append(currentPlot, line)
		}
	}
	// 检查并添加最后一个地块到GeoText
	if len(currentPlot) > 0 {
		GeoText = append(GeoText, currentPlot)
	}
	GeoText = GeoText[1:] //去掉头文件

	for index, item := range GeoText {
		var rings []orb.Ring
		//将数据转换为内外环
		Boundarys := groupBySecondItem(item)

		for _, geos := range Boundarys {
			OuterBoundaryIs := stringToCoords(geos)
			for _, coord := range OuterBoundaryIs {
				x := coord[0]
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
			}
			rings = append(rings, OuterBoundaryIs)
		}

		//构造几何
		geometry := orb.Polygon(rings)
		feature := geojson.NewFeature(geometry)
		//构造属性

		encoding := detectEncoding(FilePath)

		feature.Properties = makeProperties(Propertie[index], encoding)
		featureCollection.Append(feature)
	}

	return featureCollection, isTransform
}

func groupBySecondItem(data []string) [][]string {
	// 创建一个结果的切片，每个分组初始化为一个空切片
	groups := make(map[string][]string)

	// 遍历输入数据
	for _, line := range data {
		// 将当前行按逗号分割
		parts := strings.Split(line, ",")
		if len(parts) > 1 {
			key := parts[1]                         // 获取第二个项作为分组的键
			groups[key] = append(groups[key], line) // 将当前行加入对应的分组
		}
	}

	// 将分组结果转化为[][]string
	var result [][]string
	for _, group := range groups {
		result = append(result, group) // 将每个分组加入结果切片
	}

	return result // 返回分组结果
}
func stringToCoords(Coordinates []string) []orb.Point {
	var coords []orb.Point
	for _, coord := range Coordinates {
		mycoord := strings.Split(coord, ",")
		if len(mycoord) >= 2 {
			x, _ := strconv.ParseFloat(mycoord[3], 64)
			y, _ := strconv.ParseFloat(mycoord[2], 64)
			if x > 0 && y > 0 {
				coords = append(coords, orb.Point{x, y})
			}
		}

	}
	return coords

}

func makeProperties(Propertie string, encoding string) map[string]interface{} {
	if strings.Contains(encoding, "GB") {
		Propertie = GbkToUtf8(Propertie)
	}
	// 将输入字符串按逗号分隔成切片
	mycoord := strings.Split(Propertie, ",")

	// 初始化一个空的map来存储数据
	data := make(map[string]interface{}) // 确保data被初始化

	// 检查切片长度是否符合预期
	if len(mycoord) < 8 { // 如果切片长度少于8，返回空map
		return data // 返回空map，避免越界
	}
	// 将切片中的值赋给map中的相关字段
	data["地块编号"] = mycoord[0] // 地块编号
	data["地块面积"] = mycoord[1] // 地块面积
	data["地块用途"] = mycoord[2] // 地块用途
	data["地类编码"] = mycoord[3] // 地类编码
	data["界址点数"] = mycoord[4] // 界址点数
	data["图幅号"] = mycoord[5]  // 图幅号
	data["图形属性"] = mycoord[6] // 图形属性
	data["生产时间"] = mycoord[7] // 生产时间

	return data // 返回构建好的数据map
}

func detectEncoding(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Println("无法读取文件: %v", err)
	}
	detector := chardet.NewTextDetector()
	result, err := detector.DetectBest(data)
	if err != nil {
		log.Println("编码检测失败: %v", err)
	}
	return result.Charset
}
