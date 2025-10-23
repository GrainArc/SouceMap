package Transformer

import (
	"bytes"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/rpaloschi/dxf-go/document"
	"github.com/rpaloschi/dxf-go/entities"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
	"log"
	"math"
	"os"
)

const (
	tolerance = 1e-6 // 浮点数比较容差
)

func GbkToUtf8(s string) string {
	gbkDecoder := simplifiedchinese.GBK.NewDecoder()
	utf8String, _, err := transform.String(gbkDecoder, s)
	if err != nil {
		// 如果解码失败，打印错误，然后返回原始字符串
		return s
	}
	return utf8String
}

func Utf8ToGbk(input string) []byte {
	// 创建GBK编码器
	gbkEncoder := simplifiedchinese.GBK.NewEncoder()   // 创建新的GBK编码器
	var output bytes.Buffer                            // 创建字节缓冲区用于存储转换后的结果
	writer := transform.NewWriter(&output, gbkEncoder) // 使用编码器创建新的写入器

	if _, err := writer.Write([]byte(input)); err != nil { // 尝试写入转换内容
		return nil // 如果发生错误，返回nil和错误信息
	}
	if err := writer.Close(); err != nil { // 关闭写入器
		return nil // 如果关闭时发生错误，返回nil和错误信息
	}

	return output.Bytes() // 返回转换后的GBK字节和nil错误
}

// 判断两个点是否相等（考虑浮点数误差）
func pointsEqual(p1, p2 orb.Point) bool {
	return math.Abs(p1[0]-p2[0]) < tolerance && math.Abs(p1[1]-p2[1]) < tolerance
}

// 判断线是否闭合（首尾节点相等）
func isClosedLine(coords []orb.Point) bool {
	if len(coords) < 2 {
		return false
	}
	return pointsEqual(coords[0], coords[len(coords)-1])
}

// 根据坐标判断投影带
func updateTransform(x float64, isTransform *string) {
	if *isTransform != "" {
		return // 已经设置过了
	}
	if x >= 33000000 && x < 34000000 {
		*isTransform = "4521"
	} else if x >= 34000000 && x < 35000000 {
		*isTransform = "4522"
	} else if x >= 35000000 && x < 36000000 {
		*isTransform = "4523"
	} else if x >= 36000000 && x < 37000000 {
		*isTransform = "4524"
	}
}

// 创建几何要素（自动判断是线还是面）
func createFeature(coords []orb.Point, layerName string, forceClosed bool) *geojson.Feature {
	if len(coords) < 2 {
		return nil
	}

	// 判断是否应该创建面：1. 强制闭合 或 2. 首尾节点相等
	shouldBePolygon := forceClosed || isClosedLine(coords)

	if shouldBePolygon {
		// 确保闭合（如果最后一点不等于第一点，添加第一点）
		closedCoords := coords
		if !pointsEqual(coords[0], coords[len(coords)-1]) {
			closedCoords = append([]orb.Point{}, coords...)
			closedCoords = append(closedCoords, coords[0])
		}

		// 面至少需要4个点（包括闭合点）
		if len(closedCoords) >= 4 {
			polygon := orb.Polygon{closedCoords}
			feature := geojson.NewFeature(polygon)
			feature.Properties["layername"] = GbkToUtf8(layerName)
			return feature
		}
	}

	// 创建线
	line := orb.LineString(coords)
	feature := geojson.NewFeature(line)
	feature.Properties["layername"] = GbkToUtf8(layerName)
	return feature
}

func ConvertDXFToGeoJSON2(dxfFilePath string) (*geojson.FeatureCollection, string) {
	file, err := os.Open(dxfFilePath)
	isTransform := ""
	if err != nil {
		log.Println(err)
	}
	defer file.Close()

	doc, err := document.DxfDocumentFromStream(file)
	if err != nil {
		log.Println(err)
	}
	featureCollection := geojson.NewFeatureCollection()

	// 处理实体
	for _, entity := range doc.Entities.Entities {
		if polyline, ok := entity.(*entities.Polyline); ok { //线文件
			var coords []orb.Point
			for _, vertex := range polyline.Vertices {
				x := vertex.Location.X
				updateTransform(x, &isTransform)

				if x >= 33000000 && x <= 37000000 {
					coords = append(coords, orb.Point{vertex.Location.X, vertex.Location.Y})
				}
			}

			// 使用新的创建函数，自动判断线/面
			if feature := createFeature(coords, polyline.LayerName, false); feature != nil {
				featureCollection.Append(feature)
			}

		} else if lwpolyline, ok := entity.(*entities.LWPolyline); ok { //面文件
			var coords []orb.Point
			for _, vertex := range lwpolyline.Points {
				x := vertex.Point.X
				updateTransform(x, &isTransform)

				if x >= 33000000 && x <= 37000000 {
					coords = append(coords, orb.Point{vertex.Point.X, vertex.Point.Y})
				}
			}

			// 使用新的创建函数，传入Closed标志
			if feature := createFeature(coords, lwpolyline.LayerName, lwpolyline.Closed); feature != nil {
				featureCollection.Append(feature)
			}
		}
	}

	//如果是块文件
	for _, block := range doc.Blocks {
		for _, entity := range block.Entities {
			if polyline, ok := entity.(*entities.Polyline); ok {
				var coords []orb.Point
				for _, vertex := range polyline.Vertices {
					x := vertex.Location.X
					updateTransform(x, &isTransform)

					if x >= 33000000 && x <= 37000000 {
						coords = append(coords, orb.Point{vertex.Location.X, vertex.Location.Y})
					}
				}

				// 使用新的创建函数，自动判断线/面
				if feature := createFeature(coords, polyline.LayerName, false); feature != nil {
					featureCollection.Append(feature)
				}

			} else if lwpolyline, ok := entity.(*entities.LWPolyline); ok {
				var coords []orb.Point
				for _, vertex := range lwpolyline.Points {
					x := vertex.Point.X
					updateTransform(x, &isTransform)

					if x >= 33000000 && x <= 37000000 {
						coords = append(coords, orb.Point{vertex.Point.X, vertex.Point.Y})
					}
				}

				// 使用新的创建函数，传入Closed标志
				if feature := createFeature(coords, lwpolyline.LayerName, lwpolyline.Closed); feature != nil {
					featureCollection.Append(feature)
				}
			}
		}
	}

	return featureCollection, isTransform
}
