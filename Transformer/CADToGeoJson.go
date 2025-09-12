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
	"os"
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

func ConvertDXFToGeoJSON2(dxfFilePath string) (*geojson.FeatureCollection, string) {
	file, err := os.Open(dxfFilePath)
	isTransform := ""
	if err != nil {
		log.Println(err)
	}
	doc, err := document.DxfDocumentFromStream(file)
	if err != nil {
		log.Println(err)
	}
	featureCollection := geojson.NewFeatureCollection()
	for _, entity := range doc.Entities.Entities {
		if polyline, ok := entity.(*entities.Polyline); ok { //线文件
			var coords []orb.Point
			for _, vertex := range polyline.Vertices {
				x := vertex.Location.X
				if x >= 33000000 && x <= 34000000 {
					isTransform = "4521"
				} else if x >= 34000000 && x <= 35000000 {
					isTransform = "4522"
				} else if x >= 35000000 && x <= 36000000 {
					isTransform = "4523"
				} else if x >= 36000000 && x <= 37000000 {
					isTransform = "4524"
				}
				if x >= 33000000 && x <= 37000000 {
					coords = append(coords, orb.Point{vertex.Location.X, vertex.Location.Y})

				}

			}
			if len(coords) >= 2 {
				line := orb.LineString(coords)
				feature := geojson.NewFeature(line)
				feature.Properties["layername"] = GbkToUtf8(polyline.LayerName)
				featureCollection.Append(feature)
			}
		} else if lwpolyline, ok := entity.(*entities.LWPolyline); ok { //面文件

			if lwpolyline.Closed == true {
				var coords []orb.Point
				for _, vertex := range lwpolyline.Points {
					x := vertex.Point.X
					if x >= 33000000 && x <= 34000000 {
						isTransform = "4521"
					} else if x >= 34000000 && x <= 35000000 {
						isTransform = "4522"
					} else if x >= 35000000 && x <= 36000000 {
						isTransform = "4523"
					} else if x >= 36000000 && x <= 37000000 {
						isTransform = "4524"
					}
					if x >= 33000000 && x <= 37000000 {
						coords = append(coords, orb.Point{vertex.Point.X, vertex.Point.Y})
					}

				}
				if len(coords) >= 4 {
					polygon := orb.Polygon{coords}
					feature := geojson.NewFeature(polygon)
					feature.Properties["layername"] = GbkToUtf8(lwpolyline.LayerName)
					featureCollection.Append(feature)
				}

			} else {
				var coords []orb.Point
				for _, vertex := range lwpolyline.Points {
					x := vertex.Point.X
					if x >= 33000000 && x <= 34000000 {
						isTransform = "4521"
					} else if x >= 34000000 && x <= 35000000 {
						isTransform = "4522"
					} else if x >= 35000000 && x <= 36000000 {
						isTransform = "4523"
					} else if x >= 36000000 && x <= 37000000 {
						isTransform = "4524"
					}
					if x >= 33000000 && x <= 37000000 {
						coords = append(coords, orb.Point{vertex.Point.X, vertex.Point.Y})
					}

				}
				if len(coords) >= 2 {
					line := orb.LineString(coords)
					feature := geojson.NewFeature(line)
					feature.Properties["layername"] = GbkToUtf8(lwpolyline.LayerName)
					featureCollection.Append(feature)
				}

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
					if x >= 33000000 && x <= 34000000 {
						isTransform = "4521"
					} else if x >= 34000000 && x <= 35000000 {
						isTransform = "4522"
					} else if x >= 35000000 && x <= 36000000 {
						isTransform = "4523"
					} else if x >= 36000000 && x <= 37000000 {
						isTransform = "4524"
					}
					if x >= 33000000 && x <= 37000000 {
						coords = append(coords, orb.Point{vertex.Location.X, vertex.Location.Y})

					}
				}
				if len(coords) >= 2 {
					line := orb.LineString(coords)
					feature := geojson.NewFeature(line)
					feature.Properties["layername"] = GbkToUtf8(polyline.LayerName)
					featureCollection.Append(feature)
				}

			} else if lwpolyline, ok := entity.(*entities.LWPolyline); ok {
				if lwpolyline.Closed == true {
					var coords []orb.Point
					for _, vertex := range lwpolyline.Points {
						x := vertex.Point.X
						if x >= 33000000 && x <= 34000000 {
							isTransform = "4521"
						} else if x >= 34000000 && x <= 35000000 {
							isTransform = "4522"
						} else if x >= 35000000 && x <= 36000000 {
							isTransform = "4523"
						} else if x >= 36000000 && x <= 37000000 {
							isTransform = "4524"
						}
						if x >= 33000000 && x <= 37000000 {
							coords = append(coords, orb.Point{vertex.Point.X, vertex.Point.Y})

						}
					}
					if len(coords) >= 4 {
						polygon := orb.Polygon{coords}
						feature := geojson.NewFeature(polygon)
						feature.Properties["layername"] = GbkToUtf8(lwpolyline.LayerName)
						featureCollection.Append(feature)
					}

				} else {
					var coords []orb.Point

					for _, vertex := range lwpolyline.Points {
						x := vertex.Point.X
						if x >= 33000000 && x <= 34000000 {
							isTransform = "4521"
						} else if x >= 34000000 && x <= 35000000 {
							isTransform = "4522"
						} else if x >= 35000000 && x <= 36000000 {
							isTransform = "4523"
						} else if x >= 36000000 && x <= 37000000 {
							isTransform = "4524"
						}
						if x >= 33000000 && x <= 37000000 {
							coords = append(coords, orb.Point{vertex.Point.X, vertex.Point.Y})
						}

					}
					if len(coords) >= 2 {
						line := orb.LineString(coords)
						feature := geojson.NewFeature(line)
						feature.Properties["layername"] = GbkToUtf8(lwpolyline.LayerName)
						featureCollection.Append(feature)
					}

				}

			}
		}
	}

	return featureCollection, isTransform
}
