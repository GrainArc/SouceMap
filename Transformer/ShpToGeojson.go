package Transformer

import (
	"fmt"
	"gitee.com/LJ_COOL/go-shp"
	"github.com/GrainArc/SouceMap/models"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

func TrimTrailingZeros(input string) string {
	// 使用正则表达式匹配纯数字字符串（整数或小数）
	numericRegex := regexp.MustCompile(`^\d+(\.\d+)?$`)

	if !numericRegex.MatchString(input) {
		return input
	}

	// 如果输入包含小数点，处理小数部分
	if strings.Contains(input, ".") {
		parts := strings.Split(input, ".")
		intPart := parts[0]
		fracPart := parts[1]

		// 去掉尾部多余的零
		fracPart = strings.TrimRight(fracPart, "0")

		// 如果小数部分被去光了，则只返回整数部分
		if len(fracPart) == 0 {
			return intPart
		} else if len(fracPart) >= 5 {
			fracPart = fracPart[:5]
		}

		return intPart + "." + fracPart
	}

	return input
}

func trimTrailingZeros(input string) string {
	// 使用正则表达式匹配纯数字字符串（整数或小数）
	numericRegex := regexp.MustCompile(`^\d+(\.\d+)?$`)

	if !numericRegex.MatchString(input) {
		return input
	}

	// 如果输入包含小数点，处理小数部分
	if strings.Contains(input, ".") {
		parts := strings.Split(input, ".")
		intPart := parts[0]
		fracPart := parts[1]

		// 去掉尾部多余的零
		fracPart = strings.TrimRight(fracPart, "0")

		// 如果小数部分被去光了，则只返回整数部分
		if len(fracPart) == 0 {
			return intPart
		} else if len(fracPart) >= 5 {
			fracPart = fracPart[:5]
		}

		return intPart + "." + fracPart
	}

	return input
}

func SplitPoints(points []shp.Point, parts []int32) [][]shp.Point {
	var polygons [][]shp.Point
	for i, partIndex := range parts {
		start := partIndex
		var end int32
		if i < len(parts)-1 {
			end = parts[i+1]
		} else {
			end = int32(len(points))
		}
		polygon := points[start:end]
		polygons = append(polygons, polygon)
	}
	return polygons
}
func IsClockwise(points []orb.Point) bool {
	sum := 0.0
	for i := 0; i < len(points)-1; i++ {
		p1 := points[i]
		p2 := points[i+1]
		sum += (p2[0] - p1[0]) * (p2[1] + p1[1])
	}
	// If sum is positive, points are in clockwise order.
	return sum > 0
}

// splitPartsByDounts 根据dounts切片中的true和false分割parts切片。
func splitParts(parts []int32, dounts []bool) [][]int32 {
	var result [][]int32
	var currentGroup []int32
	groupStarted := false
	for i, part := range parts {
		if dounts[i] {
			if groupStarted {
				// End the current group and start a new one
				result = append(result, currentGroup)
				currentGroup = []int32{part}
			} else {
				// Start a new group
				currentGroup = []int32{part}
				groupStarted = true
			}
		} else {
			if groupStarted {
				// Continue the current group
				currentGroup = append(currentGroup, part)
			}
		}
	}
	// Append the last group if it exists
	if len(currentGroup) > 0 {
		result = append(result, currentGroup)
	}
	return result
}

func createIndexSlice(n int32) []int32 {
	indexSlice := make([]int32, 0, n) //  创建长度为0，容量为n的切片
	for i := int32(0); i < n; i++ {   //  循环变量i声明为int32类型以匹配n
		indexSlice = append(indexSlice, i) //  使用append函数填充切片
	}
	return indexSlice
}

func ConvertSHPToGeoJSON2(shpfileFilePath string) (*geojson.FeatureCollection, string) {
	shape, err := shp.Open(shpfileFilePath)
	var isTransform string
	if err != nil {
		log.Println(err)
	}
	defer shape.Close()
	featureCollection := geojson.NewFeatureCollection()
	// fields from the attribute table (DBF)
	fields := shape.Fields()

	dir := filepath.Dir(shpfileFilePath)
	// 获取文件的基础名称部分（不含路径）
	base := filepath.Base(shpfileFilePath)
	// 将文件名的后缀改为 cpg
	newBase := strings.TrimSuffix(base, filepath.Ext(base)) + ".cpg"
	// 拼接新的文件路径
	newPath := filepath.Join(dir, newBase)
	// loop through all features in the shapefile
	CPGg, err := os.ReadFile(newPath)
	var CPG string
	if err != nil {
		CPG = "GBK"
	} else {
		CPG = string(CPGg)
	}

	for shape.Next() {
		n, p := shape.Shape()
		var geometry orb.Geometry
		//fmt.Println(reflect.TypeOf(p))
		switch s := p.(type) {
		case *shp.Point:
			x := s.X
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
			geometry = orb.Point{s.X, s.Y}
			attrs := make(map[string]interface{})
			for k, f := range fields {
				if CPG == "GBK" {
					att := GbkToUtf8(f.String())
					realatt := GbkToUtf8(shape.ReadAttribute(n, k))
					attrs[att] = trimTrailingZeros(realatt)
				} else {
					realatt := shape.ReadAttribute(n, k)
					attrs[f.String()] = trimTrailingZeros(realatt)

				}

			}
			// Create and append the Feature
			feature := geojson.NewFeature(geometry)
			feature.Properties = attrs
			featureCollection.Append(feature)
		case *shp.PointZ:
			x := s.X
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
			geometry = orb.Point{s.X, s.Y}
			attrs := make(map[string]interface{})
			for k, f := range fields {
				if CPG == "GBK" {
					att := GbkToUtf8(f.String())
					realatt := GbkToUtf8(shape.ReadAttribute(n, k))
					attrs[att] = trimTrailingZeros(realatt)
				} else {
					realatt := shape.ReadAttribute(n, k)
					attrs[f.String()] = trimTrailingZeros(realatt)
				}
			}
			// Create and append the Feature
			feature := geojson.NewFeature(geometry)
			feature.Properties = attrs
			featureCollection.Append(feature)
		case *shp.PolyLine:
			coords := make([]orb.Point, len(s.Points))
			for i, vertex := range s.Points {
				x := vertex.X
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
				coords[i] = orb.Point{vertex.X, vertex.Y}
			}
			line := orb.LineString(coords)
			attrs := make(map[string]interface{})
			for k, f := range fields {
				if CPG == "GBK" {
					att := GbkToUtf8(f.String())
					realatt := GbkToUtf8(shape.ReadAttribute(n, k))
					attrs[att] = trimTrailingZeros(realatt)
				} else {
					realatt := shape.ReadAttribute(n, k)
					attrs[f.String()] = trimTrailingZeros(realatt)
				}
			}
			feature := geojson.NewFeature(line)
			feature.Properties = attrs
			featureCollection.Append(feature)
		case *shp.PolyLineZ:
			coords := make([]orb.Point, len(s.Points))
			for i, vertex := range s.Points {
				x := vertex.X
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
				coords[i] = orb.Point{vertex.X, vertex.Y}
			}
			line := orb.LineString(coords)
			attrs := make(map[string]interface{})
			for k, f := range fields {
				if CPG == "GBK" {
					att := GbkToUtf8(f.String())
					realatt := GbkToUtf8(shape.ReadAttribute(n, k))
					attrs[att] = trimTrailingZeros(realatt)
				} else {
					realatt := shape.ReadAttribute(n, k)
					attrs[f.String()] = trimTrailingZeros(realatt)
				}
			}
			feature := geojson.NewFeature(line)
			feature.Properties = attrs
			featureCollection.Append(feature)
		case *shp.Polygon:
			var MultiPolygons orb.MultiPolygon
			polygons := SplitPoints(s.Points, s.Parts)
			dounts := []bool{}
			for _, part := range polygons {
				var points []orb.Point
				for _, point := range part {
					points = append(points, orb.Point{point.X, point.Y}) //获取所有线

				}
				dount := IsClockwise(points) //判断内外环顺序
				dounts = append(dounts, dount)

			}
			polygons_index := createIndexSlice(int32(len(polygons)))
			newparts := splitParts(polygons_index, dounts)
			attrs := make(map[string]interface{})
			for _, item := range newparts {
				var rings []orb.Ring
				for _, i := range item {
					coords := make([]orb.Point, len(polygons[i]))
					for i, vertex := range polygons[i] {
						//坐标转换
						x := vertex.X
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
						coords[i] = orb.Point{vertex.X, vertex.Y}
					}
					rings = append(rings, orb.Ring(coords))

				}
				Polygon := orb.Polygon(rings)
				MultiPolygons = append(MultiPolygons, Polygon)
				// Create attributes (properties) map for the Feature

			}

			for k, f := range fields {
				if CPG == "GBK" {
					att := GbkToUtf8(f.String())
					realatt := GbkToUtf8(shape.ReadAttribute(n, k))
					attrs[att] = trimTrailingZeros(realatt)
				} else {
					realatt := shape.ReadAttribute(n, k)
					attrs[f.String()] = trimTrailingZeros(realatt)

				}
			}
			// Create and append the Feature
			feature := geojson.NewFeature(MultiPolygons)
			if len(fields) == 0 {
				attrs["attribute"] = "null"
			}
			feature.Properties = attrs

			featureCollection.Append(feature)
		case *shp.PolygonZ:
			var MultiPolygons orb.MultiPolygon
			polygons := SplitPoints(s.Points, s.Parts)
			dounts := []bool{}
			for _, part := range polygons {
				var points []orb.Point
				for _, point := range part {
					points = append(points, orb.Point{point.X, point.Y}) //获取所有线

				}
				dount := IsClockwise(points) //判断内外环顺序
				dounts = append(dounts, dount)

			}
			polygons_index := createIndexSlice(int32(len(polygons)))
			newparts := splitParts(polygons_index, dounts)
			attrs := make(map[string]interface{})
			for _, item := range newparts {
				var rings []orb.Ring
				for _, i := range item {
					coords := make([]orb.Point, len(polygons[i]))
					for i, vertex := range polygons[i] {
						//坐标转换
						x := vertex.X
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
						coords[i] = orb.Point{vertex.X, vertex.Y}
					}
					rings = append(rings, orb.Ring(coords))

				}
				Polygon := orb.Polygon(rings)
				MultiPolygons = append(MultiPolygons, Polygon)
				// Create attributes (properties) map for the Feature

			}

			for k, f := range fields {
				if CPG == "GBK" {
					att := GbkToUtf8(f.String())
					realatt := GbkToUtf8(shape.ReadAttribute(n, k))
					attrs[att] = trimTrailingZeros(realatt)
				} else {
					realatt := shape.ReadAttribute(n, k)
					attrs[f.String()] = trimTrailingZeros(realatt)
				}
			}
			// Create and append the Feature
			feature := geojson.NewFeature(MultiPolygons)
			if len(fields) == 0 {
				attrs["attribute"] = "null"
			}
			feature.Properties = attrs

			featureCollection.Append(feature)
		}

	}
	return featureCollection, isTransform
}
func createCpgFile(filename string) error {
	// 创建一个.cpg文件
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("无法创建文件: %v", err)
	}
	defer file.Close()

	// 写入内容"GBK"
	_, err = file.WriteString("GBK")
	if err != nil {
		return fmt.Errorf("写入文件失败: %v", err)
	}

	return nil
}
func ConvertGeoJSONToSHP(GeoData *geojson.FeatureCollection, shpfileFilePath string) {
	fileName := filepath.Base(shpfileFilePath)
	rootName := fileName[0 : len(fileName)-len(filepath.Ext(fileName))]
	shpfileFilePath_point := filepath.Join(filepath.Dir(shpfileFilePath), rootName) + "_点.shp"
	shpfileFilePath_polygon := filepath.Join(filepath.Dir(shpfileFilePath), rootName) + "_面.shp"
	shpfileFilePath_line := filepath.Join(filepath.Dir(shpfileFilePath), rootName) + "_线.shp"
	shpFile_polygon, _ := shp.Create(shpfileFilePath_polygon, shp.POLYGON)
	shpFile_line, _ := shp.Create(shpfileFilePath_line, shp.POLYLINE)
	shpFile_point, _ := shp.Create(shpfileFilePath_point, shp.POINT)
	createCpgFile(filepath.Join(filepath.Dir(shpfileFilePath), rootName) + "_点.cpg")
	createCpgFile(filepath.Join(filepath.Dir(shpfileFilePath), rootName) + "_面.cpg")
	createCpgFile(filepath.Join(filepath.Dir(shpfileFilePath), rootName) + "_线.cpg")
	var fields []shp.Field

	Properties := GeoData.Features[0].Properties
	// 添加字段
	var FieldMAP = make(map[string]int)
	i := 0
	for key, _ := range Properties {
		var field shp.Field
		field = shp.StringField(Utf8ToGbk(key), 120)
		fields = append(fields, field)
		FieldMAP[key] = i
		i += 1
	}

	shpFile_polygon.SetFields(fields)
	shpFile_line.SetFields(fields)
	shpFile_point.SetFields(fields)
	defer shpFile_polygon.Close()
	defer shpFile_line.Close()
	defer shpFile_point.Close()
	pn := 0
	ln := 0
	pon := 0
	mpon := 0
	for _, feature := range GeoData.Features {
		if feature.Geometry != nil {
			//// 写入 SHP 形状以及属性
			switch geom := feature.Geometry.(type) {
			case orb.Polygon:
				var PL [][]shp.Point
				for _, ring := range geom {
					var points []shp.Point
					for _, pt := range ring {
						points = append(points, shp.Point{pt[0], pt[1]})
					}
					PL = append(PL, points)
				}

				NEWPL := shp.NewPolyLine(PL)
				shpFile_polygon.Write(NEWPL)
				for key, item := range feature.Properties {
					var itemStr string

					// 检查 item 的类型并进行相应的转换
					switch v := item.(type) {
					case string:
						itemStr = v // 如果是字符串，直接赋值
					case float64:
						itemStr = fmt.Sprintf("%f", v) // 如果是 float64，转换为字符串
					case int:
						itemStr = fmt.Sprintf("%d", v) // 如果是 int，转换为字符串
					case nil:
						itemStr = "" // 如果是 nil，转换为空字符串
					default:
						itemStr = fmt.Sprintf("%v", v) // 其他类型，使用默认格式化
					}
					// 写入属性
					error := shpFile_polygon.WriteAttribute(pn, FieldMAP[key], Utf8ToGbk(itemStr))
					if error != nil {
						fmt.Println(error.Error()) // 打印错误信息
					}
				}

				pn += 1
			case orb.MultiPolygon:
				for index, _ := range geom {
					var PL [][]shp.Point
					for _, ring := range geom[index] {
						var points []shp.Point
						for _, pt := range ring {
							points = append(points, shp.Point{pt[0], pt[1]})
						}
						PL = append(PL, points)
					}
					NEWPL := shp.NewPolyLine(PL)
					shpFile_polygon.Write(NEWPL)
					for key, item := range feature.Properties {
						var itemStr string

						// 检查 item 的类型并进行相应的转换
						switch v := item.(type) {
						case string:
							itemStr = v // 如果是字符串，直接赋值
						case float64:
							itemStr = fmt.Sprintf("%f", v) // 如果是 float64，转换为字符串
						case int:
							itemStr = fmt.Sprintf("%d", v) // 如果是 int，转换为字符串
						case nil:
							itemStr = "" // 如果是 nil，转换为空字符串
						default:
							itemStr = fmt.Sprintf("%v", v) // 其他类型，使用默认格式化
						}
						// 写入属性
						error := shpFile_polygon.WriteAttribute(pn, FieldMAP[key], Utf8ToGbk(itemStr))
						if error != nil {
							fmt.Println(error.Error()) // 打印错误信息
						}
					}
					pn += 1

				}

			case orb.LineString:
				ring := geom
				var PL [][]shp.Point
				var points []shp.Point
				for _, pt := range ring {
					points = append(points, shp.Point{pt[0], pt[1]})
				}
				PL = append(PL, points)
				NEWPL := shp.NewPolyLine(PL)
				shpFile_line.Write(NEWPL)
				for key, item := range feature.Properties {
					error := shpFile_line.WriteAttribute(ln, FieldMAP[key], Utf8ToGbk(item.(string)))
					if error != nil {
						fmt.Println(error.Error())
					}
				}
				ln += 1
			case orb.Point:
				pt := geom
				var NewPT shp.Point
				NewPT.X = pt[0]
				NewPT.Y = pt[1]
				shpFile_point.Write(&NewPT)
				for key, item := range feature.Properties {
					error := shpFile_point.WriteAttribute(pon, FieldMAP[key], Utf8ToGbk(item.(string)))
					if error != nil {
						fmt.Println(error.Error())
					}
				}
				pon += 1
			case orb.MultiPoint:
				pt := geom[0]
				var NewPT shp.Point
				NewPT.X = pt[0]
				NewPT.Y = pt[1]
				shpFile_point.Write(&NewPT)
				for key, item := range feature.Properties {
					error := shpFile_point.WriteAttribute(mpon, FieldMAP[key], Utf8ToGbk(item.(string)))
					if error != nil {
						fmt.Println(error.Error())
					}
				}
				mpon += 1
			}

		}
	}
	// 确保所有文件都已关闭
	shpFile_polygon.Close()
	shpFile_line.Close()
	shpFile_point.Close()

	// 检查并删除空的shapefile及其关联文件
	checkAndDeleteEmptyShapefile(shpfileFilePath_point, rootName+"_点")
	checkAndDeleteEmptyShapefile(shpfileFilePath_polygon, rootName+"_面")
	checkAndDeleteEmptyShapefile(shpfileFilePath_line, rootName+"_线")
}

func checkAndDeleteEmptyShapefile(shpFilePath, baseName string) {
	// 获取.shp文件信息
	fileInfo, err := os.Stat(shpFilePath)
	if err != nil {
		fmt.Printf("无法获取文件信息: %s, 错误: %v\n", shpFilePath, err)
		return
	}

	// 如果.shp文件大小小于等于110字节，删除相关文件
	if fileInfo.Size() <= 110 {

		// 获取文件目录和基础名称
		dir := filepath.Dir(shpFilePath)

		// 定义要删除的文件扩展名
		extensions := []string{".shp", ".dbf", ".shx", ".cpg", ".prj"}

		// 删除所有相关文件
		for _, ext := range extensions {
			filePath := filepath.Join(dir, baseName+ext)
			if err := os.Remove(filePath); err != nil {
				// 如果文件不存在，不报错
				if !os.IsNotExist(err) {

				}
			} else {

			}
		}
	}
}

// 方位角生成
type Point struct {
	X, Y float64
}

// RotatePoint rotates point A around point B by the given angle in degrees.
func RotatePoint(A, B Point, angle float64) Point {
	// Convert angle from degrees to radians
	radians := angle * (math.Pi / 180)

	// Translate point A to origin relative to B
	xA := A.X - B.X
	yA := A.Y - B.Y

	// Apply rotation matrix
	xC := xA*math.Cos(radians) + yA*math.Sin(radians)
	yC := -xA*math.Sin(radians) + yA*math.Cos(radians)

	// Translate back to original position
	xC += B.X
	yC += B.Y

	return Point{X: xC, Y: yC}
}

func ConvertPointToArrow(pics []models.GeoPic, shpfileFilePath string) {
	fileName := filepath.Base(shpfileFilePath)
	rootName := fileName[0 : len(fileName)-len(filepath.Ext(fileName))]
	shpfileFilePath_line := filepath.Join(filepath.Dir(shpfileFilePath), rootName) + "_线.shp"
	shpFile_line, _ := shp.Create(shpfileFilePath_line, shp.POLYLINE)
	defer shpFile_line.Close()
	fields := []shp.Field{
		shp.StringField(Utf8ToGbk("picbsm"), 122),
		shp.StringField(Utf8ToGbk("角度"), 122),
		shp.StringField(Utf8ToGbk("x"), 55),
		shp.StringField(Utf8ToGbk("y"), 55),
		shp.StringField(Utf8ToGbk("tbid"), 55),
	}

	shpFile_line.SetFields(fields)
	for pn, item := range pics {
		var PL [][]shp.Point
		var points []shp.Point
		x, _ := strconv.ParseFloat(item.X, 64)
		y, _ := strconv.ParseFloat(item.Y, 64)
		angel, _ := strconv.ParseFloat(item.Angel, 64)
		p0 := Point{x, y}
		p1 := Point{x, y + 0.0001}
		p2 := RotatePoint(p1, p0, angel)
		points = append(points, shp.Point{x, y})
		points = append(points, shp.Point{p2.X, p2.Y})
		PL = append(PL, points)
		NEWPL := shp.NewPolyLine(PL)

		shpFile_line.Write(NEWPL)
		shpFile_line.WriteAttribute(pn, 0, Utf8ToGbk(item.Pic_bsm))
		shpFile_line.WriteAttribute(pn, 1, Utf8ToGbk(item.Angel))
		shpFile_line.WriteAttribute(pn, 2, Utf8ToGbk(item.X))
		shpFile_line.WriteAttribute(pn, 3, Utf8ToGbk(item.Y))
		shpFile_line.WriteAttribute(pn, 4, Utf8ToGbk(item.TBID))
	}
}

// 高性能写出
type ShapeData struct {
	Shape      shp.Shape
	Attributes map[string][]byte
	GeomType   string // "point", "line", "polygon"
}

func ConvertGeoJSONToSHP3(GeoData *geojson.FeatureCollection, shpfileFilePath string) {
	fileName := filepath.Base(shpfileFilePath)
	rootName := fileName[0 : len(fileName)-len(filepath.Ext(fileName))]
	shpfileFilePath_point := filepath.Join(filepath.Dir(shpfileFilePath), rootName) + "_点.shp"
	shpfileFilePath_polygon := filepath.Join(filepath.Dir(shpfileFilePath), rootName) + "_面.shp"
	shpfileFilePath_line := filepath.Join(filepath.Dir(shpfileFilePath), rootName) + "_线.shp"

	// 创建SHP文件
	shpFile_polygon, _ := shp.Create(shpfileFilePath_polygon, shp.POLYGON)
	shpFile_line, _ := shp.Create(shpfileFilePath_line, shp.POLYLINE)
	shpFile_point, _ := shp.Create(shpfileFilePath_point, shp.POINT)

	createCpgFile(filepath.Join(filepath.Dir(shpfileFilePath), rootName) + "_点.cpg")
	createCpgFile(filepath.Join(filepath.Dir(shpfileFilePath), rootName) + "_面.cpg")
	createCpgFile(filepath.Join(filepath.Dir(shpfileFilePath), rootName) + "_线.cpg")

	var fields []shp.Field
	Properties := GeoData.Features[0].Properties

	// 添加字段
	var FieldMAP = make(map[string]int)
	i := 0
	for key, _ := range Properties {
		var field shp.Field
		field = shp.StringField(Utf8ToGbk(key), 120)
		fields = append(fields, field)
		FieldMAP[key] = i
		i += 1
	}

	shpFile_polygon.SetFields(fields)
	shpFile_line.SetFields(fields)
	shpFile_point.SetFields(fields)

	defer shpFile_polygon.Close()
	defer shpFile_line.Close()
	defer shpFile_point.Close()

	// 并发处理数据
	const concurrency = 10
	featureCount := len(GeoData.Features)

	// 创建通道和等待组
	featureChan := make(chan *geojson.Feature, featureCount)
	resultChan := make(chan []ShapeData, concurrency)
	var wg sync.WaitGroup

	// 启动工作协程
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var localResults []ShapeData

			for feature := range featureChan {
				if feature.Geometry != nil {
					shapes := processFeature(feature)
					localResults = append(localResults, shapes...)
				}
			}

			if len(localResults) > 0 {
				resultChan <- localResults
			}
		}()
	}

	// 发送任务到通道
	go func() {
		for _, feature := range GeoData.Features {
			featureChan <- feature
		}
		close(featureChan)
	}()

	// 等待所有工作协程完成
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 收集所有结果
	var pointShapes []ShapeData
	var lineShapes []ShapeData
	var polygonShapes []ShapeData

	for results := range resultChan {
		for _, shape := range results {
			switch shape.GeomType {
			case "point":
				pointShapes = append(pointShapes, shape)
			case "line":
				lineShapes = append(lineShapes, shape)
			case "polygon":
				polygonShapes = append(polygonShapes, shape)
			}
		}
	}

	// 批量写入数据
	writeShapesToFile(shpFile_point, pointShapes, FieldMAP)
	writeShapesToFile(shpFile_line, lineShapes, FieldMAP)
	writeShapesToFile(shpFile_polygon, polygonShapes, FieldMAP)

	// 确保所有文件都已关闭
	shpFile_polygon.Close()
	shpFile_line.Close()
	shpFile_point.Close()

	genPrj(shpfileFilePath_point, rootName+"_点")
	genPrj(shpfileFilePath_polygon, rootName+"_面")
	genPrj(shpfileFilePath_line, rootName+"_线")
	// 检查并删除空的shapefile及其关联文件
	checkAndDeleteEmptyShapefile(shpfileFilePath_point, rootName+"_点")
	checkAndDeleteEmptyShapefile(shpfileFilePath_polygon, rootName+"_面")
	checkAndDeleteEmptyShapefile(shpfileFilePath_line, rootName+"_线")
}

func genPrj(shpfileFilePath_point string, rootName string) {
	// 创建PRJ文件
	dir := filepath.Dir(shpfileFilePath_point)
	prjPath := filepath.Join(dir, rootName+".prj")
	prjContent := `GEOGCS["GCS_China_Geodetic_Coordinate_System_2000",DATUM["D_China_2000",SPHEROID["CGCS2000",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]`

	if err := os.WriteFile(prjPath, []byte(prjContent), 0644); err != nil {
		// 可以根据需要处理错误，比如记录日志
		// log.Printf("创建PRJ文件失败: %v", err)
	}
}

// 处理单个要素的函数
func processFeature(feature *geojson.Feature) []ShapeData {
	var results []ShapeData

	// 转换属性
	attributes := make(map[string][]byte)
	for key, item := range feature.Properties {
		var itemStr string
		switch v := item.(type) {
		case string:
			itemStr = v
		case float64:
			itemStr = fmt.Sprintf("%f", v)
		case int:
			itemStr = fmt.Sprintf("%d", v)
		case nil:
			itemStr = ""
		default:
			itemStr = fmt.Sprintf("%v", v)
		}
		attributes[key] = Utf8ToGbk(itemStr)
	}

	// 根据几何类型处理
	switch geom := feature.Geometry.(type) {
	case orb.Polygon:
		var PL [][]shp.Point
		for _, ring := range geom {
			var points []shp.Point
			for _, pt := range ring {
				points = append(points, shp.Point{pt[0], pt[1]})
			}
			PL = append(PL, points)
		}
		NEWPL := shp.NewPolyLine(PL)
		results = append(results, ShapeData{
			Shape:      NEWPL,
			Attributes: attributes,
			GeomType:   "polygon",
		})

	case orb.MultiPolygon:
		for _, polygon := range geom {
			var PL [][]shp.Point
			for _, ring := range polygon {
				var points []shp.Point
				for _, pt := range ring {
					points = append(points, shp.Point{pt[0], pt[1]})
				}
				PL = append(PL, points)
			}
			NEWPL := shp.NewPolyLine(PL)
			results = append(results, ShapeData{
				Shape:      NEWPL,
				Attributes: attributes,
				GeomType:   "polygon",
			})
		}

	case orb.LineString:
		var PL [][]shp.Point
		var points []shp.Point
		for _, pt := range geom {
			points = append(points, shp.Point{pt[0], pt[1]})
		}
		PL = append(PL, points)
		NEWPL := shp.NewPolyLine(PL)
		results = append(results, ShapeData{
			Shape:      NEWPL,
			Attributes: attributes,
			GeomType:   "line",
		})

	case orb.Point:
		var NewPT shp.Point
		NewPT.X = geom[0]
		NewPT.Y = geom[1]
		results = append(results, ShapeData{
			Shape:      &NewPT,
			Attributes: attributes,
			GeomType:   "point",
		})

	case orb.MultiPoint:
		if len(geom) > 0 {
			pt := geom[0]
			var NewPT shp.Point
			NewPT.X = pt[0]
			NewPT.Y = pt[1]
			results = append(results, ShapeData{
				Shape:      &NewPT,
				Attributes: attributes,
				GeomType:   "point",
			})
		}
	}

	return results
}

// 批量写入形状数据到文件
func writeShapesToFile(shpFile *shp.Writer, shapes []ShapeData, fieldMap map[string]int) {
	for i, shapeData := range shapes {
		// 写入几何形状
		shpFile.Write(shapeData.Shape)

		// 写入属性
		for key, value := range shapeData.Attributes {
			if fieldIndex, exists := fieldMap[key]; exists {
				error := shpFile.WriteAttribute(i, fieldIndex, value)
				if error != nil {
					fmt.Println(error.Error())
				}
			}
		}
	}
}

func ConvertPolygonToMultiPolygon(points []shp.Point, parts []int32) (orb.MultiPolygon, string) {
	var multiPolygon orb.MultiPolygon

	polygons := SplitPoints(points, parts)
	dounts := []bool{}
	for _, part := range polygons {
		var orbPoints []orb.Point
		for _, point := range part {
			orbPoints = append(orbPoints, orb.Point{point.X, point.Y})
		}
		dount := IsClockwise(orbPoints)
		dounts = append(dounts, dount)
	}
	isTransform := ""
	polygonsIndex := createIndexSlice(int32(len(polygons)))
	newparts := splitParts(polygonsIndex, dounts)

	for _, item := range newparts {
		var rings []orb.Ring
		for _, i := range item {
			coords := make([]orb.Point, len(polygons[i]))
			for j, vertex := range polygons[i] {
				x := vertex.X
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
				coords[j] = orb.Point{vertex.X, vertex.Y}
			}
			rings = append(rings, orb.Ring(coords))
		}
		polygon := orb.Polygon(rings)
		multiPolygon = append(multiPolygon, polygon)
	}

	return multiPolygon, isTransform
}
