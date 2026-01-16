package Transformer

import (
	"fmt"
	"gitee.com/LJ_COOL/go-shp"
	"github.com/GrainArc/SouceMap/models"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
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
	// 打开 shapefile 文件
	shape, err := shp.Open(shpfileFilePath)
	if err != nil {
		// 返回错误而不是仅记录日志
		return nil, ""
	}
	defer shape.Close() // 确保文件关闭

	// 初始化 GeoJSON FeatureCollection
	featureCollection := geojson.NewFeatureCollection()

	// 从 DBF 文件获取字段定义
	fields := shape.Fields()

	// 读取字符编码配置(CPG 文件)，移到循环外避免重复读取
	encoding := readCPGEncoding(shpfileFilePath)

	// 用于存储检测到的坐标系，使用 map 去重
	detectedCRS := make(map[string]bool)

	// 遍历 shapefile 中的所有要素
	for shape.Next() {
		n, p := shape.Shape() // 获取要素索引和几何对象

		// 根据几何类型处理要素
		switch s := p.(type) {
		case *shp.Point:
			// 处理 Point 类型
			feature := processPointGeometry(s.X, s.Y, n, shape, fields, encoding, detectedCRS)
			featureCollection.Append(feature)

		case *shp.PointZ:
			// 处理 PointZ 类型(带 Z 坐标)
			feature := processPointGeometry(s.X, s.Y, n, shape, fields, encoding, detectedCRS)
			featureCollection.Append(feature)

		case *shp.PointM:
			// 处理 PointM 类型(带 M 值)
			feature := processPointGeometry(s.X, s.Y, n, shape, fields, encoding, detectedCRS)
			featureCollection.Append(feature)

		case *shp.PolyLine:
			// 处理 PolyLine 类型
			feature := processPolyLineGeometry(s.Points, n, shape, fields, encoding, detectedCRS)
			featureCollection.Append(feature)

		case *shp.PolyLineZ:
			// 处理 PolyLineZ 类型(带 Z 坐标)
			feature := processPolyLineGeometry(s.Points, n, shape, fields, encoding, detectedCRS)
			featureCollection.Append(feature)

		case *shp.PolyLineM:
			// 处理 PolyLineM 类型(带 M 值)
			feature := processPolyLineGeometry(s.Points, n, shape, fields, encoding, detectedCRS)
			featureCollection.Append(feature)

		case *shp.Polygon:
			// 处理 Polygon 类型
			feature := processPolygonGeometry(s.Points, s.Parts, n, shape, fields, encoding, detectedCRS)
			featureCollection.Append(feature)

		case *shp.PolygonZ:
			// 处理 PolygonZ 类型(带 Z 坐标)
			feature := processPolygonGeometry(s.Points, s.Parts, n, shape, fields, encoding, detectedCRS)
			featureCollection.Append(feature)

		case *shp.PolygonM:
			// 处理 PolygonM 类型(带 M 值)
			feature := processPolygonGeometry(s.Points, s.Parts, n, shape, fields, encoding, detectedCRS)
			featureCollection.Append(feature)
		}
	}

	// 从检测到的坐标系中选择一个返回(优先级排序)
	crsResult := selectCRS(detectedCRS)

	return featureCollection, crsResult
}

// readCPGEncoding 读取 CPG 文件获取字符编码
// 参数: shpfilePath - shapefile 文件路径
// 返回: 编码名称,默认为 "GBK"
func readCPGEncoding(shpfilePath string) string {
	// 获取文件所在目录
	dir := filepath.Dir(shpfilePath)
	// 获取文件基础名称(不含路径)
	base := filepath.Base(shpfilePath)
	// 将扩展名替换为 .cpg
	newBase := strings.TrimSuffix(base, filepath.Ext(base)) + ".cpg"
	// 拼接完整的 CPG 文件路径
	cpgPath := filepath.Join(dir, newBase)

	// 读取 CPG 文件内容
	cpgContent, err := os.ReadFile(cpgPath)
	if err != nil {
		// 如果文件不存在或读取失败,默认使用 GBK 编码
		return "GBK"
	}
	// 返回文件内容作为编码名称
	return strings.TrimSpace(string(cpgContent))
}

// detectCRS 根据 X 坐标值判断坐标系
// 参数: x - X 坐标值
// 返回: 坐标系 EPSG 代码
func detectCRS(x float64) string {
	// 根据坐标范围判断坐标系类型
	switch {
	case x <= 1000:
		return "4326" // WGS84 经纬度坐标系

	case x >= 100000 && x <= 10000000:
		return "4544" // CGCS2000 / 3-degree Gauss-Kruger zone

	// CGCS2000 / Gauss-Kruger 6度分带投影坐标系
	case x >= 33000000 && x <= 34000000:
		return "4521" // CGCS2000 / Gauss-Kruger CM 75E (带号13)
	case x >= 34000000 && x <= 35000000:
		return "4522" // CGCS2000 / Gauss-Kruger CM 81E (带号14)
	case x >= 35000000 && x <= 36000000:
		return "4523" // CGCS2000 / Gauss-Kruger CM 87E (带号15)
	case x >= 36000000 && x <= 37000000:
		return "4524" // CGCS2000 / Gauss-Kruger CM 93E (带号16)
	case x >= 37000000 && x <= 38000000:
		return "4525" // CGCS2000 / Gauss-Kruger CM 99E (带号17)
	case x >= 38000000 && x <= 39000000:
		return "4526" // CGCS2000 / Gauss-Kruger CM 105E (带号18)
	case x >= 39000000 && x <= 40000000:
		return "4527" // CGCS2000 / Gauss-Kruger CM 111E (带号19)
	case x >= 40000000 && x <= 41000000:
		return "4528" // CGCS2000 / Gauss-Kruger CM 117E (带号20)
	case x >= 41000000 && x <= 42000000:
		return "4529" // CGCS2000 / Gauss-Kruger CM 123E (带号21)
	case x >= 42000000 && x <= 43000000:
		return "4530" // CGCS2000 / Gauss-Kruger CM 129E (带号22)
	case x >= 43000000 && x <= 44000000:
		return "4531" // CGCS2000 / Gauss-Kruger CM 135E (带号23)

	default:
		return "" // 未知坐标系
	}
}

// buildAttributes 构建要素属性字典
// 参数: n - 要素索引, shape - shapefile 读取器, fields - 字段定义, encoding - 字符编码
// 返回: 属性字典
func buildAttributes(n int, shape *shp.Reader, fields []shp.Field, encoding string) map[string]interface{} {
	// 初始化属性字典
	attrs := make(map[string]interface{})

	// 遍历所有字段
	for k, f := range fields {
		// 读取属性值
		attrValue := shape.ReadAttribute(n, k)

		if encoding == "GBK" {
			// GBK 编码需要转换为 UTF-8
			fieldName := GbkToUtf8(f.String())                   // 转换字段名
			convertedValue := GbkToUtf8(attrValue)               // 转换属性值
			attrs[fieldName] = trimTrailingZeros(convertedValue) // 去除尾部零并存储
		} else {
			// 其他编码直接使用
			attrs[f.String()] = trimTrailingZeros(attrValue) // 去除尾部零并存储
		}
	}

	// 如果没有任何字段,添加一个默认属性
	if len(fields) == 0 {
		attrs["attribute"] = "null"
	}

	return attrs
}

// processPointGeometry 处理点类型几何对象
// 参数: x, y - 坐标值, n - 要素索引, shape - shapefile 读取器, fields - 字段定义, encoding - 编码, detectedCRS - 坐标系集合
// 返回: GeoJSON Feature 对象
func processPointGeometry(x, y float64, n int, shape *shp.Reader, fields []shp.Field, encoding string, detectedCRS map[string]bool) *geojson.Feature {
	// 检测并记录坐标系
	if crs := detectCRS(x); crs != "" {
		detectedCRS[crs] = true
	}

	// 创建点几何对象
	geometry := orb.Point{x, y}

	// 构建属性字典
	attrs := buildAttributes(n, shape, fields, encoding)

	// 创建 GeoJSON Feature 并设置属性
	feature := geojson.NewFeature(geometry)
	feature.Properties = attrs

	return feature
}

// processPolyLineGeometry 处理线类型几何对象
// 参数: points - 点集合, n - 要素索引, shape - shapefile 读取器, fields - 字段定义, encoding - 编码, detectedCRS - 坐标系集合
// 返回: GeoJSON Feature 对象
func processPolyLineGeometry(points []shp.Point, n int, shape *shp.Reader, fields []shp.Field, encoding string, detectedCRS map[string]bool) *geojson.Feature {
	// 预分配坐标数组
	coords := make([]orb.Point, len(points))

	// 转换所有点坐标
	for i, vertex := range points {
		// 检测并记录坐标系
		if crs := detectCRS(vertex.X); crs != "" {
			detectedCRS[crs] = true
		}
		// 转换为 orb.Point 格式
		coords[i] = orb.Point{vertex.X, vertex.Y}
	}

	// 创建线几何对象
	line := orb.LineString(coords)

	// 构建属性字典
	attrs := buildAttributes(n, shape, fields, encoding)

	// 创建 GeoJSON Feature 并设置属性
	feature := geojson.NewFeature(line)
	feature.Properties = attrs

	return feature
}

// processPolygonGeometry 处理面类型几何对象
// 参数: points - 点集合, parts - 部分索引, n - 要素索引, shape - shapefile 读取器, fields - 字段定义, encoding - 编码, detectedCRS - 坐标系集合
// 返回: GeoJSON Feature 对象
func processPolygonGeometry(points []shp.Point, parts []int32, n int, shape *shp.Reader, fields []shp.Field, encoding string, detectedCRS map[string]bool) *geojson.Feature {
	// 初始化多面几何对象
	var multiPolygons orb.MultiPolygon

	// 将点集按 parts 分割成多个环
	polygons := SplitPoints(points, parts)

	// 判断每个环是外环还是内环(基于顺时针/逆时针)
	dounts := make([]bool, len(polygons))
	for i, part := range polygons {
		// 转换为 orb.Point 格式用于判断
		orbPoints := make([]orb.Point, len(part))
		for j, point := range part {
			orbPoints[j] = orb.Point{point.X, point.Y}
		}
		// 判断环的方向(顺时针为外环)
		dounts[i] = IsClockwise(orbPoints)
	}

	// 创建索引切片
	polygonsIndex := createIndexSlice(int32(len(polygons)))
	// 根据环的方向将索引分组(外环+内环组成完整多边形)
	newParts := splitParts(polygonsIndex, dounts)

	// 遍历每个多边形组
	for _, item := range newParts {
		var rings []orb.Ring // 存储当前多边形的所有环

		// 遍历当前组的所有环索引
		for _, i := range item {
			// 预分配坐标数组
			coords := make([]orb.Point, len(polygons[i]))

			// 转换环上的所有点
			for j, vertex := range polygons[i] {
				// 检测并记录坐标系
				if crs := detectCRS(vertex.X); crs != "" {
					detectedCRS[crs] = true
				}
				// 转换为 orb.Point 格式
				coords[j] = orb.Point{vertex.X, vertex.Y}
			}

			// 将坐标数组转换为环并添加到环列表
			rings = append(rings, orb.Ring(coords))
		}

		// 将所有环组合成一个多边形
		polygon := orb.Polygon(rings)
		// 添加到多面几何对象
		multiPolygons = append(multiPolygons, polygon)
	}

	// 构建属性字典
	attrs := buildAttributes(n, shape, fields, encoding)

	// 创建 GeoJSON Feature 并设置属性
	feature := geojson.NewFeature(multiPolygons)
	feature.Properties = attrs

	return feature
}

// selectCRS 从检测到的坐标系中选择一个返回
// 参数: detectedCRS - 检测到的坐标系集合
// 返回: 选中的坐标系代码
func selectCRS(detectedCRS map[string]bool) string {
	// 定义坐标系优先级顺序
	priority := []string{"4326", "4544", "4521", "4522", "4523", "4524"}

	// 按优先级查找
	for _, crs := range priority {
		if detectedCRS[crs] {
			return crs // 返回找到的第一个坐标系
		}
	}

	// 如果没有匹配的,返回空字符串
	return ""
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
	dirPath := filepath.Dir(shpfileFilePath)

	shpfileFilePath_point := filepath.Join(dirPath, rootName) + "_点.shp"
	shpfileFilePath_polygon := filepath.Join(dirPath, rootName) + "_面.shp"
	shpfileFilePath_line := filepath.Join(dirPath, rootName) + "_线.shp"

	shpFile_polygon, _ := shp.Create(shpfileFilePath_polygon, shp.POLYGON)
	shpFile_line, _ := shp.Create(shpfileFilePath_line, shp.POLYLINE)
	shpFile_point, _ := shp.Create(shpfileFilePath_point, shp.POINT)

	// 创建CPG文件
	createCpgFile(filepath.Join(dirPath, rootName) + "_点.cpg")
	createCpgFile(filepath.Join(dirPath, rootName) + "_面.cpg")
	createCpgFile(filepath.Join(dirPath, rootName) + "_线.cpg")

	// 创建PRJ投影文件
	createPrjFile(filepath.Join(dirPath, rootName) + "_点.prj")
	createPrjFile(filepath.Join(dirPath, rootName) + "_面.prj")
	createPrjFile(filepath.Join(dirPath, rootName) + "_线.prj")

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
					error := shpFile_polygon.WriteAttribute(pn, FieldMAP[key], Utf8ToGbk(itemStr))
					if error != nil {
						fmt.Println(error.Error())
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
						error := shpFile_polygon.WriteAttribute(pn, FieldMAP[key], Utf8ToGbk(itemStr))
						if error != nil {
							fmt.Println(error.Error())
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

	// 检查并删除空的shapefile及其关联文件（包括prj文件）
	checkAndDeleteEmptyShapefile(shpfileFilePath_point, rootName+"_点")
	checkAndDeleteEmptyShapefile(shpfileFilePath_polygon, rootName+"_面")
	checkAndDeleteEmptyShapefile(shpfileFilePath_line, rootName+"_线")
}
func createPrjFile(prjFilePath string) error {
	prjContent := `GEOGCS["GCS_China_Geodetic_Coordinate_System_2000",DATUM["D_China_2000",SPHEROID["CGCS2000",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]`

	file, err := os.Create(prjFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(prjContent)
	return err
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
