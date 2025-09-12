package Tin

import (
	"encoding/json"
	"fmt"
	"math"
)

func CoordsToPoint3D(coords [][]float64) ([]*Point3D, error) {
	if len(coords) == 0 {
		return nil, fmt.Errorf("coords is empty")
	}

	points := make([]*Point3D, len(coords))

	for i, coord := range coords {
		if len(coord) < 2 {
			return nil, fmt.Errorf("coordinate at index %d has insufficient dimensions (need at least 2, got %d)", i, len(coord))
		}

		point := &Point3D{
			X:  coord[0],
			Y:  coord[1],
			Z:  0.0, // 默认Z值
			ID: i,
		}

		// 如果提供了Z坐标，使用它
		if len(coord) >= 3 {
			point.Z = coord[2]
		}

		points[i] = point
	}

	return points, nil
}

// GeoJSONGeometry 表示GeoJSON几何对象的结构
type GeoJSONGeometry struct {
	Type        string          `json:"type"`
	Coordinates json.RawMessage `json:"coordinates"`
}

// GeometryStringToPolygon2D 将GeoJSON Geometry字符串转换为Polygon2D对象
// 支持的几何类型：Polygon, MultiPolygon
// 对于MultiPolygon，返回第一个多边形；对于有洞的多边形，只返回外环
func GeometryStringToPolygon2D(geometryStr string) (*Polygon2D, error) {
	var geom GeoJSONGeometry
	if err := json.Unmarshal([]byte(geometryStr), &geom); err != nil {
		return nil, fmt.Errorf("failed to parse geometry JSON: %v", err)
	}

	switch geom.Type {
	case "Polygon":
		return parsePolygon(geom.Coordinates)
	case "MultiPolygon":
		return parseMultiPolygon(geom.Coordinates)
	default:
		return nil, fmt.Errorf("unsupported geometry type: %s (only Polygon and MultiPolygon are supported)", geom.Type)
	}
}

// parsePolygon 解析Polygon类型的坐标
// Polygon格式: [[[x1,y1],[x2,y2],...]] (外环 + 可选的内环)
func parsePolygon(coordinates json.RawMessage) (*Polygon2D, error) {
	var rings [][][]float64
	if err := json.Unmarshal(coordinates, &rings); err != nil {
		return nil, fmt.Errorf("failed to parse polygon coordinates: %v", err)
	}

	if len(rings) == 0 {
		return nil, fmt.Errorf("polygon has no rings")
	}

	// 只取外环（第一个环）
	outerRing := rings[0]
	if len(outerRing) < 3 {
		return nil, fmt.Errorf("polygon outer ring must have at least 3 points")
	}

	return coordsToPolygon2D(outerRing)
}

// parseMultiPolygon 解析MultiPolygon类型的坐标
// MultiPolygon格式: [[[[x1,y1],[x2,y2],...]]] (多个多边形)
func parseMultiPolygon(coordinates json.RawMessage) (*Polygon2D, error) {
	var multiPolygon [][][][]float64
	if err := json.Unmarshal(coordinates, &multiPolygon); err != nil {
		return nil, fmt.Errorf("failed to parse multipolygon coordinates: %v", err)
	}

	if len(multiPolygon) == 0 {
		return nil, fmt.Errorf("multipolygon has no polygons")
	}

	// 取第一个多边形的外环
	firstPolygon := multiPolygon[0]
	if len(firstPolygon) == 0 {
		return nil, fmt.Errorf("first polygon has no rings")
	}

	outerRing := firstPolygon[0]
	if len(outerRing) < 3 {
		return nil, fmt.Errorf("polygon outer ring must have at least 3 points")
	}

	return coordsToPolygon2D(outerRing)
}

// coordsToPolygon2D 将坐标数组转换为Polygon2D
func coordsToPolygon2D(coords [][]float64) (*Polygon2D, error) {
	if len(coords) < 3 {
		return nil, fmt.Errorf("polygon must have at least 3 points")
	}

	points := make([]*Point2D, 0, len(coords))

	for i, coord := range coords {
		if len(coord) < 2 {
			return nil, fmt.Errorf("coordinate at index %d has insufficient dimensions", i)
		}

		// 检查坐标值是否有效
		if math.IsNaN(coord[0]) || math.IsInf(coord[0], 0) ||
			math.IsNaN(coord[1]) || math.IsInf(coord[1], 0) {
			return nil, fmt.Errorf("invalid coordinate at index %d: [%f, %f]", i, coord[0], coord[1])
		}

		point := &Point2D{
			X:  coord[0],
			Y:  coord[1],
			ID: i,
		}
		points = append(points, point)
	}

	// 检查并移除重复的闭合点（GeoJSON多边形的第一个和最后一个点通常相同）
	if len(points) > 1 {
		first := points[0]
		last := points[len(points)-1]
		if math.Abs(first.X-last.X) < 1e-10 && math.Abs(first.Y-last.Y) < 1e-10 {
			points = points[:len(points)-1]
			// 重新分配ID
			for i := range points {
				points[i].ID = i
			}
		}
	}

	return &Polygon2D{Points: points}, nil
}

// GeometryStringToPolygon2DWithAllRings 将GeoJSON Geometry字符串转换为多个Polygon2D对象
// 返回所有的环（外环和内环），适用于需要处理带洞多边形的情况
func GeometryStringToPolygon2DWithAllRings(geometryStr string) ([]*Polygon2D, error) {
	var geom GeoJSONGeometry
	if err := json.Unmarshal([]byte(geometryStr), &geom); err != nil {
		return nil, fmt.Errorf("failed to parse geometry JSON: %v", err)
	}

	switch geom.Type {
	case "Polygon":
		return parsePolygonAllRings(geom.Coordinates)
	case "MultiPolygon":
		return parseMultiPolygonAllRings(geom.Coordinates)
	default:
		return nil, fmt.Errorf("unsupported geometry type: %s", geom.Type)
	}
}

// parsePolygonAllRings 解析Polygon的所有环
func parsePolygonAllRings(coordinates json.RawMessage) ([]*Polygon2D, error) {
	var rings [][][]float64
	if err := json.Unmarshal(coordinates, &rings); err != nil {
		return nil, fmt.Errorf("failed to parse polygon coordinates: %v", err)
	}

	if len(rings) == 0 {
		return nil, fmt.Errorf("polygon has no rings")
	}

	polygons := make([]*Polygon2D, 0, len(rings))
	for i, ring := range rings {
		if len(ring) < 3 {
			return nil, fmt.Errorf("ring %d must have at least 3 points", i)
		}

		polygon, err := coordsToPolygon2D(ring)
		if err != nil {
			return nil, fmt.Errorf("failed to parse ring %d: %v", i, err)
		}
		polygons = append(polygons, polygon)
	}

	return polygons, nil
}

// parseMultiPolygonAllRings 解析MultiPolygon的所有多边形的所有环
func parseMultiPolygonAllRings(coordinates json.RawMessage) ([]*Polygon2D, error) {
	var multiPolygon [][][][]float64
	if err := json.Unmarshal(coordinates, &multiPolygon); err != nil {
		return nil, fmt.Errorf("failed to parse multipolygon coordinates: %v", err)
	}

	if len(multiPolygon) == 0 {
		return nil, fmt.Errorf("multipolygon has no polygons")
	}

	var allPolygons []*Polygon2D
	for i, polygon := range multiPolygon {
		for j, ring := range polygon {
			if len(ring) < 3 {
				return nil, fmt.Errorf("polygon %d ring %d must have at least 3 points", i, j)
			}

			poly, err := coordsToPolygon2D(ring)
			if err != nil {
				return nil, fmt.Errorf("failed to parse polygon %d ring %d: %v", i, j, err)
			}
			allPolygons = append(allPolygons, poly)
		}
	}

	return allPolygons, nil
}

// GeometryStringToMultiPolygon2D 将GeoJSON Geometry字符串转换为多个独立的Polygon2D对象
// 每个多边形只包含外环，适用于MultiPolygon数据
func GeometryStringToMultiPolygon2D(geometryStr string) ([]*Polygon2D, error) {
	var geom GeoJSONGeometry
	if err := json.Unmarshal([]byte(geometryStr), &geom); err != nil {
		return nil, fmt.Errorf("failed to parse geometry JSON: %v", err)
	}

	switch geom.Type {
	case "Polygon":
		polygon, err := parsePolygon(geom.Coordinates)
		if err != nil {
			return nil, err
		}
		return []*Polygon2D{polygon}, nil
	case "MultiPolygon":
		return parseMultiPolygonOuterRings(geom.Coordinates)
	default:
		return nil, fmt.Errorf("unsupported geometry type: %s", geom.Type)
	}
}

// parseMultiPolygonOuterRings 解析MultiPolygon的所有外环
func parseMultiPolygonOuterRings(coordinates json.RawMessage) ([]*Polygon2D, error) {
	var multiPolygon [][][][]float64
	if err := json.Unmarshal(coordinates, &multiPolygon); err != nil {
		return nil, fmt.Errorf("failed to parse multipolygon coordinates: %v", err)
	}

	if len(multiPolygon) == 0 {
		return nil, fmt.Errorf("multipolygon has no polygons")
	}

	polygons := make([]*Polygon2D, 0, len(multiPolygon))
	for i, polygon := range multiPolygon {
		if len(polygon) == 0 {
			return nil, fmt.Errorf("polygon %d has no rings", i)
		}

		// 只取外环
		outerRing := polygon[0]
		if len(outerRing) < 3 {
			return nil, fmt.Errorf("polygon %d outer ring must have at least 3 points", i)
		}

		poly, err := coordsToPolygon2D(outerRing)
		if err != nil {
			return nil, fmt.Errorf("failed to parse polygon %d: %v", i, err)
		}
		polygons = append(polygons, poly)
	}

	return polygons, nil
}

// GetPolygonInfo 获取多边形的基本信息
func GetPolygonInfo(polygon *Polygon2D) map[string]interface{} {
	info := make(map[string]interface{})

	if polygon == nil || len(polygon.Points) == 0 {
		return info
	}

	info["pointCount"] = len(polygon.Points)

	// 计算边界框
	minX, maxX := polygon.Points[0].X, polygon.Points[0].X
	minY, maxY := polygon.Points[0].Y, polygon.Points[0].Y

	for _, point := range polygon.Points {
		if point.X < minX {
			minX = point.X
		}
		if point.X > maxX {
			maxX = point.X
		}
		if point.Y < minY {
			minY = point.Y
		}
		if point.Y > maxY {
			maxY = point.Y
		}
	}

	info["bounds"] = map[string]float64{
		"minX":   minX,
		"maxX":   maxX,
		"minY":   minY,
		"maxY":   maxY,
		"width":  maxX - minX,
		"height": maxY - minY,
	}

	// 计算周长
	perimeter := 0.0
	for i := 0; i < len(polygon.Points); i++ {
		j := (i + 1) % len(polygon.Points)
		dx := polygon.Points[j].X - polygon.Points[i].X
		dy := polygon.Points[j].Y - polygon.Points[i].Y
		perimeter += math.Sqrt(dx*dx + dy*dy)
	}
	info["perimeter"] = perimeter

	// 计算面积（使用鞋带公式）
	area := 0.0
	for i := 0; i < len(polygon.Points); i++ {
		j := (i + 1) % len(polygon.Points)
		area += polygon.Points[i].X * polygon.Points[j].Y
		area -= polygon.Points[j].X * polygon.Points[i].Y
	}
	area = math.Abs(area) / 2.0
	info["area"] = area

	return info
}
