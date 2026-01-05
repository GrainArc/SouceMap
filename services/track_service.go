// services/track_service.go
package services

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"math"
	"sort"
	"strings"
)

type TrackService struct{}

func NewTrackService() *TrackService {
	return &TrackService{}
}

func (s *TrackService) GetAndBreakGeometries(
	ctx context.Context,
	layerNames []string,
	bbox geojson.FeatureCollection,
) (*geojson.FeatureCollection, error) {
	bboxWKT := s.featureCollectionToWKT(bbox)
	if bboxWKT == "" {
		return nil, fmt.Errorf("invalid bbox")
	}

	// 构建 UNION 查询
	var unionQueries []string
	for _, layerName := range layerNames {
		unionQueries = append(unionQueries, fmt.Sprintf(`
			SELECT ST_Intersection(t.geom, bbox.geom) as geom
			FROM %s t, bbox
			WHERE ST_Intersects(t.geom, bbox.geom)
		`, layerName))
	}

	query := fmt.Sprintf(`
		WITH bbox AS (
			SELECT ST_GeomFromText($1, 4326) as geom
		),
		all_geoms AS (
			%s
		),
		-- 展开所有几何（处理 GeometryCollection）
		dumped AS (
			SELECT (ST_Dump(g.geom)).geom as geom
			FROM all_geoms g
			WHERE g.geom IS NOT NULL 
			  AND NOT ST_IsEmpty(g.geom)
		),
		-- 转换为线
		as_lines AS (
			SELECT 
				CASE 
					WHEN ST_GeometryType(d.geom) = 'ST_LineString' THEN d.geom
					WHEN ST_GeometryType(d.geom) = 'ST_Polygon' THEN ST_Boundary(d.geom)
					ELSE NULL
				END as geom
			FROM dumped d
			WHERE ST_GeometryType(d.geom) IN ('ST_LineString', 'ST_Polygon')
		),
		-- 过滤有效线
		valid_lines AS (
			SELECT l.geom
			FROM as_lines l
			WHERE l.geom IS NOT NULL 
			  AND NOT ST_IsEmpty(l.geom)
			  AND ST_GeometryType(l.geom) = 'ST_LineString'
		),
		-- 节点化打断
		noded AS (
			SELECT ST_Node(ST_Collect(v.geom)) as geom
			FROM valid_lines v
		),
		-- 展开为单线段
		segments AS (
			SELECT 
				row_number() OVER () as id,
				(ST_Dump(n.geom)).geom as geom
			FROM noded n
			WHERE n.geom IS NOT NULL
		)
		SELECT 
			s.id,
			ST_AsGeoJSON(s.geom, 15)::json as geom_json,
			ST_Length(s.geom::geography) as length
		FROM segments s
		WHERE ST_GeometryType(s.geom) = 'ST_LineString'
		  AND ST_Length(s.geom) > 0
	`, strings.Join(unionQueries, "\n\t\tUNION ALL\n\t\t"))

	var geometries []struct {
		ID       int             `gorm:"column:id"`
		GeomJSON json.RawMessage `gorm:"column:geom_json"`
		Length   float64         `gorm:"column:length"`
	}

	if err := models.DB.WithContext(ctx).Raw(query, bboxWKT).Scan(&geometries).Error; err != nil {
		return nil, fmt.Errorf("failed to get and break geometries: %w", err)
	}

	fc := geojson.NewFeatureCollection()

	for _, geom := range geometries {
		var geoJSONGeom geojson.Geometry
		if err := json.Unmarshal(geom.GeomJSON, &geoJSONGeom); err != nil {
			continue
		}

		orbGeom := geoJSONGeom.Geometry()
		if orbGeom == nil {
			continue
		}

		feature := geojson.NewFeature(orbGeom)
		feature.Properties["id"] = geom.ID
		feature.Properties["length"] = geom.Length

		fc.Append(feature)
	}

	return fc, nil
}

func (s *TrackService) CalculateShortestPath(
	ctx context.Context,
	linesGeoJSON *geojson.FeatureCollection,
	startPoint []float64,
	endPoint []float64,
) (*geojson.FeatureCollection, error) {
	if linesGeoJSON == nil || len(linesGeoJSON.Features) == 0 {
		return nil, fmt.Errorf("no line segments available")
	}

	if len(startPoint) < 2 || len(endPoint) < 2 {
		return nil, fmt.Errorf("invalid start or end point")
	}

	// 转换为 orb.Point
	startOrbPoint := orb.Point{startPoint[0], startPoint[1]}
	endOrbPoint := orb.Point{endPoint[0], endPoint[1]}

	// 找到起点和终点在线段上的最近投影点，并打断线段
	splitLinesGeoJSON, actualStartPoint, actualEndPoint := splitLinesAtPoints(
		linesGeoJSON,
		startOrbPoint,
		endOrbPoint,
	)

	if splitLinesGeoJSON == nil || len(splitLinesGeoJSON.Features) == 0 {
		return nil, fmt.Errorf("failed to split lines")
	}

	// 使用打断后的线段构建图
	graph := buildGraph(splitLinesGeoJSON)

	// 找到实际起点和终点对应的节点
	startNode := findExactNode(graph, actualStartPoint)
	endNode := findExactNode(graph, actualEndPoint)

	if startNode == nil || endNode == nil {
		return nil, fmt.Errorf("could not find valid nodes for routing")
	}

	// 使用 Dijkstra 算法计算最短路径
	path := dijkstra(graph, startNode, endNode)

	if len(path) == 0 {
		return nil, fmt.Errorf("no path found between start and end points")
	}

	// 构建 GeoJSON FeatureCollection
	resultCollection := &geojson.FeatureCollection{
		Type:     "FeatureCollection",
		Features: make([]*geojson.Feature, 0),
	}

	// 将所有边串联成一个 LineString
	mergedLineString := mergeEdgesToLineString(path)

	// 计算总成本
	totalCost := 0.0
	edgeIDs := make([]int, 0, len(path))
	for _, edge := range path {
		totalCost += edge.Cost
		edgeIDs = append(edgeIDs, edge.ID)
	}

	// 创建单个 Feature，包含合并后的 LineString
	feature := geojson.NewFeature(mergedLineString)
	feature.Properties = geojson.Properties{
		"total_cost":   totalCost,
		"total_length": calculateLength(mergedLineString),
		"edge_count":   len(path),
		"edge_ids":     edgeIDs,
	}
	resultCollection.Features = append(resultCollection.Features, feature)

	return resultCollection, nil
}

// mergeEdgesToLineString 将多条边合并成一条 LineString
func mergeEdgesToLineString(edges []*Edge) orb.LineString {
	if len(edges) == 0 {
		return orb.LineString{}
	}

	// 预估总点数以优化内存分配
	totalPoints := 0
	for _, edge := range edges {
		totalPoints += len(edge.LineString)
	}

	merged := make(orb.LineString, 0, totalPoints)

	for i, edge := range edges {
		ls := edge.LineString
		if len(ls) == 0 {
			continue
		}

		if i == 0 {
			// 第一条边，添加所有点
			merged = append(merged, ls...)
		} else {
			// 后续边，跳过第一个点（因为它与前一条边的最后一个点重复）
			if len(ls) > 1 {
				merged = append(merged, ls[1:]...)
			}
		}
	}

	return merged
}

// SplitResult 存储线段打断的结果
type SplitResult struct {
	Feature      *geojson.Feature
	SplitPoint   orb.Point
	SegmentIndex int     // 在哪个线段上打断
	T            float64 // 投影参数 (0-1)，表示在该 segment 上的位置
}

func splitLinesAtPoints(
	fc *geojson.FeatureCollection,
	startPoint, endPoint orb.Point,
) (*geojson.FeatureCollection, orb.Point, orb.Point) {

	// 找到起点和终点的最近线段和投影点
	startSplit := findNearestLineAndProject(fc, startPoint)
	endSplit := findNearestLineAndProject(fc, endPoint)

	if startSplit == nil || endSplit == nil {
		return nil, orb.Point{}, orb.Point{}
	}

	// 创建新的 FeatureCollection
	newFC := &geojson.FeatureCollection{
		Type:     "FeatureCollection",
		Features: make([]*geojson.Feature, 0),
	}

	// 记录需要打断的线段索引 -> 分割点列表
	splitIndices := make(map[int][]SplitResult)

	// 找到起点所在的 feature 索引
	startFeatureIdx := -1
	for i, feature := range fc.Features {
		if feature == startSplit.Feature {
			startFeatureIdx = i
			break
		}
	}

	// 找到终点所在的 feature 索引
	endFeatureIdx := -1
	for i, feature := range fc.Features {
		if feature == endSplit.Feature {
			endFeatureIdx = i
			break
		}
	}

	// 添加分割点到对应的 feature
	if startFeatureIdx >= 0 {
		splitIndices[startFeatureIdx] = append(splitIndices[startFeatureIdx], *startSplit)
	}
	if endFeatureIdx >= 0 {
		splitIndices[endFeatureIdx] = append(splitIndices[endFeatureIdx], *endSplit)
	}

	// 遍历所有线段，进行打断或保持原样
	for i, feature := range fc.Features {
		if feature.Geometry == nil {
			continue
		}

		lineString, ok := feature.Geometry.(orb.LineString)
		if !ok || len(lineString) < 2 {
			continue
		}

		splits, needSplit := splitIndices[i]
		if !needSplit {
			// 不需要打断，直接添加
			newFC.Features = append(newFC.Features, feature)
			continue
		}

		// 需要打断线段
		// 关键修复：先按 SegmentIndex 排序，再按 T 值排序
		sort.Slice(splits, func(a, b int) bool {
			if splits[a].SegmentIndex != splits[b].SegmentIndex {
				return splits[a].SegmentIndex < splits[b].SegmentIndex
			}
			return splits[a].T < splits[b].T
		})

		// 去重：如果两个分割点非常接近，只保留一个
		splits = deduplicateSplits(splits)

		// 执行打断
		splitLines := splitLineStringMultiple(lineString, splits)

		// 添加打断后的线段
		for _, splitLine := range splitLines {
			if len(splitLine) < 2 {
				continue
			}
			// 过滤掉太短的线段（小于 0.001 米）
			if calculateLength(splitLine) < 0.001 {
				continue
			}
			newFeature := geojson.NewFeature(splitLine)
			// 复制原始属性
			if feature.Properties != nil {
				for k, v := range feature.Properties {
					newFeature.Properties[k] = v
				}
			}
			newFC.Features = append(newFC.Features, newFeature)
		}
	}

	return newFC, startSplit.SplitPoint, endSplit.SplitPoint
}

// deduplicateSplits 去除非常接近的分割点
func deduplicateSplits(splits []SplitResult) []SplitResult {
	if len(splits) <= 1 {
		return splits
	}

	result := make([]SplitResult, 0, len(splits))
	result = append(result, splits[0])

	for i := 1; i < len(splits); i++ {
		prev := result[len(result)-1]
		curr := splits[i]

		// 如果在同一个 segment 上且 T 值非常接近，跳过
		if prev.SegmentIndex == curr.SegmentIndex {
			if math.Abs(prev.T-curr.T) < 0.0001 {
				continue
			}
		}

		// 如果两点距离非常近（小于 0.01 米），跳过
		dist := haversineDistance(prev.SplitPoint, curr.SplitPoint)
		if dist < 0.01 {
			continue
		}

		result = append(result, curr)
	}

	return result
}

// splitLineStringMultiple 根据多个分割点打断线段（支持同一 segment 上多个分割点）
func splitLineStringMultiple(ls orb.LineString, splits []SplitResult) []orb.LineString {
	if len(splits) == 0 {
		return []orb.LineString{ls}
	}

	result := make([]orb.LineString, 0)
	currentLine := make(orb.LineString, 0)

	// 为每个 segment 创建分割点列表
	segmentSplits := make(map[int][]SplitResult)
	for _, split := range splits {
		segmentSplits[split.SegmentIndex] = append(segmentSplits[split.SegmentIndex], split)
	}

	// 遍历线段的每个点
	for i := 0; i < len(ls); i++ {
		currentLine = append(currentLine, ls[i])

		// 如果不是最后一个点，检查当前 segment 是否有分割点
		if i < len(ls)-1 {
			segmentIdx := i
			splitsOnSegment, hasSplits := segmentSplits[segmentIdx]

			if hasSplits && len(splitsOnSegment) > 0 {
				// 当前 segment 上有分割点，按 T 值顺序处理
				// splitsOnSegment 已经按 T 值排序了
				for _, split := range splitsOnSegment {
					// 检查分割点是否与当前线段的最后一个点重合
					if len(currentLine) > 0 {
						lastPoint := currentLine[len(currentLine)-1]
						if haversineDistance(lastPoint, split.SplitPoint) < 0.01 {
							// 分割点与最后一个点重合，不需要添加
							// 但仍然需要结束当前线段并开始新线段
							if len(currentLine) >= 2 {
								result = append(result, currentLine)
							}
							currentLine = make(orb.LineString, 0)
							currentLine = append(currentLine, split.SplitPoint)
							continue
						}
					}

					// 添加分割点到当前线段
					currentLine = append(currentLine, split.SplitPoint)

					// 保存当前线段（如果有效）
					if len(currentLine) >= 2 {
						result = append(result, currentLine)
					}

					// 开始新线段，以分割点为起点
					currentLine = make(orb.LineString, 0)
					currentLine = append(currentLine, split.SplitPoint)
				}
			}
		}
	}

	// 添加最后一段（如果有效）
	if len(currentLine) >= 2 {
		result = append(result, currentLine)
	}

	return result
}

// findNearestLineAndProject 找到距离目标点最近的线段和投影点
func findNearestLineAndProject(fc *geojson.FeatureCollection, target orb.Point) *SplitResult {
	var result *SplitResult
	minDist := math.MaxFloat64

	for _, feature := range fc.Features {
		if feature.Geometry == nil {
			continue
		}

		lineString, ok := feature.Geometry.(orb.LineString)
		if !ok || len(lineString) < 2 {
			continue
		}

		// 遍历线段的每一段
		for i := 0; i < len(lineString)-1; i++ {
			p1 := lineString[i]
			p2 := lineString[i+1]

			// 计算投影点和参数 t
			projPoint, t := projectPointToSegmentWithT(target, p1, p2)
			dist := haversineDistance(projPoint, target)

			if dist < minDist {
				minDist = dist
				result = &SplitResult{
					Feature:      feature,
					SplitPoint:   projPoint,
					SegmentIndex: i,
					T:            t,
				}
			}
		}
	}

	return result
}

// projectPointToSegmentWithT 计算点到线段的投影点和参数 t
func projectPointToSegmentWithT(point, segStart, segEnd orb.Point) (orb.Point, float64) {
	x := point[0]
	y := point[1]
	x1 := segStart[0]
	y1 := segStart[1]
	x2 := segEnd[0]
	y2 := segEnd[1]

	dx := x2 - x1
	dy := y2 - y1

	if dx == 0 && dy == 0 {
		return segStart, 0
	}

	t := ((x-x1)*dx + (y-y1)*dy) / (dx*dx + dy*dy)

	// 限制 t 在 [0, 1] 范围内
	if t < 0 {
		return segStart, 0
	} else if t > 1 {
		return segEnd, 1
	}

	projX := x1 + t*dx
	projY := y1 + t*dy

	return orb.Point{projX, projY}, t
}

// splitLineString 根据分割点打断线段
func splitLineString(ls orb.LineString, splits []SplitResult) []orb.LineString {
	if len(splits) == 0 {
		return []orb.LineString{ls}
	}

	result := make([]orb.LineString, 0)
	currentLine := make(orb.LineString, 0)

	segmentIdx := 0
	splitIdx := 0

	for i := 0; i < len(ls); i++ {
		currentLine = append(currentLine, ls[i])

		// 检查当前点之后是否有分割点
		if i < len(ls)-1 {
			// 检查是否在当前线段上有分割点
			for splitIdx < len(splits) && splits[splitIdx].SegmentIndex == segmentIdx {
				split := splits[splitIdx]

				// 添加分割点
				currentLine = append(currentLine, split.SplitPoint)

				// 保存当前线段
				if len(currentLine) >= 2 {
					result = append(result, currentLine)
				}

				// 开始新线段
				currentLine = make(orb.LineString, 0)
				currentLine = append(currentLine, split.SplitPoint)

				splitIdx++
			}
			segmentIdx++
		}
	}

	// 添加最后一段
	if len(currentLine) >= 2 {
		result = append(result, currentLine)
	}

	return result
}

// Node 表示图中的节点
type Node struct {
	ID    string
	Point orb.Point
	Edges []*Edge
}

// Edge 表示图中的边
type Edge struct {
	ID         int
	From       *Node
	To         *Node
	LineString orb.LineString
	Cost       float64
}

// Graph 表示路网图
type Graph struct {
	Nodes map[string]*Node
	Edges []*Edge
}

// buildGraph 从 GeoJSON 构建图
func buildGraph(fc *geojson.FeatureCollection) *Graph {
	graph := &Graph{
		Nodes: make(map[string]*Node),
		Edges: make([]*Edge, 0),
	}

	for i, feature := range fc.Features {
		if feature.Geometry == nil {
			continue
		}

		lineString, ok := feature.Geometry.(orb.LineString)
		if !ok || len(lineString) < 2 {
			continue
		}

		// 获取起点和终点
		startPoint := lineString[0]
		endPoint := lineString[len(lineString)-1]

		// 创建或获取起点节点
		startNodeID := fmt.Sprintf("%.12f,%.12f", startPoint[0], startPoint[1])
		startNode := graph.Nodes[startNodeID]
		if startNode == nil {
			startNode = &Node{
				ID:    startNodeID,
				Point: startPoint,
				Edges: make([]*Edge, 0),
			}
			graph.Nodes[startNodeID] = startNode
		}

		// 创建或获取终点节点
		endNodeID := fmt.Sprintf("%.12f,%.12f", endPoint[0], endPoint[1])
		endNode := graph.Nodes[endNodeID]
		if endNode == nil {
			endNode = &Node{
				ID:    endNodeID,
				Point: endPoint,
				Edges: make([]*Edge, 0),
			}
			graph.Nodes[endNodeID] = endNode
		}

		// 计算边的成本（使用长度）
		cost := calculateLength(lineString)

		// 创建边（双向）
		edge := &Edge{
			ID:         i,
			From:       startNode,
			To:         endNode,
			LineString: lineString,
			Cost:       cost,
		}
		startNode.Edges = append(startNode.Edges, edge)

		// 反向边
		reverseEdge := &Edge{
			ID:         i,
			From:       endNode,
			To:         startNode,
			LineString: reverseLineString(lineString),
			Cost:       cost,
		}
		endNode.Edges = append(endNode.Edges, reverseEdge)

		graph.Edges = append(graph.Edges, edge)
	}

	return graph
}

// calculateLength 计算线段长度（使用 Haversine 公式）
func calculateLength(ls orb.LineString) float64 {
	length := 0.0
	for i := 0; i < len(ls)-1; i++ {
		length += haversineDistance(ls[i], ls[i+1])
	}
	return length
}

// haversineDistance 计算两点之间的距离（米）
func haversineDistance(p1, p2 orb.Point) float64 {
	const earthRadius = 6371000 // 地球半径（米）

	lat1 := p1[1] * math.Pi / 180
	lat2 := p2[1] * math.Pi / 180
	deltaLat := (p2[1] - p1[1]) * math.Pi / 180
	deltaLon := (p2[0] - p1[0]) * math.Pi / 180

	a := math.Sin(deltaLat/2)*math.Sin(deltaLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*
			math.Sin(deltaLon/2)*math.Sin(deltaLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRadius * c
}

// reverseLineString 反转线段
func reverseLineString(ls orb.LineString) orb.LineString {
	reversed := make(orb.LineString, len(ls))
	for i, j := 0, len(ls)-1; i < len(ls); i, j = i+1, j-1 {
		reversed[i] = ls[j]
	}
	return reversed
}

// findExactNode 找到与给定点精确匹配的节点
func findExactNode(graph *Graph, point orb.Point) *Node {
	nodeID := fmt.Sprintf("%.12f,%.12f", point[0], point[1])

	// 先尝试精确匹配
	if node, exists := graph.Nodes[nodeID]; exists {
		return node
	}

	// 如果没有精确匹配，找最近的节点（容差范围内）
	const tolerance = 0.1 // 约 0.1 米
	var nearestNode *Node
	minDist := math.MaxFloat64

	for _, node := range graph.Nodes {
		dist := haversineDistance(node.Point, point)
		if dist < minDist {
			minDist = dist
			nearestNode = node
		}
	}

	// 如果距离在容差范围内，认为是同一个点
	if minDist < tolerance {
		return nearestNode
	}

	return nil
}

// findNearestNode 找到距离给定点最近的节点（考虑线段上的投影点）
func findNearestNode(graph *Graph, point []float64) *Node {
	var nearestNode *Node
	minDist := math.MaxFloat64
	targetPoint := orb.Point{point[0], point[1]}

	// 遍历所有边，找到最近的投影点
	for _, edge := range graph.Edges {
		// 计算目标点到线段的最近点
		closestPoint := findClosestPointOnLineString(edge.LineString, targetPoint)
		dist := haversineDistance(closestPoint, targetPoint)

		if dist < minDist {
			minDist = dist
			// 判断最近点更靠近起点还是终点
			distToStart := haversineDistance(closestPoint, edge.From.Point)
			distToEnd := haversineDistance(closestPoint, edge.To.Point)

			if distToStart < distToEnd {
				nearestNode = edge.From
			} else {
				nearestNode = edge.To
			}
		}
	}

	return nearestNode
}

// findClosestPointOnLineString 找到线段上距离目标点最近的点
func findClosestPointOnLineString(ls orb.LineString, target orb.Point) orb.Point {
	if len(ls) == 0 {
		return orb.Point{}
	}

	const intersectionThreshold = 0.5 // 0.2米阈值

	// 第一步：查找最近的线段顶点（交点）
	var nearestVertex orb.Point
	minVertexDist := math.MaxFloat64
	hasValidVertex := false

	for _, vertex := range ls {
		dist := haversineDistance(vertex, target)
		if dist < minVertexDist {
			minVertexDist = dist
			nearestVertex = vertex
			hasValidVertex = true
		}
	}

	// 如果最近的顶点距离在阈值内，优先使用顶点
	if hasValidVertex && minVertexDist <= intersectionThreshold {
		return nearestVertex
	}

	// 第二步：如果没有合适的顶点，使用传统的垂足投影方法
	var closestPoint orb.Point
	minDist := math.MaxFloat64

	// 遍历线段的每一段
	for i := 0; i < len(ls)-1; i++ {
		p1 := ls[i]
		p2 := ls[i+1]

		// 计算目标点到当前线段的投影点
		projPoint := projectPointToSegment(target, p1, p2)
		dist := haversineDistance(projPoint, target)

		if dist < minDist {
			minDist = dist
			closestPoint = projPoint
		}
	}

	return closestPoint
}

// projectPointToSegment 计算点到线段的投影点（垂足点）
func projectPointToSegment(point, segStart, segEnd orb.Point) orb.Point {
	// 将地理坐标转换为近似的笛卡尔坐标进行计算
	// 注意：这是简化计算，对于小范围内的计算足够精确

	x := point[0]
	y := point[1]
	x1 := segStart[0]
	y1 := segStart[1]
	x2 := segEnd[0]
	y2 := segEnd[1]

	// 线段向量
	dx := x2 - x1
	dy := y2 - y1

	// 如果线段退化为点
	if dx == 0 && dy == 0 {
		return segStart
	}

	// 计算投影参数 t
	// t = ((point - segStart) · (segEnd - segStart)) / |segEnd - segStart|²
	t := ((x-x1)*dx + (y-y1)*dy) / (dx*dx + dy*dy)

	// 限制 t 在 [0, 1] 范围内
	// t < 0: 投影点在起点之前，返回起点
	// t > 1: 投影点在终点之后，返回终点
	// 0 <= t <= 1: 投影点在线段上
	if t < 0 {
		return segStart
	} else if t > 1 {
		return segEnd
	}

	// 计算投影点坐标
	projX := x1 + t*dx
	projY := y1 + t*dy

	return orb.Point{projX, projY}
}

// dijkstra 实现 Dijkstra 最短路径算法
func dijkstra(graph *Graph, start, end *Node) []*Edge {
	dist := make(map[string]float64)
	prev := make(map[string]*Edge)
	visited := make(map[string]bool)

	// 初始化距离
	for id := range graph.Nodes {
		dist[id] = math.MaxFloat64
	}
	dist[start.ID] = 0

	// 优先队列
	pq := &PriorityQueue{}
	heap.Init(pq)
	heap.Push(pq, &Item{node: start, priority: 0})

	for pq.Len() > 0 {
		item := heap.Pop(pq).(*Item)
		current := item.node

		if visited[current.ID] {
			continue
		}
		visited[current.ID] = true

		if current.ID == end.ID {
			break
		}

		// 检查所有相邻边
		for _, edge := range current.Edges {
			neighbor := edge.To
			if visited[neighbor.ID] {
				continue
			}

			newDist := dist[current.ID] + edge.Cost
			if newDist < dist[neighbor.ID] {
				dist[neighbor.ID] = newDist
				prev[neighbor.ID] = edge
				heap.Push(pq, &Item{node: neighbor, priority: newDist})
			}
		}
	}

	// 重建路径
	if dist[end.ID] == math.MaxFloat64 {
		return nil // 没有找到路径
	}

	path := make([]*Edge, 0)
	for nodeID := end.ID; nodeID != start.ID; {
		edge := prev[nodeID]
		if edge == nil {
			break
		}
		path = append([]*Edge{edge}, path...) // 前置插入
		nodeID = edge.From.ID
	}

	return path
}

// 优先队列实现
type Item struct {
	node     *Node
	priority float64
	index    int
}

type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

// 辅助函数：将 FeatureCollection 转换为 WKT
func (s *TrackService) featureCollectionToWKT(fc geojson.FeatureCollection) string {
	if len(fc.Features) == 0 {
		return ""
	}

	// 假设第一个 feature 是 bbox 多边形
	feature := fc.Features[0]
	if feature.Geometry == nil {
		return ""
	}

	switch geom := feature.Geometry.(type) {
	case orb.Polygon:
		if len(geom) == 0 || len(geom[0]) == 0 {
			return ""
		}
		coords := geom[0]
		wkt := "POLYGON(("
		for i, coord := range coords {
			if i > 0 {
				wkt += ","
			}
			wkt += fmt.Sprintf("%f %f", coord[0], coord[1])
		}
		wkt += "))"
		return wkt

	case orb.MultiPolygon:
		if len(geom) == 0 ||
			len(geom[0]) == 0 ||
			len(geom[0][0]) == 0 {
			return ""
		}
		coords := geom[0][0]
		wkt := "POLYGON(("
		for i, coord := range coords {
			if i > 0 {
				wkt += ","
			}
			wkt += fmt.Sprintf("%f %f", coord[0], coord[1])
		}
		wkt += "))"
		return wkt
	}

	return ""
}

// FindNearestPointOnLines 修改签名，添加 maxDistance 参数
func (s *TrackService) FindNearestPointOnLines(
	ctx context.Context,
	linesGeoJSON *geojson.FeatureCollection,
	targetPoint []float64,
	maxDistance float64, // 新增参数：最大捕捉距离(米)，0表示不限制
) (snappedPoint []float64, distance float64, lineID int, err error) {
	if linesGeoJSON == nil || len(linesGeoJSON.Features) == 0 {
		return nil, 0, 0, fmt.Errorf("no line segments available")
	}

	if len(targetPoint) < 2 {
		return nil, 0, 0, fmt.Errorf("invalid target point")
	}

	target := orb.Point{targetPoint[0], targetPoint[1]}
	var nearestPoint orb.Point
	minDistance := math.MaxFloat64
	nearestLineID := -1

	// 遍历所有线段找最近点
	for _, feature := range linesGeoJSON.Features {
		if feature.Geometry == nil {
			continue
		}

		lineString, ok := feature.Geometry.(orb.LineString)
		if !ok || len(lineString) < 2 {
			continue
		}

		// 获取线段ID
		var featureID int
		if id, ok := feature.Properties["id"].(float64); ok {
			featureID = int(id)
		} else if id, ok := feature.Properties["id"].(int); ok {
			featureID = id
		}

		// 找到线段上的最近点
		closestPoint := findClosestPointOnLineString(lineString, target)
		dist := haversineDistance(closestPoint, target)

		if dist < minDistance {
			minDistance = dist
			nearestPoint = closestPoint
			nearestLineID = featureID
		}
	}

	if nearestLineID == -1 {
		return nil, 0, 0, fmt.Errorf("no nearest point found")
	}

	// 新增：检查距离阈值
	if maxDistance > 0 && minDistance > maxDistance {
		// 超过阈值，返回原始点
		return targetPoint, minDistance, -1, nil
	}

	return []float64{nearestPoint[0], nearestPoint[1]}, minDistance, nearestLineID, nil
}
