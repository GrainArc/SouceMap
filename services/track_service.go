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
	// 构建 bbox 的 WKT
	bboxWKT := s.featureCollectionToWKT(bbox)
	if bboxWKT == "" {
		return nil, fmt.Errorf("invalid bbox")
	}

	// 构建 UNION 查询获取所有图层的几何
	var unionQueries []string
	for _, layerName := range layerNames {
		unionQueries = append(unionQueries, fmt.Sprintf(`
			SELECT geom, '%s' as layer
			FROM %s
			WHERE ST_Intersects(geom, ST_GeomFromText($1, 4326))
		`, layerName, layerName))
	}

	query := fmt.Sprintf(`
		WITH all_geoms AS (
			%s
		),
		dumped AS (
			-- 展开多几何为单几何
			SELECT layer, (ST_Dump(geom)).geom as geom
			FROM all_geoms
		),
		as_lines AS (
			-- 将所有几何转换为线
			SELECT 
				layer,
				CASE 
					WHEN ST_GeometryType(geom) = 'ST_LineString' THEN geom
					WHEN ST_GeometryType(geom) = 'ST_Polygon' THEN ST_Boundary(geom)
					WHEN ST_GeometryType(geom) = 'ST_MultiLineString' THEN geom
					WHEN ST_GeometryType(geom) = 'ST_MultiPolygon' THEN ST_Boundary(geom)
					ELSE NULL
				END as geom
			FROM dumped
			WHERE ST_GeometryType(geom) != 'ST_Point'
		),
		exploded_lines AS (
			-- 展开为单线
			SELECT layer, (ST_Dump(geom)).geom as geom
			FROM as_lines
			WHERE geom IS NOT NULL
		),
		noded_collection AS (
			-- 跨图层打断：收集所有线并统一节点化
			SELECT ST_Node(ST_Collect(geom)) as noded_geom
			FROM exploded_lines
		),
		noded_lines AS (
			-- 展开节点化后的几何,并关联回原始图层
			SELECT 
				row_number() OVER () as id,
				dumped_noded.geom,
				-- 找到与该几何相交的原始图层（可能有多个）
				(
					SELECT string_agg(DISTINCT el.layer, ',')
					FROM exploded_lines el
					WHERE ST_Intersects(el.geom, dumped_noded.geom)
				) as layer
			FROM noded_collection,
			LATERAL (SELECT (ST_Dump(noded_geom)).geom) AS dumped_noded(geom)
		)
		SELECT 
			id,
			layer,
			ST_AsGeoJSON(geom, 15)::json as geom_json,  -- 设置精度为15位小数
			ST_Length(geom::geography) as length
		FROM noded_lines
		WHERE ST_GeometryType(geom) = 'ST_LineString'
		  AND ST_Length(geom) > 0
	`, strings.Join(unionQueries, "\n\t\tUNION ALL\n\t\t"))

	var geometries []struct {
		ID       int             `gorm:"column:id"`
		Layer    string          `gorm:"column:layer"`
		GeomJSON json.RawMessage `gorm:"column:geom_json"`
		Length   float64         `gorm:"column:length"`
	}

	if err := models.DB.WithContext(ctx).Raw(query, bboxWKT).Scan(&geometries).Error; err != nil {
		return nil, fmt.Errorf("failed to get and break geometries: %w", err)
	}

	fc := geojson.NewFeatureCollection()

	// 将几何添加到 FeatureCollection
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
		feature.Properties["layer"] = geom.Layer
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

	// 添加路径中的每条边
	totalCost := 0.0
	for i, edge := range path {
		feature := geojson.NewFeature(edge.LineString)
		totalCost += edge.Cost
		feature.Properties = geojson.Properties{
			"seq":      i + 1,
			"edge_id":  edge.ID,
			"cost":     edge.Cost,
			"agg_cost": totalCost,
		}
		resultCollection.Features = append(resultCollection.Features, feature)
	}

	return resultCollection, nil
}

// SplitResult 存储线段打断的结果
type SplitResult struct {
	Feature      *geojson.Feature
	SplitPoint   orb.Point
	SegmentIndex int     // 在哪个线段上打断
	T            float64 // 投影参数
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

	// 记录需要打断的线段索引
	splitIndices := make(map[int][]SplitResult)

	// 记录起点线段的分割信息
	startFeatureIdx := -1
	for i, feature := range fc.Features {
		if feature == startSplit.Feature {
			startFeatureIdx = i
			break
		}
	}
	if startFeatureIdx >= 0 {
		splitIndices[startFeatureIdx] = append(splitIndices[startFeatureIdx], *startSplit)
	}

	// 记录终点线段的分割信息
	endFeatureIdx := -1
	for i, feature := range fc.Features {
		if feature == endSplit.Feature {
			endFeatureIdx = i
			break
		}
	}
	if endFeatureIdx >= 0 {
		// 如果起点和终点在同一条线段上，需要特殊处理
		if startFeatureIdx == endFeatureIdx {
			splitIndices[endFeatureIdx] = append(splitIndices[endFeatureIdx], *endSplit)
		} else {
			splitIndices[endFeatureIdx] = append(splitIndices[endFeatureIdx], *endSplit)
		}
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
		// 按 T 值排序分割点
		sort.Slice(splits, func(i, j int) bool {
			return splits[i].T < splits[j].T
		})

		// 执行打断
		splitLines := splitLineString(lineString, splits)

		// 添加打断后的线段
		for _, splitLine := range splitLines {
			if len(splitLine) < 2 {
				continue
			}
			newFeature := geojson.NewFeature(splitLine)
			// 复制原始属性
			if feature.Properties != nil {
				newFeature.Properties = feature.Properties
			}
			newFC.Features = append(newFC.Features, newFeature)
		}
	}

	return newFC, startSplit.SplitPoint, endSplit.SplitPoint
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
	const tolerance = 0.000001 // 约 0.1 米
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
