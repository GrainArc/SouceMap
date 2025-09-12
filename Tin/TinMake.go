package Tin

import (
	"fmt"
	"math"
)

// 计算三维点到二维点的水平距离
func distance3DTo2D(p3d *Point3D, p2d *Point2D) float64 {
	dx := p3d.X - p2d.X
	dy := p3d.Y - p2d.Y
	return math.Sqrt(dx*dx + dy*dy)
}

// 计算两个三维点间距离
func distance3D(p1, p2 *Point3D) float64 {
	dx := p1.X - p2.X
	dy := p1.Y - p2.Y
	return math.Sqrt(dx*dx + dy*dy)
}

// 找到距离二维点最近的三维点，返回其高程值
func findNearestElevation(point2D *Point2D, points3D []*Point3D) float64 {
	if len(points3D) == 0 {
		return 0.0
	}

	minDistance := math.Inf(1)
	nearestZ := points3D[0].Z

	for _, p3d := range points3D {
		dist := distance3DTo2D(p3d, point2D)
		if dist < minDistance {
			minDistance = dist
			nearestZ = p3d.Z
		}
	}

	return nearestZ
}

// 计算三角形外接圆圆心和半径（基于XY平面投影）
func circumcircle3D(p1, p2, p3 *Point3D) (cx, cy, r float64) {
	ax, ay := p1.X, p1.Y
	bx, by := p2.X, p2.Y
	cx1, cy1 := p3.X, p3.Y

	d := 2 * (ax*(by-cy1) + bx*(cy1-ay) + cx1*(ay-by))
	if math.Abs(d) < 1e-10 {
		return 0, 0, math.Inf(1)
	}

	ux := (ax*ax+ay*ay)*(by-cy1) + (bx*bx+by*by)*(cy1-ay) + (cx1*cx1+cy1*cy1)*(ay-by)
	uy := (ax*ax+ay*ay)*(cx1-bx) + (bx*bx+by*by)*(ax-cx1) + (cx1*cx1+cy1*cy1)*(bx-ax)

	cx = ux / d
	cy = uy / d
	r = math.Sqrt((cx-ax)*(cx-ax) + (cy-ay)*(cy-ay))

	return cx, cy, r
}

// 判断点是否在三角形外接圆内（基于XY投影）
func inCircumcircle3D(p *Point3D, t *Triangle3D) bool {
	cx, cy, r := circumcircle3D(t.P1, t.P2, t.P3)
	if math.IsInf(r, 1) {
		return false
	}
	dist := math.Sqrt((p.X-cx)*(p.X-cx) + (p.Y-cy)*(p.Y-cy))
	return dist < r
}

// 创建超级三角形
func createSuperTriangle3D(points []*Point3D) *Triangle3D {
	minX, minY := math.Inf(1), math.Inf(1)
	maxX, maxY := math.Inf(-1), math.Inf(-1)

	for _, p := range points {
		if p.X < minX {
			minX = p.X
		}
		if p.X > maxX {
			maxX = p.X
		}
		if p.Y < minY {
			minY = p.Y
		}
		if p.Y > maxY {
			maxY = p.Y
		}
	}

	dx := maxX - minX
	dy := maxY - minY
	deltaMax := math.Max(dx, dy)
	midX := (minX + maxX) / 2
	midY := (minY + maxY) / 2

	p1 := &Point3D{X: midX - 20*deltaMax, Y: midY - deltaMax, Z: 0, ID: -1}
	p2 := &Point3D{X: midX, Y: midY + 20*deltaMax, Z: 0, ID: -2}
	p3 := &Point3D{X: midX + 20*deltaMax, Y: midY - deltaMax, Z: 0, ID: -3}

	return &Triangle3D{P1: p1, P2: p2, P3: p3, ID: -1}
}

// Delaunay三角剖分
func delaunayTriangulation3D(points []*Point3D) []*Triangle3D {
	if len(points) < 3 {
		return nil
	}

	superTriangle := createSuperTriangle3D(points)
	triangles := []*Triangle3D{superTriangle}

	for _, point := range points {
		var badTriangles []*Triangle3D

		// 找到包含当前点的外接圆的三角形
		for _, triangle := range triangles {
			if inCircumcircle3D(point, triangle) {
				badTriangles = append(badTriangles, triangle)
			}
		}

		// 找到多边形边界
		var polygon []*Edge3D
		for _, badTriangle := range badTriangles {
			edges := []*Edge3D{
				{badTriangle.P1, badTriangle.P2},
				{badTriangle.P2, badTriangle.P3},
				{badTriangle.P3, badTriangle.P1},
			}

			for _, edge := range edges {
				shared := false
				for _, otherBadTriangle := range badTriangles {
					if otherBadTriangle == badTriangle {
						continue
					}
					otherEdges := []*Edge3D{
						{otherBadTriangle.P1, otherBadTriangle.P2},
						{otherBadTriangle.P2, otherBadTriangle.P3},
						{otherBadTriangle.P3, otherBadTriangle.P1},
					}

					for _, otherEdge := range otherEdges {
						if (edge.P1 == otherEdge.P1 && edge.P2 == otherEdge.P2) ||
							(edge.P1 == otherEdge.P2 && edge.P2 == otherEdge.P1) {
							shared = true
							break
						}
					}
					if shared {
						break
					}
				}
				if !shared {
					polygon = append(polygon, edge)
				}
			}
		}

		// 移除坏三角形
		var newTriangles []*Triangle3D
		for _, triangle := range triangles {
			bad := false
			for _, badTriangle := range badTriangles {
				if triangle == badTriangle {
					bad = true
					break
				}
			}
			if !bad {
				newTriangles = append(newTriangles, triangle)
			}
		}
		triangles = newTriangles

		// 创建新三角形
		triangleID := len(triangles)
		for _, edge := range polygon {
			newTriangle := &Triangle3D{
				P1: edge.P1,
				P2: edge.P2,
				P3: point,
				ID: triangleID,
			}
			triangles = append(triangles, newTriangle)
			triangleID++
		}
	}

	// 移除包含超级三角形顶点的三角形
	var finalTriangles []*Triangle3D
	for _, triangle := range triangles {
		if triangle.P1.ID >= 0 && triangle.P2.ID >= 0 && triangle.P3.ID >= 0 {
			finalTriangles = append(finalTriangles, triangle)
		}
	}

	return finalTriangles
}

// 创建三维TIN
func CreateTIN3D(polygon *Polygon2D, points3D []*Point3D) *TIN3D {
	// 将二维多边形顶点转换为三维点（通过最近点插值获得高程）
	var polygonPoints3D []*Point3D
	for i, p2d := range polygon.Points {
		z := findNearestElevation(p2d, points3D)
		polygonPoints3D = append(polygonPoints3D, &Point3D{
			X: p2d.X, Y: p2d.Y, Z: z, ID: i,
		})
	}

	// 合并多边形顶点和输入的三维点
	allPoints3D := make([]*Point3D, 0, len(polygonPoints3D)+len(points3D))
	allPoints3D = append(allPoints3D, polygonPoints3D...)

	// 添加输入的三维点，重新分配ID避免冲突
	baseID := len(polygonPoints3D)
	for i, p := range points3D {
		newPoint := &Point3D{X: p.X, Y: p.Y, Z: p.Z, ID: baseID + i}
		allPoints3D = append(allPoints3D, newPoint)
	}

	// 执行Delaunay三角剖分
	triangles := delaunayTriangulation3D(allPoints3D)

	// 生成边
	edgeMap := make(map[string]*Edge3D)
	for _, triangle := range triangles {
		edges := []*Edge3D{
			{triangle.P1, triangle.P2},
			{triangle.P2, triangle.P3},
			{triangle.P3, triangle.P1},
		}

		for _, edge := range edges {
			key1 := fmt.Sprintf("%d-%d", edge.P1.ID, edge.P2.ID)
			key2 := fmt.Sprintf("%d-%d", edge.P2.ID, edge.P1.ID)

			if _, exists := edgeMap[key1]; !exists {
				if _, exists := edgeMap[key2]; !exists {
					edgeMap[key1] = edge
				}
			}
		}
	}

	var edges []*Edge3D
	for _, edge := range edgeMap {
		edges = append(edges, edge)
	}

	return &TIN3D{
		Points:    allPoints3D,
		Triangles: triangles,
		Edges:     edges,
	}
}

// 计算三角形面积（三维）
func (t *Triangle3D) Area() float64 {
	// 使用向量叉积计算面积
	v1 := &Point3D{
		X: t.P2.X - t.P1.X,
		Y: t.P2.Y - t.P1.Y,
		Z: t.P2.Z - t.P1.Z,
	}
	v2 := &Point3D{
		X: t.P3.X - t.P1.X,
		Y: t.P3.Y - t.P1.Y,
		Z: t.P3.Z - t.P1.Z,
	}

	// 叉积
	cross := &Point3D{
		X: v1.Y*v2.Z - v1.Z*v2.Y,
		Y: v1.Z*v2.X - v1.X*v2.Z,
		Z: v1.X*v2.Y - v1.Y*v2.X,
	}

	// 叉积的模长的一半就是三角形面积
	length := math.Sqrt(cross.X*cross.X + cross.Y*cross.Y + cross.Z*cross.Z)
	return length / 2.0
}

// 计算三角形法向量
func (t *Triangle3D) Normal() (float64, float64, float64) {
	v1 := &Point3D{
		X: t.P2.X - t.P1.X,
		Y: t.P2.Y - t.P1.Y,
		Z: t.P2.Z - t.P1.Z,
	}
	v2 := &Point3D{
		X: t.P3.X - t.P1.X,
		Y: t.P3.Y - t.P1.Y,
		Z: t.P3.Z - t.P1.Z,
	}

	// 叉积得到法向量
	nx := v1.Y*v2.Z - v1.Z*v2.Y
	ny := v1.Z*v2.X - v1.X*v2.Z
	nz := v1.X*v2.Y - v1.Y*v2.X

	// 归一化
	length := math.Sqrt(nx*nx + ny*ny + nz*nz)
	if length > 0 {
		nx /= length
		ny /= length
		nz /= length
	}

	return nx, ny, nz
}

// 打印TIN信息
func (tin *TIN3D) Print() {
	fmt.Printf("3D TIN Statistics:\n")
	fmt.Printf("Points: %d\n", len(tin.Points))
	fmt.Printf("Triangles: %d\n", len(tin.Triangles))
	fmt.Printf("Edges: %d\n", len(tin.Edges))

	// 计算高程统计
	if len(tin.Points) > 0 {
		minZ, maxZ := tin.Points[0].Z, tin.Points[0].Z
		var avgZ float64

		for _, p := range tin.Points {
			if p.Z < minZ {
				minZ = p.Z
			}
			if p.Z > maxZ {
				maxZ = p.Z
			}
			avgZ += p.Z
		}
		avgZ /= float64(len(tin.Points))

		fmt.Printf("Elevation - Min: %.2f, Max: %.2f, Avg: %.2f\n", minZ, maxZ, avgZ)
	}

	// 计算总面积
	var totalArea float64
	for _, triangle := range tin.Triangles {
		totalArea += triangle.Area()
	}
	fmt.Printf("Total Surface Area: %.2f\n", totalArea)

	fmt.Println("\nTriangles:")
	for i, triangle := range tin.Triangles {
		area := triangle.Area()
		fmt.Printf("Triangle %d: Area=%.2f\n", i, area)
		fmt.Printf("  P1: (%.2f,%.2f,%.2f)\n", triangle.P1.X, triangle.P1.Y, triangle.P1.Z)
		fmt.Printf("  P2: (%.2f,%.2f,%.2f)\n", triangle.P2.X, triangle.P2.Y, triangle.P2.Z)
		fmt.Printf("  P3: (%.2f,%.2f,%.2f)\n", triangle.P3.X, triangle.P3.Y, triangle.P3.Z)
	}
}
