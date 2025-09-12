package Tin

import (
	"fmt"
	"math"
)

// 在原有代码基础上添加以下函数

// 计算向量叉积的Z分量（用于判断点在三角形内部）
func crossProductZ(p1, p2, p3 *Point3D) float64 {
	return (p2.X-p1.X)*(p3.Y-p1.Y) - (p3.X-p1.X)*(p2.Y-p1.Y)
}

// 判断二维点是否在三角形内部（基于重心坐标）
func pointInTriangle(px, py float64, t *Triangle3D) bool {
	// 使用重心坐标判断点是否在三角形内
	x1, y1 := t.P1.X, t.P1.Y
	x2, y2 := t.P2.X, t.P2.Y
	x3, y3 := t.P3.X, t.P3.Y

	// 计算重心坐标
	denominator := (y2-y3)*(x1-x3) + (x3-x2)*(y1-y3)
	if math.Abs(denominator) < 1e-10 {
		return false // 三角形退化
	}

	a := ((y2-y3)*(px-x3) + (x3-x2)*(py-y3)) / denominator
	b := ((y3-y1)*(px-x3) + (x1-x3)*(py-y3)) / denominator
	c := 1 - a - b

	// 点在三角形内当且仅当所有重心坐标都非负
	return a >= 0 && b >= 0 && c >= 0
}

// 使用重心坐标在三角形内插值高程
func interpolateElevationInTriangle(px, py float64, t *Triangle3D) float64 {
	x1, y1, z1 := t.P1.X, t.P1.Y, t.P1.Z
	x2, y2, z2 := t.P2.X, t.P2.Y, t.P2.Z
	x3, y3, z3 := t.P3.X, t.P3.Y, t.P3.Z

	// 计算重心坐标
	denominator := (y2-y3)*(x1-x3) + (x3-x2)*(y1-y3)
	if math.Abs(denominator) < 1e-10 {
		// 三角形退化，返回平均高程
		return (z1 + z2 + z3) / 3.0
	}

	a := ((y2-y3)*(px-x3) + (x3-x2)*(py-y3)) / denominator
	b := ((y3-y1)*(px-x3) + (x1-x3)*(py-y3)) / denominator
	c := 1 - a - b

	// 使用重心坐标插值高程
	return a*z1 + b*z2 + c*z3
}

// 获取二维点在TIN上的投影高程
func (tin *TIN3D) GetElevationAt(x, y float64) (float64, error) {
	// 遍历所有三角形，找到包含该点的三角形
	for _, triangle := range tin.Triangles {
		if pointInTriangle(x, y, triangle) {
			elevation := interpolateElevationInTriangle(x, y, triangle)
			return elevation, nil
		}
	}

	// 如果没有找到包含该点的三角形，返回错误
	return 0, fmt.Errorf("point (%.2f, %.2f) is not inside any triangle of the TIN", x, y)
}

// 批量获取多个点的高程
func (tin *TIN3D) GetElevationsAt(points []Point2D) ([]float64, error) {
	elevations := make([]float64, len(points))

	for i, point := range points {
		elevation, err := tin.GetElevationAt(point.X, point.Y)
		if err != nil {
			return nil, fmt.Errorf("failed to get elevation for point %d: %v", i, err)
		}
		elevations[i] = elevation
	}

	return elevations, nil
}

// 获取指定区域内的高程网格
func (tin *TIN3D) GetElevationGrid(minX, minY, maxX, maxY float64, stepX, stepY float64) ([][]float64, error) {
	if stepX <= 0 || stepY <= 0 {
		return nil, fmt.Errorf("step size must be positive")
	}

	// 计算网格尺寸
	nx := int(math.Ceil((maxX-minX)/stepX)) + 1
	ny := int(math.Ceil((maxY-minY)/stepY)) + 1

	// 初始化网格
	grid := make([][]float64, ny)
	for i := range grid {
		grid[i] = make([]float64, nx)
	}

	// 填充网格
	for i := 0; i < ny; i++ {
		y := minY + float64(i)*stepY
		for j := 0; j < nx; j++ {
			x := minX + float64(j)*stepX

			elevation, err := tin.GetElevationAt(x, y)
			if err != nil {
				// 如果点在TIN外部，可以使用NaN或者最近点插值
				grid[i][j] = math.NaN()
			} else {
				grid[i][j] = elevation
			}
		}
	}

	return grid, nil
}

// 计算指定点的坡度和坡向
func (tin *TIN3D) GetSlopeAndAspect(x, y float64, delta float64) (slope, aspect float64, err error) {
	if delta <= 0 {
		delta = 0.1 // 默认采样间距
	}

	// 获取中心点高程
	z0, err := tin.GetElevationAt(x, y)
	if err != nil {
		return 0, 0, err
	}

	// 获取周围4个点的高程
	zE, errE := tin.GetElevationAt(x+delta, y)
	zW, errW := tin.GetElevationAt(x-delta, y)
	zN, errN := tin.GetElevationAt(x, y+delta)
	zS, errS := tin.GetElevationAt(x, y-delta)

	// 如果边界点无法获取，使用中心点高程
	if errE != nil {
		zE = z0
	}
	if errW != nil {
		zW = z0
	}
	if errN != nil {
		zN = z0
	}
	if errS != nil {
		zS = z0
	}

	// 计算梯度
	dzdx := (zE - zW) / (2 * delta)
	dzdy := (zN - zS) / (2 * delta)

	// 计算坡度（弧度）
	slope = math.Atan(math.Sqrt(dzdx*dzdx + dzdy*dzdy))

	// 计算坡向（弧度，从北方向顺时针）
	if dzdx == 0 && dzdy == 0 {
		aspect = 0 // 平地
	} else {
		aspect = math.Atan2(dzdx, dzdy)
		if aspect < 0 {
			aspect += 2 * math.Pi
		}
	}

	return slope, aspect, nil
}
