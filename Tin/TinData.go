package Tin

// Point3D 表示一个三维点
type Point3D struct {
	X, Y, Z float64
	ID      int
}

// Point2D 表示一个二维点
type Point2D struct {
	X, Y float64
	ID   int
}

// Triangle3D 表示一个三维三角形
type Triangle3D struct {
	P1, P2, P3 *Point3D
	ID         int
}

// Edge3D 表示一条三维边
type Edge3D struct {
	P1, P2 *Point3D
}

// Polygon2D 表示一个二维多边形面
type Polygon2D struct {
	Points []*Point2D
}

// TIN3D 三维三角不规则网络
type TIN3D struct {
	Points    []*Point3D
	Triangles []*Triangle3D
	Edges     []*Edge3D
}
