package views

import (
	"bytes"
	"encoding/json"
	"fmt"
	Tin2 "github.com/GrainArc/SouceMap/Tin"
	"github.com/GrainArc/SouceMap/Transformer"
	"github.com/GrainArc/SouceMap/config"
	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/models"
	"github.com/GrainArc/SouceMap/pgmvt"
	"github.com/chai2010/webp"
	"github.com/gin-gonic/gin"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"image"
	"image/color"
	"log"
	"math"
	"net/http"
	"strings"
	"sync"
)

func GetRGBToDEM(lon float64, lat float64, ResultTiles []models.Tile, maxZoom int64) float64 {

	x1, y1 := pgmvt.LonLatToTile(lon, lat, maxZoom)
	var realDem models.Tile
	for _, item := range ResultTiles {
		if item.TileRow == y1 && item.TileColumn == x1 {
			realDem = item
		}
	}
	// 自动检测图片格式
	img, format, err := decodeImage(realDem.TileData)
	if err != nil {
		log.Printf("图片解码失败（格式：%s）：%v", format, err)
		return 0
	}

	// 计算lat，lon在该照片上的相对坐标x,y
	//计算所在瓦片的经纬度坐标
	topLeft, _, bottomLeft, bottomRight := pgmvt.TileToLatLon(int(maxZoom), int(realDem.TileColumn), int(realDem.TileRow))

	// 计算瓦片经纬度范围
	tileLeft := topLeft[0]
	tileRight := bottomRight[0]
	tileTop := topLeft[1]
	tileBottom := bottomLeft[1]
	lonRatio := (lon - tileLeft) / (tileRight - tileLeft)
	latRatio := (tileTop - lat) / (tileTop - tileBottom)

	// 获取图片实际尺寸
	bounds := img.Bounds()
	width := bounds.Max.X - bounds.Min.X
	height := bounds.Max.Y - bounds.Min.Y

	// 计算像素坐标（考虑边界溢出）
	x := int(math.Floor(float64(width) * lonRatio))
	y := int(math.Floor(float64(height) * latRatio))

	// 检查坐标范围
	bounds2 := img.Bounds()
	if x < bounds2.Min.X || x >= bounds2.Max.X || y < bounds2.Min.Y || y >= bounds2.Max.Y {

		return 0
	}

	// 获取颜色值（统一处理）
	r, g, b := getRGB(img.At(x, y))

	// Mapbox DEM高程计算公式（优化版）
	height2 := calculateElevation(r, g, b)
	return height2
}

// 自动检测并解码图片
func decodeImage(data []byte) (image.Image, string, error) {
	// 先尝试WebP解码（Mapbox常用）
	if img, err := webp.Decode(bytes.NewReader(data)); err == nil {
		return img, "webp", nil
	}

	// 再尝试PNG解码
	if img, format, err := image.Decode(bytes.NewReader(data)); err == nil {
		return img, format, nil
	}

	return nil, "unknown", fmt.Errorf("无法识别的图片格式")
}

// 统一获取RGB值
func getRGB(c color.Color) (r, g, b uint8) {
	switch color := c.(type) {
	case color.NRGBA:
		return color.R, color.G, color.B
	case color.RGBA:
		return color.R, color.G, color.B
	default:
		r32, g32, b32, _ := c.RGBA()
		return uint8(r32 >> 8), uint8(g32 >> 8), uint8(b32 >> 8)
	}
}

// 高程计算公式（Mapbox官方算法）
func calculateElevation(r, g, b uint8) float64 {
	// 公式：height = (R * 256² + G * 256 + B) * 0.1 - 10000
	return (float64(r)*65536+float64(g)*256+float64(b))*0.1 - 10000
}

// 填挖方接口

type FillData struct {
	SJMList [][]float64
	Geojson geojson.FeatureCollection
}

func (uc *UserController) GetExcavationFillVolume(c *gin.Context) {
	var jsonData FillData
	c.BindJSON(&jsonData) //将前端geojson转换为geo对象
	type efData struct {
		Excavation float64
		Fill       float64
	}
	tile := pgmvt.Bounds(jsonData.Geojson.Features[0].Geometry)

	DB, err := gorm.Open(sqlite.Open(config.Dem), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		c.JSON(http.StatusOK, efData{
			Excavation: 0,
			Fill:       0,
		})
		return
	}
	defer func() {
		if DB, err := DB.DB(); err == nil {
			DB.Close()
		}
	}()
	//获取最大层级
	var maxZoom int64
	DB.Model(&models.Tile{}).Select("MAX(zoom_level)").Scan(&maxZoom)
	//保留最大层级的tile
	newtile := pgmvt.KeepTile(maxZoom, tile)

	//向数据库请求需要的瓦片
	area := methods.CalculateArea(jsonData.Geojson.Features[0])

	var conditions []string
	var args []interface{}
	for _, t := range newtile {
		conditions = append(conditions, "(tile_column = ? AND tile_row = ? AND zoom_level = ?)")
		args = append(args, t.X, t.Y, t.Z)
	}

	// 执行批量查询
	var ResultTiles []models.Tile
	DB.Where(strings.Join(conditions, " OR "), args...).Find(&ResultTiles)
	//获取某个点在

	//构造三维点字符串
	pointz1 := jsonData.SJMList

	//构造TIN
	geo := Transformer.GetGeometryString(jsonData.Geojson.Features[0])

	coords, P_area := GetTilesPoints(geo, area)

	Excavation := 0.00
	Fill := 0.00
	Pg2D, _ := Tin2.GeometryStringToPolygon2D(geo)
	Pts3D, _ := Tin2.CoordsToPoint3D(pointz1)
	Tin := Tin2.CreateTIN3D(Pg2D, Pts3D)

	coords2 := MakeZList(coords, Tin)

	var wg sync.WaitGroup
	results := make(chan float64, 10)

	for _, coord := range coords2 {
		if coord[2] != 0 {
			wg.Add(1)
			go func(x, y, z float64) {
				defer wg.Done()
				ZSBG := GetRGBToDEM(x, y, ResultTiles, maxZoom)
				volume := (z - ZSBG) * P_area
				results <- volume
			}(coord[0], coord[1], coord[2])
		}

	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for volume := range results {
		if volume < 0 {
			Excavation += volume
		} else {
			Fill += volume
		}
	}

	var result efData
	result.Excavation = math.Abs(Excavation)
	result.Fill = math.Abs(Fill)
	c.JSON(http.StatusOK, result)

}

func GetTilesPoints(geo string, area float64) ([][]float64, float64) {
	var size float64
	if area >= 7000 {

		size2 := math.Sqrt(area / (7000 * 12321000000))
		size = math.Round(size2*1000000) / 1000000 // 保留两位小数

	} else {
		size = 0.00001
	}

	polygon_4490 := fmt.Sprintf(`{
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            104,
                            30
                        ],
                        [
                            %f,
                            %f
                        ],
                        [
                            %f,
                            %f
                        ],
                        [
                            %f,
                            %f
                        ],
                        [
                            104,
                            30
                        ]
                    ]
                ]
            }`, 104.00+size, 30.00, 104.00+size, 30.00+size, 104.00, 30.00+size)
	polygon_4490area := methods.CalculateAreaByStr(polygon_4490)
	sql := fmt.Sprintf(`
	WITH input_geom AS (
  SELECT ST_SetSRID(ST_GeomFromGeoJSON('%s'),4326) AS geom
),
tiles AS (
  SELECT grid.tile
  FROM input_geom AS i,
       LATERAL ST_SquareGrid(%f, ST_Expand(i.geom, %f)) AS grid(tile)
  WHERE ST_Intersects(grid.tile, i.geom)
),
centroids AS (
  SELECT 
    ST_Centroid(tile) AS center,
    ST_Y(ST_Centroid(tile)) AS lat,
    ST_X(ST_Centroid(tile)) AS lng
  FROM tiles
)
SELECT ST_AsGeoJSON(ST_Union(center)) as geojson FROM centroids
`, geo, size, size)
	type geometryData struct {
		GeoJSON []byte `gorm:"column:geojson"` // 假设数据库中存储geojson的列名为"geojson"

	}
	var geomData geometryData
	GeoDB := models.DB
	err := GeoDB.Raw(sql).Scan(&geomData)

	//  如果执行数据库操作时发生错误，向客户端返回错误信息
	if err.Error != nil {
		fmt.Println(err.Error)
		//  提前返回，防止进一步执行
	}
	var geo2 geojson.Geometry
	json.Unmarshal(geomData.GeoJSON, &geo2)

	var coords [][]float64
	switch geom := geo2.Geometry().(type) {
	case orb.Point:
		coords = append(coords, []float64{geom.Lon(), geom.Lat()})
	case orb.MultiPoint:
		for _, pt := range geom {
			coords = append(coords, []float64{pt.Lon(), pt.Lat()})
		}
	}
	return coords, polygon_4490area
}

func MakeZList(coords [][]float64, T *Tin2.TIN3D) [][]float64 {
	var coords2 = make([][]float64, len(coords))

	type task struct {
		index int
		x     float64
		y     float64
	}

	type result struct {
		index int
		z     float64
	}

	// 创建带缓冲的channel（缓冲大小根据任务量调整）
	tasks := make(chan task, len(coords))
	results := make(chan result, len(coords))

	// 创建固定数量的worker
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range tasks {
				z, _ := T.GetElevationAt(t.x, t.y)

				results <- result{t.index, z}

			}
		}()
	}

	// 发送任务
	go func() {
		for i, item := range coords {
			tasks <- task{index: i, x: item[0], y: item[1]}
		}
		close(tasks)
	}()

	// 等待所有worker完成并关闭结果channel
	go func() {
		wg.Wait()
		close(results)
	}()

	// 收集结果（自动保持原始顺序）
	for res := range results {
		coords2[res.index] = []float64{coords[res.index][0], coords[res.index][1], res.z}
	}
	return coords2

}

type Point struct {
	X float64 `json:"x" binding:"required"` // X坐标（经度），必填字段，使用JSON标签和验证标签
	Y float64 `json:"y" binding:"required"` // Y坐标（纬度），必填字段，使用JSON标签和验证标签
}

func (uc *UserController) GetHeightFromDEM(c *gin.Context) {
	var jsonData Point
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		// 如果JSON绑定失败，返回400错误和详细错误信息
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format", // 错误类型说明
			"message": err.Error(),              // 具体错误详情
		})
		return // 提前返回，避免继续执行
	}

	tile := pgmvt.GetPointTile(jsonData.X, jsonData.Y)

	DB, err := gorm.Open(sqlite.Open(config.Dem), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"Height": -999,
		})
		return
	}
	defer func() {
		if DB, err := DB.DB(); err == nil {
			DB.Close()
		}
	}()
	//获取最大层级
	var maxZoom int64
	DB.Model(&models.Tile{}).Select("MAX(zoom_level)").Scan(&maxZoom)

	newtile := pgmvt.KeepTile(maxZoom, tile)[0]

	// 执行批量查询
	var ResultTiles []models.Tile

	DB.Where("(tile_column = ? AND tile_row = ? AND zoom_level = ?)", newtile.X, newtile.Y, newtile.Z).First(&ResultTiles)
	//保留最大层级的tile

	h := GetRGBToDEM(jsonData.X, jsonData.Y, ResultTiles, maxZoom)

	c.JSON(http.StatusOK, gin.H{
		"Height": h,
	})

}
