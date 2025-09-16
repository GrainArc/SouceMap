package methods

import (
	"fmt"
	"github.com/GrainArc/SouceMap/Transformer"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/yofu/dxf"
	"github.com/yofu/dxf/color"
	"github.com/yofu/dxf/entity"
	"log"
)

func ConvertGeoJSONToDXF(featureCollection geojson.FeatureCollection, outputFilename string) {
	d := dxf.NewDrawing()
	d.Header().LtScale = 1.0 // 调整比例因子，确保正确的比例（如果需要）
	// 导入所有的特征（面要素）
	for _, feature := range featureCollection.Features {
		switch geom := feature.Geometry.(type) {
		case orb.Polygon:
			// 创建一个DXF层以保存多边形
			layerName := "Polygon"
			d.AddLayer(layerName, color.Red, dxf.DefaultLineType, true)
			d.ChangeLayer(layerName)
			// 将多边形的边界添加到DXF中
			ring := geom[0]
			lwp := entity.NewLwPolyline(len(ring))
			for j, pt := range ring {
				if pt[0] <= 2000 {
					newx, newy := Transformer.CoordTransformAToB(pt[0], pt[1], "4326", "4523")
					lwp.Vertices[j] = []float64{newx, newy}
				} else {
					lwp.Vertices[j] = []float64{pt[0], pt[1]}
				}
			}
			d.AddEntity(lwp)
		case orb.MultiPolygon:
			// 创建一个DXF层以保存多边形
			layerName := "MultiPolygon"
			d.AddLayer(layerName, color.Red, dxf.DefaultLineType, true)
			d.ChangeLayer(layerName)
			// 将多边形的边界添加到DXF中
			ring := geom[0][0]
			lwp := entity.NewLwPolyline(len(ring))
			for j, pt := range ring {
				if pt[0] <= 2000 {
					newx, newy := Transformer.CoordTransformAToB(pt[0], pt[1], "4326", "4523")
					lwp.Vertices[j] = []float64{newx, newy}
				} else {
					lwp.Vertices[j] = []float64{pt[0], pt[1]}
				}

			}
			d.AddEntity(lwp)

		default:
			// 如果遇到非Polygon几何类型，打印一条消息
			fmt.Printf("Unsupported geometry type: %T", geom)
		}
	}

	// 保存DXF文件
	err := d.SaveAs(outputFilename)
	if err != nil {
		log.Println(err)
	}

}
