package WordGenerator

import (
	"fmt"
	"gitee.com/gooffice/gooffice/color"
	"gitee.com/gooffice/gooffice/common"
	"gitee.com/gooffice/gooffice/document"
	"gitee.com/gooffice/gooffice/measurement"
	"gitee.com/gooffice/gooffice/schema/soo/wml"
	"github.com/GrainArc/SouceMap/Transformer"
	"github.com/GrainArc/SouceMap/models"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"math"
	"strings"
)

func ReplaceWord(word *document.Document, position string, replacer string) {
	for _, p := range word.Paragraphs() {
		for _, r := range p.Runs() {
			txt := r.Text()
			find := strings.Contains(txt, position)
			if find {
				r.ClearContent()
				r.AddText(strings.ReplaceAll(txt, position, replacer))
			}
		}
	}
}

func BoundaryPointsTable(word *document.Document, geo *geojson.FeatureCollection) {
	Tables := word.Tables()
	Ptable := Tables[1]

	for _, feature := range geo.Features {

		switch geom := feature.Geometry.(type) {

		case orb.Polygon:
			// 将多边形的边界添加到DXF中
			ring := geom[0]
			for j, pt := range ring {
				row := Ptable.AddRow()
				cell0 := row.AddCell()
				cell0.Properties().SetVerticalAlignment(wml.ST_VerticalJcCenter)
				cell0.AddParagraph().AddRun().AddText(fmt.Sprintf("%d", j+1))
				cell1 := row.AddCell()
				cell1.Properties().SetVerticalAlignment(wml.ST_VerticalJcCenter)
				cell1.AddParagraph().AddRun().AddText(fmt.Sprintf("J%d", j+1))
				cell2 := row.AddCell()
				cell2.Properties().SetVerticalAlignment(wml.ST_VerticalJcCenter)
				cell3 := row.AddCell()
				cell3.Properties().SetVerticalAlignment(wml.ST_VerticalJcCenter)
				if pt[0] <= 2000 {
					newx, newy := Transformer.CoordTransform4326To4523(pt[0], pt[1])
					cell2.AddParagraph().AddRun().AddText(fmt.Sprintf("%4f", newx))
					cell3.AddParagraph().AddRun().AddText(fmt.Sprintf("%4f", newy))
				} else {
					cell2.AddParagraph().AddRun().AddText(fmt.Sprintf("%4f", pt[0]))
					cell3.AddParagraph().AddRun().AddText(fmt.Sprintf("%4f", pt[1]))
				}
			}
			borders := Ptable.Properties().Borders()
			borders.SetAll(wml.ST_BorderSingle, color.Auto, 1*measurement.Point)
		case orb.MultiPolygon:
			// 将多边形的边界添加到DXF中
			ring := geom[0][0]
			for j, pt := range ring {
				row := Ptable.AddRow()
				cell0 := row.AddCell()
				cell0.Properties().SetVerticalAlignment(wml.ST_VerticalJcCenter)
				cell0.AddParagraph().AddRun().AddText(fmt.Sprintf("%d", j+1))
				cell1 := row.AddCell()
				cell1.Properties().SetVerticalAlignment(wml.ST_VerticalJcCenter)
				cell1.AddParagraph().AddRun().AddText(fmt.Sprintf("J%d", j+1))
				cell2 := row.AddCell()
				cell2.Properties().SetVerticalAlignment(wml.ST_VerticalJcCenter)
				cell3 := row.AddCell()
				cell3.Properties().SetVerticalAlignment(wml.ST_VerticalJcCenter)
				if pt[0] <= 2000 {
					newx, newy := Transformer.CoordTransform4326To4523(pt[0], pt[1])
					cell2.AddParagraph().AddRun().AddText(fmt.Sprintf("%4f", newx))
					cell3.AddParagraph().AddRun().AddText(fmt.Sprintf("%4f", newy))
				} else {
					cell2.AddParagraph().AddRun().AddText(fmt.Sprintf("%4f", pt[0]))
					cell3.AddParagraph().AddRun().AddText(fmt.Sprintf("%4f", pt[1]))
				}
			}
			borders := Ptable.Properties().Borders()
			borders.SetAll(wml.ST_BorderSingle, color.Auto, 1*measurement.Point)

		default:
			// 如果遇到非Polygon几何类型，打印一条消息
			fmt.Printf("Unsupported geometry type: %T", geom)
		}
	}
	//word.AddParagraph().AddRun().AddPageBreak()
}

func ZDTable(word *document.Document, MSG *models.TempLayerAttribute, ZDPIC *string) {
	Tables := word.Tables()
	ZTable := Tables[0]
	Rows := ZTable.Rows()
	//插入宗地图
	if ZDPIC != nil {
		img1, _ := common.ImageFromFile(*ZDPIC)
		pic, _ := word.AddImage(img1)
		cells := Rows[0].Cells()
		pp := cells[0].Paragraphs()
		pp[0].AddRun().AddText(MSG.Layername + "地块权属调查报告")
		cells2 := Rows[1].Cells()
		pp2 := cells2[0].Paragraphs()
		run := pp2[0].AddRun()
		anchored, _ := run.AddDrawingInline(pic)
		ow := img1.Size.X
		oh := img1.Size.Y
		scaleFactor := 13.69 * measurement.Inch / 2.54 / float64(ow)
		newHeight := measurement.Distance(float64(oh) * scaleFactor)
		anchored.SetSize(13.69*measurement.Inch/2.54, newHeight)
		//插入四至信息
		cells = Rows[2].Cells()
		pp = cells[1].Paragraphs()
		pp[0].AddRun().AddText(MSG.B)
		cells = Rows[3].Cells()
		pp = cells[1].Paragraphs()
		pp[0].AddRun().AddText(MSG.D)
		cells = Rows[4].Cells()
		pp = cells[1].Paragraphs()
		pp[0].AddRun().AddText(MSG.N)
		cells = Rows[5].Cells()
		pp = cells[1].Paragraphs()
		pp[0].AddRun().AddText(MSG.X)
		//插入签字信息
		zjrimg, _ := common.ImageFromBytes(MSG.DCR)
		DCR, _ := word.AddImage(zjrimg)
		cells = Rows[2].Cells()
		pp = cells[3].Paragraphs()
		run = pp[0].AddRun()
		anchored, _ = run.AddDrawingInline(DCR)
		ow = img1.Size.X
		oh = img1.Size.Y
		scaleFactor = 5.3 * measurement.Inch / 2.54 / float64(ow)
		newHeight = measurement.Distance(float64(oh) * scaleFactor)
		anchored.SetSize(5.3*measurement.Inch/2.54, newHeight)
		ZJRimg, _ := common.ImageFromBytes(MSG.ZJR)
		ZJR, _ := word.AddImage(ZJRimg)
		cells = Rows[4].Cells()
		pp = cells[3].Paragraphs()
		run = pp[0].AddRun()
		anchored, _ = run.AddDrawingInline(ZJR)
		ow = img1.Size.X
		oh = img1.Size.Y
		scaleFactor = 5.3 * measurement.Inch / 2.54 / float64(ow)
		newHeight = measurement.Distance(float64(oh) * scaleFactor)
		anchored.SetSize(5.3*measurement.Inch/2.54, newHeight)
	}

}
func isOdd(n int) bool {
	return n%2 != 0
}
func PICTable(word *document.Document, PICS []models.GeoPic) {
	Tables := word.Tables()
	ZTable := Tables[2]
	Rows := ZTable.Rows()

	for index, item := range PICS {
		if index <= 3 {
			var Cellindex int
			if isOdd(index) {
				Cellindex = 1
			} else {
				Cellindex = 0
			}
			Rowindex := int(math.Ceil(float64(index+1) / 2))

			cells := Rows[Rowindex].Cells()
			pp := cells[Cellindex].Paragraphs()
			run := pp[0].AddRun()
			img1, _ := common.ImageFromFile("./PIC/" + item.TBID + "/" + item.Pic_bsm + ".jpg")
			pic, _ := word.AddImage(img1)
			anchored, _ := run.AddDrawingInline(pic)
			ow := img1.Size.X
			oh := img1.Size.Y
			scaleFactor := 7 * measurement.Inch / 2.54 / float64(ow)
			newHeight := measurement.Distance(float64(oh) * scaleFactor)
			anchored.SetSize(7*measurement.Inch/2.54, newHeight)
		} else {
			Row := ZTable.AddRow()
			if isOdd(index) == false {
				cell1 := Row.AddCell()
				cell1.Properties().SetVerticalAlignment(wml.ST_VerticalJcCenter)
				pp := cell1.AddParagraph()
				pp.Properties().SetAlignment(wml.ST_JcCenter)
				run := pp.AddRun()
				img1, _ := common.ImageFromFile("./PIC/" + item.TBID + "/" + item.Pic_bsm + ".jpg")
				pic, _ := word.AddImage(img1)
				anchored, _ := run.AddDrawingInline(pic)
				ow := img1.Size.X
				oh := img1.Size.Y
				scaleFactor := 7 * measurement.Inch / 2.54 / float64(ow)
				newHeight := measurement.Distance(float64(oh) * scaleFactor)
				anchored.SetSize(7*measurement.Inch/2.54, newHeight)

				cell2 := Row.AddCell()
				cell2.Properties().SetVerticalAlignment(wml.ST_VerticalJcCenter)
				pp = cell2.AddParagraph()
				pp.Properties().SetAlignment(wml.ST_JcCenter)
				run = pp.AddRun()
				img1, _ = common.ImageFromFile("./PIC/" + item.TBID + "/" + PICS[index+1].Pic_bsm + ".jpg")
				pic, _ = word.AddImage(img1)
				anchored, _ = run.AddDrawingInline(pic)
				ow = img1.Size.X
				oh = img1.Size.Y
				scaleFactor = 7 * measurement.Inch / 2.54 / float64(ow)
				newHeight = measurement.Distance(float64(oh) * scaleFactor)
				anchored.SetSize(7*measurement.Inch/2.54, newHeight)

			}

		}

	}
}
