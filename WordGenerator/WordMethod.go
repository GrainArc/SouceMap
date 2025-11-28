package WordGenerator

import (
	"fmt"
	"gitee.com/gooffice/gooffice/color"
	"gitee.com/gooffice/gooffice/common"
	"gitee.com/gooffice/gooffice/document"
	"gitee.com/gooffice/gooffice/measurement"
	"gitee.com/gooffice/gooffice/schema/soo/wml"
	"github.com/GrainArc/SouceMap/methods"
)

// 插入一级标题
func AddHeading1(doc *document.Document, text string) {
	para := doc.AddParagraph()
	para.SetOutlineLvl(1)
	run := para.AddRun()
	run.Properties().SetSize(22)
	run.Properties().SetFontFamily("仿宋")
	run.Properties().SetBold(true)
	run.AddText(text)
	para.SetStyle("标题 1")
	para.Properties().SetHeadingLevel(1)
}

// 插入2级标题
func AddHeading2(doc *document.Document, text string) {
	para := doc.AddParagraph()
	para.SetOutlineLvl(2)
	run := para.AddRun()
	run.Properties().SetSize(16)
	run.Properties().SetFontFamily("仿宋")
	run.Properties().SetBold(true)
	run.AddText(text)
	para.SetStyle("标题 2")
	para.Properties().SetHeadingLevel(2)
}

// 插入2级标题
func AddHeading3(doc *document.Document, text string) {
	para := doc.AddParagraph()
	para.SetOutlineLvl(3)
	run := para.AddRun()
	run.Properties().SetSize(15)
	run.Properties().SetFontFamily("仿宋")
	run.Properties().SetBold(true)
	run.AddText(text)
	para.SetStyle("标题 3")
	para.Properties().SetHeadingLevel(3)
}

// 插入正文
func AddText(doc *document.Document, text string, iscenter bool) {
	para := doc.AddParagraph()
	if iscenter {
		para.Properties().SetAlignment(wml.ST_JcCenter)
	}
	run := para.AddRun()
	run.Properties().SetSize(14)

	run.AddText(text)
}

func AddTextBlod(doc *document.Document, text string, iscenter bool) {
	para := doc.AddParagraph()
	if iscenter {
		para.Properties().SetAlignment(wml.ST_JcCenter)
	}
	run := para.AddRun()
	run.Properties().SetSize(14)
	run.Properties().SetBold(true)
	run.AddText(text)
}

// 分析表格导出
func OutTable(doc *document.Document, text []methods.Result, area float64) {
	//插入文本
	table := doc.AddTable()
	table.Properties().SetAlignment(wml.ST_JcTableCenter)
	// width of the page
	table.Properties().SetWidthPercent(100)
	// with thick borers
	borders := table.Properties().Borders()
	borders.SetAll(wml.ST_BorderSingle, color.Auto, 1*measurement.Point)
	row := table.AddRow()
	Paragraph := row.AddCell().AddParagraph()
	Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
	run := Paragraph.AddRun()
	run.Properties().SetBold(true)
	run.Properties().SetSize(12)
	run.AddText("分类")
	Paragraph = row.AddCell().AddParagraph()
	Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
	run = Paragraph.AddRun()
	run.Properties().SetBold(true)
	run.Properties().SetSize(12)
	run.AddText("用地面积（平方米）")
	Paragraph = row.AddCell().AddParagraph()
	Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
	run = Paragraph.AddRun()
	run.Properties().SetBold(true)
	run.Properties().SetSize(12)
	run.AddText("用地面积（亩）")
	Paragraph = row.AddCell().AddParagraph()
	Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
	run = Paragraph.AddRun()
	run.Properties().SetBold(true)
	run.Properties().SetSize(12)
	run.AddText("占总用地面积比例")
	ALLpercent := 0.00
	for _, item := range text {
		row = table.AddRow()
		Paragraph = row.AddCell().AddParagraph()
		Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
		run = Paragraph.AddRun()
		run.Properties().SetSize(12)
		run.AddText(item.Dlmc)
		Paragraph = row.AddCell().AddParagraph()
		Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
		run = Paragraph.AddRun()
		run.Properties().SetSize(12)
		run.AddText(fmt.Sprintf("%.2fm²", item.Area))
		Paragraph = row.AddCell().AddParagraph()
		Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
		run = Paragraph.AddRun()
		run.Properties().SetSize(12)
		run.AddText(fmt.Sprintf("%.2f亩", item.Area/666.667))
		percent := fmt.Sprintf("%.2f", (item.Area/area)*100) + "%"
		ALLpercent = ALLpercent + (item.Area/area)*100
		Paragraph = row.AddCell().AddParagraph()
		Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
		run = Paragraph.AddRun()
		run.Properties().SetSize(12)
		run.AddText(percent)

	}
	row = table.AddRow()
	Paragraph = row.AddCell().AddParagraph()
	Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
	run = Paragraph.AddRun()
	run.Properties().SetBold(true)
	run.Properties().SetSize(12)
	run.AddText("合计")
	Paragraph = row.AddCell().AddParagraph()
	Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
	run = Paragraph.AddRun()
	run.Properties().SetBold(true)
	run.Properties().SetSize(12)
	run.AddText(fmt.Sprintf("%.2fm²", area))
	Paragraph = row.AddCell().AddParagraph()
	Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
	run = Paragraph.AddRun()
	run.Properties().SetBold(true)
	run.Properties().SetSize(12)
	run.AddText(fmt.Sprintf("%.2f亩", area/666.667))
	Paragraph = row.AddCell().AddParagraph()
	Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
	run = Paragraph.AddRun()
	run.Properties().SetBold(true)
	run.Properties().SetSize(12)
	run.AddText(fmt.Sprintf("%.2f", ALLpercent) + "%")

}

// 全表导出
// 分析表格导出
type LayerSchema2 struct {
	ID      int64
	Main    string
	CN      string
	EN      string
	Date    string
	Type    string
	Opacity string
	Color   string `json:"Color"`
}

func OutSchema(doc *document.Document, text []LayerSchema2) {
	//插入文本
	table := doc.AddTable()
	table.Properties().SetAlignment(wml.ST_JcTableCenter)
	// width of the page
	table.Properties().SetWidthPercent(100)
	// with thick borers
	borders := table.Properties().Borders()
	borders.SetAll(wml.ST_BorderSingle, color.Auto, 1*measurement.Point)
	row := table.AddRow()
	Paragraph := row.AddCell().AddParagraph()
	Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
	run := Paragraph.AddRun()
	run.Properties().SetBold(true)
	run.Properties().SetSize(12)
	run.AddText("主目录")
	Paragraph = row.AddCell().AddParagraph()
	Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
	run = Paragraph.AddRun()
	run.Properties().SetBold(true)
	run.Properties().SetSize(12)
	run.AddText("图层名")
	Paragraph = row.AddCell().AddParagraph()
	Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
	run = Paragraph.AddRun()
	run.Properties().SetBold(true)
	run.Properties().SetSize(12)
	run.AddText("图层数据表名")
	Paragraph = row.AddCell().AddParagraph()
	Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
	run = Paragraph.AddRun()
	run.Properties().SetBold(true)
	run.Properties().SetSize(12)
	run.AddText("图层类型")
	Paragraph = row.AddCell().AddParagraph()
	Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
	run = Paragraph.AddRun()
	run.Properties().SetBold(true)
	run.Properties().SetSize(12)
	run.AddText("更新日期")
	for _, item := range text {
		row = table.AddRow()
		Paragraph = row.AddCell().AddParagraph()
		Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
		run = Paragraph.AddRun()
		run.Properties().SetSize(12)
		run.AddText(item.Main)
		Paragraph = row.AddCell().AddParagraph()
		Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
		run = Paragraph.AddRun()
		run.Properties().SetSize(12)
		run.AddText(item.CN)
		Paragraph = row.AddCell().AddParagraph()
		Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
		run = Paragraph.AddRun()
		run.Properties().SetSize(12)
		run.AddText(item.EN)
		Paragraph = row.AddCell().AddParagraph()
		Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
		run = Paragraph.AddRun()
		run.Properties().SetSize(12)
		run.AddText(item.Type)

		Paragraph = row.AddCell().AddParagraph()
		Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
		run = Paragraph.AddRun()
		run.Properties().SetSize(12)
		run.AddText(item.Date)

	}

}

// 表格插入图片
func InsertImageIntoTableCell(doc *document.Document, img []byte, tableIndex, rowIndex, colIndex int, width float64) error {
	// 获取文档中的所有表格
	tables := doc.Tables()
	if tableIndex >= len(tables) {
		return fmt.Errorf("表格索引 %d 超出范围,文档中共有 %d 个表格", tableIndex, len(tables))
	}

	table := tables[tableIndex]

	// 获取指定行
	rows := table.Rows()
	if rowIndex >= len(rows) {
		return fmt.Errorf("行索引 %d 超出范围,表格中共有 %d 行", rowIndex, len(rows))
	}

	row := rows[rowIndex]

	// 获取指定单元格
	cells := row.Cells()
	if colIndex >= len(cells) {
		return fmt.Errorf("列索引 %d 超出范围,该行中共有 %d 列", colIndex, len(cells))
	}

	cell := cells[colIndex]

	// 获取单元格中的段落
	para := cell.Paragraphs()

	// 创建图片引用
	img2, err := common.ImageFromBytes(img)
	if err != nil {
		return fmt.Errorf("解析图片失败: %v", err)
	}

	imgRef, err := doc.AddImage(img2)
	if err != nil {
		return fmt.Errorf("添加图片失败: %v", err)
	}

	origWidth := float64(img2.Size.X)
	origHeight := float64(img2.Size.Y)

	// 计算按比例缩放后的高度
	aspectRatio := origHeight / origWidth
	height := width * aspectRatio

	// 在段落中插入图片
	run := para[0].AddRun()
	inlineImg, err := run.AddDrawingInline(imgRef)
	if err != nil {
		return fmt.Errorf("插入图片失败: %v", err)
	}

	// 设置图片大小（保持宽高比）
	inlineImg.SetSize(
		measurement.Distance(width)*measurement.Inch,
		measurement.Distance(height)*measurement.Inch,
	)

	return nil
}
