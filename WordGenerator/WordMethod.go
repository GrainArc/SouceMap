package WordGenerator

import (
	"fmt"
	"gitee.com/gooffice/gooffice/color"
	"gitee.com/gooffice/gooffice/document"
	"gitee.com/gooffice/gooffice/measurement"
	"gitee.com/gooffice/gooffice/schema/soo/wml"
	"github.com/fmecool/SouceMap/methods"
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
