package services

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"gitee.com/gooffice/gooffice/color"
	"gitee.com/gooffice/gooffice/common"
	"gitee.com/gooffice/gooffice/document"
	"gitee.com/gooffice/gooffice/measurement"
	"gitee.com/gooffice/gooffice/schema/soo/wml"
	"github.com/GrainArc/SouceMap/ImgHandler"
	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/models"
	"github.com/paulmach/orb/geojson"
	"os"
	"strings"
)

//go:embed ZBZ/N.png
var defaultZBZData []byte

// DocumentBuilder Word文档构建器
type DocumentBuilder struct {
	doc    *document.Document
	Geo    *geojson.FeatureCollection
	IMGMap []ImgMap
}

// NewDocumentBuilderFromFile 从现有文件创建文档构建器
func NewDocumentBuilderFromFile(filename string) (*DocumentBuilder, error) {
	doc, err := document.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("打开文档失败: %v", err)
	}
	return &DocumentBuilder{
		doc: doc,
	}, nil
}

// 在 services 包中添加
func NewDocumentBuilderFromBytes(data []byte) (*DocumentBuilder, error) {
	// 创建临时文件
	tmpFile, err := os.CreateTemp("", "template-*.docx")
	if err != nil {
		return nil, fmt.Errorf("创建临时文件失败: %w", err)
	}
	defer os.Remove(tmpFile.Name()) // 使用后删除

	// 写入数据
	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		return nil, fmt.Errorf("写入临时文件失败: %w", err)
	}
	tmpFile.Close()

	// 使用现有方法加载
	return NewDocumentBuilderFromFile(tmpFile.Name())
}

// GetDocument 获取底层文档对象
func (db *DocumentBuilder) GetDocument() *document.Document {
	return db.doc
}

// AddHeading1 插入1级标题
func (db *DocumentBuilder) AddHeading1(config models.Heading1Config) *DocumentBuilder {
	para := db.doc.AddParagraph()
	para.SetOutlineLvl(1)

	// 设置对齐方式
	db.setAlignment(para, config.Alignment)

	run := para.AddRun()

	// 应用文本样式
	db.applyTextStyle(run, config.Style)

	run.AddText(config.Text)
	para.SetStyle("标题 1")
	para.Properties().SetHeadingLevel(1)

	return db
}

// AddHeading2 插入2级标题
func (db *DocumentBuilder) AddHeading2(config models.Heading2Config) *DocumentBuilder {
	para := db.doc.AddParagraph()
	para.SetOutlineLvl(2)

	// 设置对齐方式
	db.setAlignment(para, config.Alignment)

	run := para.AddRun()

	// 应用文本样式
	db.applyTextStyle(run, config.Style)

	run.AddText(config.Text)
	para.SetStyle("标题 2")
	para.Properties().SetHeadingLevel(2)

	return db
}

// AddHeading3 插入3级标题
func (db *DocumentBuilder) AddHeading3(config models.Heading3Config) *DocumentBuilder {
	para := db.doc.AddParagraph()
	para.SetOutlineLvl(3)

	// 设置对齐方式
	db.setAlignment(para, config.Alignment)

	run := para.AddRun()

	// 应用文本样式
	db.applyTextStyle(run, config.Style)

	run.AddText(config.Text)
	para.SetStyle("标题 3")
	para.Properties().SetHeadingLevel(3)

	return db
}

// AddParagraph 插入正文
func (db *DocumentBuilder) AddParagraph(config models.ParagraphConfig) *DocumentBuilder {
	para := db.doc.AddParagraph()

	// 设置对齐方式
	db.setAlignment(para, config.Alignment)

	// 设置缩进
	if config.Indent > 0 {
		// 每个字符约等于2个字符宽度，这里使用磅值
		indentValue := measurement.Distance(config.Indent * 14) // 假设14磅为一个字符宽度
		para.Properties().SetFirstLineIndent(indentValue)
	}
	run := para.AddRun()

	// 应用文本样式
	db.applyTextStyle(run, config.Style)

	run.AddText(config.Text)

	return db
}

// AddParagraph 插入正文
func (db *DocumentBuilder) AddAnalysisParagraphConfig(config models.AnalysisParagraphConfig) *DocumentBuilder {
	para := db.doc.AddParagraph()

	// 设置对齐方式
	db.setAlignment(para, config.Alignment)

	// 设置缩进
	if config.Indent > 0 {
		// 每个字符约等于2个字符宽度，这里使用磅值
		indentValue := measurement.Distance(config.Indent * 14) // 假设14磅为一个字符宽度
		para.Properties().SetFirstLineIndent(indentValue)
	}
	run := para.AddRun()
	DB := models.DB
	var sechema models.MySchema
	DB.Where("en = ?", config.SourceLayer).First(&sechema)

	// 应用文本样式
	db.applyTextStyle(run, config.Style)
	text := methods.GeoIntersect(*db.Geo, config.SourceLayer, config.Attributes)
	area := methods.CalculateArea(db.Geo.Features[0])
	var Text string
	if len(text) == 1 && text[0].Dlmc == "未占用" {
		Text = "未涉及"
	} else {
		var XMXX []string
		for _, item := range text {
			ST := fmt.Sprintf("项目范围内%s为%.2fm²，占项目范围的%.2f", item.Dlmc, item.Area, (item.Area/area)*100) + "%"
			XMXX = append(XMXX, ST)
		}
		result := strings.Join(XMXX, "；")
		MaxDL := ""
		if len(text) >= 2 {
			maxAreaIndex := 0
			maxArea := text[0].Area
			for i, result := range text {
				if result.Area > maxArea {
					maxArea = result.Area
					maxAreaIndex = i
				}

			}
			MaxDL = fmt.Sprintf("其中主要为%s", text[maxAreaIndex].Dlmc)
			Text = "	" + result + "；" + MaxDL + fmt.Sprintf("。数据来源为%s", sechema.CN)
		} else {
			Text = "	" + result + "；" + fmt.Sprintf("。数据来源为%s", sechema.CN)
		}
	}

	run.AddText(Text)
	return db
}

func attToCN(att string, en string) string {
	DB := models.DB
	if strings.Contains(att, ",") {

		temp := strings.Split(att, ",")
		var result []string

		for _, v := range temp {
			// 去除空格
			v = strings.TrimSpace(v)
			if v == "" {
				continue
			}

			var ce models.ChineseProperty
			err := DB.Where("layer_name = ? AND e_name = ?", en, v).First(&ce).Error

			if err == nil && ce.CName != "" {
				// 找到匹配的中文名称
				result = append(result, ce.CName)
			} else {
				// 未找到匹配,保留原英文
				result = append(result, v)
			}
		}

		// 用逗号连接所有结果
		return strings.Join(result, "、")
	}

	// 如果不包含逗号,单个值处理
	var ce models.ChineseProperty
	err := DB.Where("layer_name = ? AND e_name = ?", en, strings.TrimSpace(att)).First(&ce).Error

	if err == nil && ce.CName != "" {
		return ce.CName
	}

	return att
}

// AddTable 插入表格
func (db *DocumentBuilder) AddTable(config models.TableConfig) error {

	area := methods.CalculateArea(db.Geo.Features[0])
	// 添加表格标题（如果有）
	if config.Caption != "" {
		captionPara := db.doc.AddParagraph()
		captionPara.Properties().SetAlignment(wml.ST_JcCenter)
		captionRun := captionPara.AddRun()
		captionRun.Properties().SetBold(true)
		captionRun.AddText(config.Caption)
	}

	text := methods.GeoIntersect(*db.Geo, config.SourceLayer, config.Attributes)

	//插入表格
	table := db.doc.AddTable()
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
	run.AddText(attToCN(config.Attributes, config.SourceLayer))
	Paragraph = row.AddCell().AddParagraph()
	Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
	run = Paragraph.AddRun()
	run.Properties().SetBold(true)
	run.Properties().SetSize(12)
	run.AddText("面积（平方米）")
	Paragraph = row.AddCell().AddParagraph()
	Paragraph.Properties().SetAlignment(wml.ST_JcCenter)
	run = Paragraph.AddRun()
	run.Properties().SetBold(true)
	run.Properties().SetSize(12)
	run.AddText("面积（亩）")
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

	return nil
}

// AddImage 插入图片
func (db *DocumentBuilder) AddImage(config models.ImageConfig, Img []byte) error {
	// 添加图片标题（如果有）
	if config.Caption != "" {
		captionPara := db.doc.AddParagraph()
		captionPara.Properties().SetAlignment(wml.ST_JcCenter)
		captionRun := captionPara.AddRun()
		captionRun.Properties().SetBold(true)
		captionRun.AddText(config.Caption)
	}

	// 最终要插入的图片数据
	var finalImg []byte
	var err error

	// 制作图例
	DB := models.DB
	var Colors []models.AttColor
	DB.Where("layer_name = ?", config.SourceLayer).Find(&Colors)

	// 判断是否需要添加图例
	if len(Colors) > 1 {
		// 多个颜色配置，需要制作图例
		ATT := Colors[0].AttName
		text := methods.GeoIntersect(*db.Geo, config.SourceLayer, ATT)
		legend, err := ImgHandler.TLImgMakeFilter(config.SourceLayer, text)
		if err != nil {
			return fmt.Errorf("制作图例失败: %v", err)
		}

		// 将图例嵌入到原图中（右上角）
		finalImg, err = ImgHandler.EmbedEagleEye(legend, Img, 0.15, 1, 2)
		if err != nil {
			return fmt.Errorf("嵌入图例失败: %v", err)
		}
	} else {
		// 没有图例，直接使用原图
		finalImg = Img
	}

	// 插入指北针（左上角）
	ZBZ := defaultZBZData
	finalImg, err = ImgHandler.EmbedEagleEye(ZBZ, finalImg, 0.05, 1, 1)
	if err != nil {
		return fmt.Errorf("嵌入指北针失败: %v", err)
	}

	// 将图片数据添加到文档中
	fImg, err := common.ImageFromBytes(finalImg)
	imgRef, err := db.doc.AddImage(fImg)
	if err != nil {
		return fmt.Errorf("添加图片到文档失败: %v", err)
	}

	// 创建段落并插入图片
	para := db.doc.AddParagraph()

	// 设置图片对齐方式
	db.setAlignment(para, config.Alignment)

	// 在段落中添加图片
	run := para.AddRun()
	inlineImg, err := run.AddDrawingInline(imgRef)
	if err != nil {
		return fmt.Errorf("插入图片失败: %v", err)
	}

	// 设置图片宽度
	if config.Width > 0 {
		widthEMU := config.Width

		imgSize := imgRef.Size()
		// 计算高度（保持宽高比）
		aspectRatio := float64(imgSize.Y) / float64(imgSize.X)
		heightEMU := widthEMU * aspectRatio
		// 设置图片尺寸
		inlineImg.SetSize(measurement.Distance(widthEMU)*measurement.Inch,
			measurement.Distance(heightEMU)*measurement.Inch)
	} else {
		// 如果没有指定宽度，使用默认宽度（例如页面宽度的80%）
		defaultWidth := 8
		imgSize := imgRef.Size()
		aspectRatio := float64(imgSize.Y) / float64(imgSize.X)
		defaultHeight := float64(defaultWidth) * aspectRatio
		inlineImg.SetSize(measurement.Distance(defaultWidth)*measurement.Inch,
			measurement.Distance(defaultHeight)*measurement.Inch)
	}

	// 添加图片后的空行
	db.doc.AddParagraph()

	return nil
}

type ImgMap struct {
	IMG       []byte
	LayerName string
}

func GetImg(ImgMap []ImgMap, LayerName string) []byte {
	for _, img := range ImgMap {
		if img.LayerName == LayerName {
			return img.IMG
		}
	}
	return nil
}

// AddContentItem 根据ContentItem配置添加单个内容
func (db *DocumentBuilder) AddContentItem(item models.ContentItem) error {
	configJSON, err := json.Marshal(item.Config)
	if err != nil {
		return fmt.Errorf("配置序列化失败: %v", err)
	}

	switch item.Type {
	case "heading1":
		var config models.Heading1Config
		if err := json.Unmarshal(configJSON, &config); err != nil {
			return fmt.Errorf("解析heading1配置失败: %v", err)
		}
		db.AddHeading1(config)

	case "heading2":
		var config models.Heading2Config
		if err := json.Unmarshal(configJSON, &config); err != nil {
			return fmt.Errorf("解析heading2配置失败: %v", err)
		}
		db.AddHeading2(config)

	case "heading3":
		var config models.Heading3Config
		if err := json.Unmarshal(configJSON, &config); err != nil {
			return fmt.Errorf("解析heading3配置失败: %v", err)
		}
		db.AddHeading3(config)

	case "paragraph":
		var config models.ParagraphConfig
		if err := json.Unmarshal(configJSON, &config); err != nil {
			return fmt.Errorf("解析paragraph配置失败: %v", err)
		}
		db.AddParagraph(config)
	case "analysis_paragraph":
		var config models.AnalysisParagraphConfig
		if err := json.Unmarshal(configJSON, &config); err != nil {
			return fmt.Errorf("解析AddAnalysisParagraphConfig配置失败: %v", err)
		}
		db.AddAnalysisParagraphConfig(config)
	case "table":
		var config models.TableConfig
		if err := json.Unmarshal(configJSON, &config); err != nil {
			return fmt.Errorf("解析table配置失败: %v", err)
		}

		if err := db.AddTable(config); err != nil {
			return err
		}

	case "image":
		var config models.ImageConfig
		if err := json.Unmarshal(configJSON, &config); err != nil {
			return fmt.Errorf("解析image配置失败: %v", err)
		}
		if err := db.AddImage(config, GetImg(db.IMGMap, config.SourceLayer)); err != nil {
			return err
		}

	default:
		return fmt.Errorf("未知的内容类型: %s", item.Type)
	}

	return nil
}

// AddContentItems 批量添加内容
func (db *DocumentBuilder) AddContentItems(items []models.ContentItem) error {
	// 按Order排序
	sortedItems := make([]models.ContentItem, len(items))
	copy(sortedItems, items)

	for i := 0; i < len(sortedItems); i++ {
		for j := i + 1; j < len(sortedItems); j++ {
			if sortedItems[i].Order > sortedItems[j].Order {
				sortedItems[i], sortedItems[j] = sortedItems[j], sortedItems[i]
			}
		}
	}

	// 依次添加内容
	for _, item := range sortedItems {
		if err := db.AddContentItem(item); err != nil {
			return fmt.Errorf("添加内容失败 (order=%d, type=%s): %v", item.Order, item.Type, err)
		}
	}

	return nil
}

// Save 保存文档
func (db *DocumentBuilder) Save(filename string) error {
	return db.doc.SaveToFile(filename)
}

// SaveAndClose 保存并关闭文档
func (db *DocumentBuilder) SaveAndClose(filename string) error {
	if err := db.Save(filename); err != nil {
		return err
	}
	return db.Close()
}

// Close 关闭文档
func (db *DocumentBuilder) Close() error {
	if db.doc != nil {
		return db.doc.Close()
	}
	return nil
}

// 私有辅助方法

// setAlignment 设置段落对齐方式
func (db *DocumentBuilder) setAlignment(para document.Paragraph, alignment string) {
	switch alignment {
	case "left":
		para.Properties().SetAlignment(wml.ST_JcLeft)
	case "center":
		para.Properties().SetAlignment(wml.ST_JcCenter)
	case "right":
		para.Properties().SetAlignment(wml.ST_JcRight)
	case "justify":
		para.Properties().SetAlignment(wml.ST_JcBoth)
	default:
		para.Properties().SetAlignment(wml.ST_JcLeft)
	}
}

// setTableAlignment 设置表格对齐方式
func (db *DocumentBuilder) setTableAlignment(table document.Table, alignment string) {
	switch alignment {
	case "left":
		table.Properties().SetAlignment(wml.ST_JcTableLeft)
	case "center":
		table.Properties().SetAlignment(wml.ST_JcTableCenter)
	case "right":
		table.Properties().SetAlignment(wml.ST_JcTableRight)
	}
}

// applyTextStyle 应用文本样式
func (db *DocumentBuilder) applyTextStyle(run document.Run, style models.TextStyle) {
	if style.FontFamily != "" {
		run.Properties().SetFontFamily(style.FontFamily)
	}

	if style.FontSize > 0 {
		run.Properties().SetSize(measurement.Distance(style.FontSize))
	}

	if style.Bold {
		run.Properties().SetBold(true)
	}

	if style.Italic {
		run.Properties().SetItalic(true)
	}

	if style.Underline {
		run.Properties().SetUnderline(wml.ST_UnderlineSingle, color.Auto)
	}

	if style.Color != "" {
		run.Properties().SetColor(db.parseColor(style.Color))
	}
}

// 解析颜色（十六进制转换）
func (db *DocumentBuilder) parseColor(hexColor string) color.Color {
	if hexColor == "" {
		return color.Auto
	}

	// 移除 # 前缀
	hexColor = strings.TrimPrefix(hexColor, "#")

	// 确保是6位十六进制
	if len(hexColor) != 6 {
		return color.Auto
	}

	var r, g, b uint8
	fmt.Sscanf(hexColor, "%02x%02x%02x", &r, &g, &b)

	return color.RGB(r, g, b)
}
