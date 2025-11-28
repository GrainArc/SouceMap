package models

import "gorm.io/datatypes"

type Report struct {
	ID         int64          `gorm:"primary_key;autoIncrement"`
	ReportName string         `gorm:"type:varchar(255)"`
	Layers     datatypes.JSON `gorm:"type:jsonb"`
	Content    datatypes.JSON `gorm:"type:jsonb"`
}

type ContentItem struct {
	Type   string      `json:"type"`   // 类型：heading1, heading2, heading3, paragraph, analysis_paragraph, table, image
	Order  int         `json:"order"`  // 排序序号
	Config interface{} `json:"config"` // 具体配置，根据type不同而不同
}

// Heading1Config 一级标题配置
type Heading1Config struct {
	Text      string    `json:"text"`      // 标题文本
	Alignment string    `json:"alignment"` // 对齐方式：left, center, right
	Style     TextStyle `json:"style"`     // 文本样式
}

// Heading2Config 二级标题配置
type Heading2Config struct {
	Text      string    `json:"text"`
	Alignment string    `json:"alignment"`
	Style     TextStyle `json:"style"`
}

// Heading3Config 三级标题配置
type Heading3Config struct {
	Text      string    `json:"text"`
	Alignment string    `json:"alignment"`
	Style     TextStyle `json:"style"`
}

// ParagraphConfig 正文配置
type ParagraphConfig struct {
	Text      string    `json:"text"`      // 正文内容
	Alignment string    `json:"alignment"` // 对齐方式
	Style     TextStyle `json:"style"`     // 文本样式
	Indent    int       `json:"indent"`    // 缩进（单位：字符）
}

// 分析情况配置
type AnalysisParagraphConfig struct {
	SourceLayer string    `json:"source_layer"`
	Attributes  string    `json:"attributes"`
	Alignment   string    `json:"alignment"` // 对齐方式
	Style       TextStyle `json:"style"`     // 文本样式
	Indent      int       `json:"indent"`    // 缩进（单位：字符）
}

// TextStyle 文本样式
type TextStyle struct {
	FontFamily string `json:"font_family"` // 字体
	FontSize   int    `json:"font_size"`   // 字号
	Bold       bool   `json:"bold"`        // 加粗
	Italic     bool   `json:"italic"`      // 斜体
	Underline  bool   `json:"underline"`   // 下划线
	Color      string `json:"color"`       // 颜色（十六进制）
}

// TableConfig 表格配置
type TableConfig struct {
	SourceLayer string     `json:"source_layer"`
	Attributes  string     `json:"attributes"`
	Style       TableStyle `json:"style"`   // 表格样式
	Caption     string     `json:"caption"` // 表格标题
}

// TableStyle 表格样式
type TableStyle struct {
	BorderColor string    `json:"border_color"` // 边框颜色
	BorderWidth int       `json:"border_width"` // 边框宽度
	HeaderBg    string    `json:"header_bg"`    // 表头背景色
	HeaderStyle TextStyle `json:"header_style"` // 表头文本样式
	CellStyle   TextStyle `json:"cell_style"`   // 单元格文本样式
	Alignment   string    `json:"alignment"`    // 表格对齐方式
}

// ImageConfig 图片配置
type ImageConfig struct {
	SourceLayer string  `json:"source_layer"` // 图片使用图层
	Width       float64 `json:"width"`        // 宽度
	Alignment   string  `json:"alignment"`    // 对齐方式
	LegendType  string  `json:"legend_type"`  //图例样式，单图层使用（线、面、点、阴影线、双阴影线）
	Caption     string  `json:"caption"`      // 图片标题
}
