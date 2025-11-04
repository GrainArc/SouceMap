package methods

import (
	"bytes"
	_ "embed"
	"github.com/golang/freetype"
	"github.com/golang/freetype/truetype"
	"golang.org/x/image/font"
	"image"
	"image/color"
	"image/png"
	"log"
	"math"
	"strconv"
	"strings"
)

type LegendItem struct {
	Property string `json:"Property"`
	Color    string `json:"Color"`
}

//go:embed fonts/simhei.ttf
var defaultFontData []byte

func parseRGB(colorStr string) (color.RGBA, error) {
	colorStr = strings.TrimPrefix(colorStr, "RGB(")
	colorStr = strings.TrimSuffix(colorStr, ")")

	parts := strings.Split(colorStr, ",")
	if len(parts) != 3 {
		return color.RGBA{}, nil
	}

	r, _ := strconv.Atoi(strings.TrimSpace(parts[0]))
	g, _ := strconv.Atoi(strings.TrimSpace(parts[1]))
	b, _ := strconv.Atoi(strings.TrimSpace(parts[2]))

	return color.RGBA{uint8(r), uint8(g), uint8(b), 255}, nil
}

func loadFont() (*truetype.Font, error) {
	fontBytes := defaultFontData

	f, err := truetype.Parse(fontBytes)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func drawChineseText(img *image.RGBA, x, y int, text string, fontSize float64, fontColor color.Color, ttfFont *truetype.Font) error {
	c := freetype.NewContext()
	c.SetDPI(72)
	c.SetFont(ttfFont)
	c.SetFontSize(fontSize)
	c.SetClip(img.Bounds())
	c.SetDst(img)
	c.SetSrc(image.NewUniform(fontColor))
	c.SetHinting(font.HintingFull)

	pt := freetype.Pt(x, y)
	_, err := c.DrawString(text, pt)
	return err
}

func CreateLegend(items []LegendItem) ([]byte, error) {

	ttfFont, err := loadFont()
	if err != nil {
		return nil, err
	}

	// 图例参数
	itemHeight := 35
	colorBoxWidth := 50
	colorBoxHeight := 25
	textOffsetX := 65
	padding := 15
	minItemWidth := 150 // 最小宽度
	itemWidths := make([]int, len(items))

	maxItemWidth := minItemWidth

	for i, item := range items {
		textWidth := calculateTextWidth(item.Property, 14, ttfFont)
		itemWidth := textOffsetX + textWidth + 20 // 20为右侧留白

		if itemWidth < minItemWidth {
			itemWidth = minItemWidth
		}

		itemWidths[i] = itemWidth
		if itemWidth > maxItemWidth {
			maxItemWidth = itemWidth
		}
	}

	// 使用统一的项宽度（取最大值，保证布局整齐）
	itemWidth := maxItemWidth
	// 计算最佳列数，使布局接近正方形
	numItems := len(items)
	numCols := calculateOptimalColumns(numItems, itemWidth, itemHeight)
	numRows := (numItems + numCols - 1) / numCols // 向上取整

	// 计算图像尺寸
	width := numCols*itemWidth + padding*2
	height := numRows*itemHeight + padding*2

	// 创建图像
	img := image.NewRGBA(image.Rect(0, 0, width, height))

	// 填充白色背景
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.Set(x, y, color.White)
		}
	}

	// 绘制每个图例项
	for i, item := range items {
		row := i / numCols
		col := i % numCols

		xPos := padding + col*itemWidth
		yPos := padding + row*itemHeight

		// 解析颜色
		col1, err := parseRGB(item.Color)
		if err != nil {
			log.Printf("解析颜色失败: %v", err)
			continue
		}

		// 绘制颜色框
		for dy := 0; dy < colorBoxHeight; dy++ {
			for dx := 0; dx < colorBoxWidth; dx++ {
				img.Set(xPos+dx, yPos+dy+5, col1)
			}
		}

		// 绘制边框
		borderColor := color.RGBA{80, 80, 80, 255}
		for dx := 0; dx < colorBoxWidth; dx++ {
			img.Set(xPos+dx, yPos+5, borderColor)
			img.Set(xPos+dx, yPos+5+colorBoxHeight-1, borderColor)
		}
		for dy := 0; dy < colorBoxHeight; dy++ {
			img.Set(xPos, yPos+5+dy, borderColor)
			img.Set(xPos+colorBoxWidth-1, yPos+5+dy, borderColor)
		}

		// 绘制中文文字
		err = drawChineseText(img, xPos+textOffsetX, yPos+22, item.Property, 14, color.Black, ttfFont)
		if err != nil {
			log.Printf("绘制文字失败: %v", err)
		}
	}

	// 将图像编码为PNG格式的字节数组
	var buf bytes.Buffer
	err = png.Encode(&buf, img)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func calculateTextWidth(text string, fontSize float64, ttfFont *truetype.Font) int {
	// 创建字体上下文
	opts := truetype.Options{
		Size: fontSize,
		DPI:  72,
	}
	face := truetype.NewFace(ttfFont, &opts)
	defer face.Close()

	// 计算文本宽度
	width := 0
	for _, r := range text {
		advance, ok := face.GlyphAdvance(r)
		if !ok {
			// 如果字符不存在，使用默认宽度
			width += int(fontSize)
			continue
		}
		width += advance.Round()
	}

	return width
}

// calculateOptimalColumns 计算最佳列数，使布局接近正方形
func calculateOptimalColumns(numItems, itemWidth, itemHeight int) int {
	if numItems == 0 {
		return 1
	}

	optimalCols := int(math.Sqrt(float64(numItems) * float64(itemHeight) / float64(itemWidth)))

	if optimalCols < 1 {
		optimalCols = 1
	}

	// 限制最大列数（避免太宽）
	maxCols := 6
	if optimalCols > maxCols {
		optimalCols = maxCols
	}

	// 如果项目很少，限制列数
	if optimalCols > numItems {
		optimalCols = numItems
	}

	return optimalCols
}
