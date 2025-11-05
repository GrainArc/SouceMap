package ImgHandler

import (
	"bytes"
	_ "embed"
	"fmt"
	"github.com/golang/freetype"
	"github.com/golang/freetype/truetype"
	"golang.org/x/image/font"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"log"
	"math"
	"strconv"
	"strings"
)

//go:embed fonts/simhei.ttf
var defaultFontData []byte

type LegendItem struct {
	Property string `json:"Property"`
	Color    string `json:"Color"`
	GeoType  string `json:"GeoType"`
}

// parseColor 解析多种颜色格式
// 支持格式:
//   - RGB(255, 0, 0)
//   - RGBA(255, 0, 0, 128)
//   - #FF0000
//   - #F00
//   - FF0000 (无#前缀)
func parseColor(colorStr string) (color.RGBA, error) {
	colorStr = strings.TrimSpace(colorStr)

	// 处理 RGB/RGBA 格式
	if strings.HasPrefix(strings.ToUpper(colorStr), "RGB") {
		return parseRGBFormat(colorStr)
	}

	// 处理 HEX 格式
	return parseHexFormat(colorStr)
}

// parseRGBFormat 解析 RGB(r,g,b) 或 RGBA(r,g,b,a) 格式
func parseRGBFormat(colorStr string) (color.RGBA, error) {
	// 移除 RGB( 或 RGBA( 前缀和 ) 后缀
	colorStr = strings.TrimPrefix(strings.ToUpper(colorStr), "RGBA(")
	colorStr = strings.TrimPrefix(colorStr, "RGB(")
	colorStr = strings.TrimSuffix(colorStr, ")")

	parts := strings.Split(colorStr, ",")
	if len(parts) < 3 || len(parts) > 4 {
		return color.RGBA{}, fmt.Errorf("invalid RGB format: expected 3 or 4 components, got %d", len(parts))
	}

	// 解析 R, G, B
	r, err := parseColorComponent(parts[0])
	if err != nil {
		return color.RGBA{}, fmt.Errorf("invalid R component: %w", err)
	}

	g, err := parseColorComponent(parts[1])
	if err != nil {
		return color.RGBA{}, fmt.Errorf("invalid G component: %w", err)
	}

	b, err := parseColorComponent(parts[2])
	if err != nil {
		return color.RGBA{}, fmt.Errorf("invalid B component: %w", err)
	}

	// 解析 Alpha（如果存在）
	a := uint8(255)

	return color.RGBA{R: r, G: g, B: b, A: a}, nil
}

// parseHexFormat 解析十六进制颜色格式
// 支持: #RRGGBB, #RGB, RRGGBB, RGB
func parseHexFormat(colorStr string) (color.RGBA, error) {
	// 移除 # 前缀
	colorStr = strings.TrimPrefix(colorStr, "#")

	// 验证是否为有效的十六进制字符
	for _, ch := range colorStr {
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')) {
			return color.RGBA{}, fmt.Errorf("invalid hex color: %s", colorStr)
		}
	}

	var r, g, b uint8

	switch len(colorStr) {
	case 3: // #RGB 格式，扩展为 #RRGGBB
		rVal, _ := strconv.ParseUint(string(colorStr[0])+string(colorStr[0]), 16, 8)
		gVal, _ := strconv.ParseUint(string(colorStr[1])+string(colorStr[1]), 16, 8)
		bVal, _ := strconv.ParseUint(string(colorStr[2])+string(colorStr[2]), 16, 8)
		r, g, b = uint8(rVal), uint8(gVal), uint8(bVal)

	case 6: // #RRGGBB 格式
		rVal, _ := strconv.ParseUint(colorStr[0:2], 16, 8)
		gVal, _ := strconv.ParseUint(colorStr[2:4], 16, 8)
		bVal, _ := strconv.ParseUint(colorStr[4:6], 16, 8)
		r, g, b = uint8(rVal), uint8(gVal), uint8(bVal)

	default:
		return color.RGBA{}, fmt.Errorf("invalid hex color length: expected 3 or 6, got %d", len(colorStr))
	}

	return color.RGBA{R: r, G: g, B: b, A: 255}, nil
}

// parseColorComponent 解析单个颜色分量（0-255）
func parseColorComponent(s string) (uint8, error) {
	s = strings.TrimSpace(s)
	val, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}

	if val < 0 || val > 255 {
		return 0, fmt.Errorf("color component out of range [0-255]: %d", val)
	}

	return uint8(val), nil
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

// drawCircle 绘制圆形（用于点符号）
func drawCircle(img *image.RGBA, centerX, centerY, radius int, fillColor color.Color, borderColor color.Color) {
	for y := -radius; y <= radius; y++ {
		for x := -radius; x <= radius; x++ {
			if x*x+y*y <= radius*radius {
				img.Set(centerX+x, centerY+y, fillColor)
			}
		}
	}

	// 绘制圆形边框（使用更精确的算法）
	for angle := 0.0; angle < 360; angle += 0.5 {
		rad := angle * math.Pi / 180
		x := centerX + int(float64(radius)*math.Cos(rad))
		y := centerY + int(float64(radius)*math.Sin(rad))
		img.Set(x, y, borderColor)
	}
}

// drawPolygonSymbol 绘制面符号（填充矩形）
func drawPolygonSymbol(img *image.RGBA, xPos, yPos, width, height int, fillColor color.Color, borderColor color.Color) {
	// 填充矩形
	for dy := 0; dy < height; dy++ {
		for dx := 0; dx < width; dx++ {
			img.Set(xPos+dx, yPos+dy, fillColor)
		}
	}

	// 绘制边框
	for dx := 0; dx < width; dx++ {
		img.Set(xPos+dx, yPos, borderColor)
		img.Set(xPos+dx, yPos+height-1, borderColor)
	}
	for dy := 0; dy < height; dy++ {
		img.Set(xPos, yPos+dy, borderColor)
		img.Set(xPos+width-1, yPos+dy, borderColor)
	}
}

// drawLineSymbol 绘制线符号（加粗线条）
func drawLineSymbol(img *image.RGBA, xPos, yPos, width, height int, lineColor color.Color, borderColor color.Color) {
	lineThickness := 3 // 线条厚度

	// 绘制外框（浅灰色背景）
	bgColor := color.RGBA{240, 240, 240, 255}
	for dy := 0; dy < height; dy++ {
		for dx := 0; dx < width; dx++ {
			img.Set(xPos+dx, yPos+dy, bgColor)
		}
	}

	// 绘制中心线（加粗）
	centerY := yPos + height/2
	for dy := -lineThickness / 2; dy <= lineThickness/2; dy++ {
		for dx := 0; dx < width; dx++ {
			img.Set(xPos+dx, centerY+dy, lineColor)
		}
	}

	// 绘制外框边框
	for dx := 0; dx < width; dx++ {
		img.Set(xPos+dx, yPos, borderColor)
		img.Set(xPos+dx, yPos+height-1, borderColor)
	}
	for dy := 0; dy < height; dy++ {
		img.Set(xPos, yPos+dy, borderColor)
		img.Set(xPos+width-1, yPos+dy, borderColor)
	}
}

// drawHatchSymbol 绘制阴影线符号（斜线填充）
func drawHatchSymbol(img *image.RGBA, xPos, yPos, width, height int, hatchColor color.Color, borderColor color.Color) {
	// 绘制白色背景
	bgColor := color.White
	for dy := 0; dy < height; dy++ {
		for dx := 0; dx < width; dx++ {
			img.Set(xPos+dx, yPos+dy, bgColor)
		}
	}

	// 绘制斜线（从左上到右下）
	spacing := 4   // 斜线间距
	lineWidth := 2 // 斜线宽度

	// 计算需要绘制的斜线数量
	maxOffset := width + height
	for offset := -height; offset < maxOffset; offset += spacing {
		// 绘制每条斜线
		for w := 0; w < lineWidth; w++ {
			for i := 0; i < width+height; i++ {
				x := i
				y := i - offset + w

				// 检查是否在矩形范围内
				if x >= 0 && x < width && y >= 0 && y < height {
					img.Set(xPos+x, yPos+y, hatchColor)
				}
			}
		}
	}

	// 绘制外框边框
	for dx := 0; dx < width; dx++ {
		img.Set(xPos+dx, yPos, borderColor)
		img.Set(xPos+dx, yPos+height-1, borderColor)
	}
	for dy := 0; dy < height; dy++ {
		img.Set(xPos, yPos+dy, borderColor)
		img.Set(xPos+width-1, yPos+dy, borderColor)
	}
}

// drawCrossHatchSymbol 绘制交叉阴影线符号
func drawCrossHatchSymbol(img *image.RGBA, xPos, yPos, width, height int, hatchColor color.Color, borderColor color.Color) {
	// 绘制白色背景
	bgColor := color.White
	for dy := 0; dy < height; dy++ {
		for dx := 0; dx < width; dx++ {
			img.Set(xPos+dx, yPos+dy, bgColor)
		}
	}

	spacing := 5
	lineWidth := 1

	// 绘制从左上到右下的斜线
	maxOffset := width + height
	for offset := -height; offset < maxOffset; offset += spacing {
		for w := 0; w < lineWidth; w++ {
			for i := 0; i < width+height; i++ {
				x := i
				y := i - offset + w
				if x >= 0 && x < width && y >= 0 && y < height {
					img.Set(xPos+x, yPos+y, hatchColor)
				}
			}
		}
	}

	// 绘制从右上到左下的斜线
	for offset := -height; offset < maxOffset; offset += spacing {
		for w := 0; w < lineWidth; w++ {
			for i := 0; i < width+height; i++ {
				x := width - i - 1
				y := i - offset + w
				if x >= 0 && x < width && y >= 0 && y < height {
					img.Set(xPos+x, yPos+y, hatchColor)
				}
			}
		}
	}

	// 绘制边框
	for dx := 0; dx < width; dx++ {
		img.Set(xPos+dx, yPos, borderColor)
		img.Set(xPos+dx, yPos+height-1, borderColor)
	}
	for dy := 0; dy < height; dy++ {
		img.Set(xPos, yPos+dy, borderColor)
		img.Set(xPos+width-1, yPos+dy, borderColor)
	}
}

// drawSymbol 根据几何类型绘制对应符号
func drawSymbol(img *image.RGBA, xPos, yPos int, symbolWidth, symbolHeight int, geoType string, symbolColor color.Color) {
	borderColor := color.RGBA{80, 80, 80, 255}

	switch strings.ToLower(geoType) {
	case "point":
		// 点：绘制圆形符号
		radius := symbolHeight / 2
		centerX := xPos + symbolWidth/2
		centerY := yPos + symbolHeight/2
		drawCircle(img, centerX, centerY, radius-2, symbolColor, borderColor)

	case "linestring", "line":
		// 线：绘制加粗线条
		drawLineSymbol(img, xPos, yPos, symbolWidth, symbolHeight, symbolColor, borderColor)

	case "polygon", "multipolygon":
		// 面：绘制填充矩形
		drawPolygonSymbol(img, xPos, yPos, symbolWidth, symbolHeight, symbolColor, borderColor)

	case "hatch", "hatchfill", "阴影线": // 新增：阴影线类型
		// 阴影线：绘制斜线填充
		drawHatchSymbol(img, xPos, yPos, symbolWidth, symbolHeight, symbolColor, borderColor)
	case "CrossHatch", "crossHatch", "crosshatch", "cross_hatch": // 交叉阴影线
		//
		drawCrossHatchSymbol(img, xPos, yPos, symbolWidth, symbolHeight, symbolColor, borderColor)

	default:
		// 默认：绘制填充矩形
		drawPolygonSymbol(img, xPos, yPos, symbolWidth, symbolHeight, symbolColor, borderColor)
	}
}

func CreateLegend(items []LegendItem) ([]byte, error) {
	// 加载字体
	ttfFont, err := loadFont()
	if err != nil {
		return nil, err
	}

	// 图例参数
	itemHeight := 40 // 增加高度以适应圆形符号
	symbolWidth := 50
	symbolHeight := 25
	textOffsetX := 65
	padding := 15
	minItemWidth := 150

	maxItemWidth := minItemWidth

	// 计算每个项的宽度
	for _, item := range items {
		textWidth := calculateTextWidth(item.Property, 14, ttfFont)
		itemWidth := textOffsetX + textWidth + 20

		if itemWidth < minItemWidth {
			itemWidth = minItemWidth
		}

		if itemWidth > maxItemWidth {
			maxItemWidth = itemWidth
		}
	}

	// 使用统一的项宽度
	itemWidth := maxItemWidth
	numItems := len(items)
	numCols := calculateOptimalColumns(numItems, itemWidth, itemHeight)
	numRows := (numItems + numCols - 1) / numCols

	// 计算图像尺寸
	width := numCols*itemWidth + padding*2
	height := numRows*itemHeight + padding*2

	// 创建图像
	img := image.NewRGBA(image.Rect(0, 0, width, height))

	// 填充白色背景
	draw.Draw(img, img.Bounds(), &image.Uniform{color.White}, image.Point{}, draw.Src)

	// 绘制每个图例项
	for i, item := range items {
		row := i / numCols
		col := i % numCols

		xPos := padding + col*itemWidth
		yPos := padding + row*itemHeight

		// 解析颜色
		symbolColor, err := parseColor(item.Color)
		if err != nil {
			log.Printf("解析颜色失败: %v", err)
			continue
		}

		// 根据几何类型绘制符号
		symbolYOffset := (itemHeight - symbolHeight) / 2
		drawSymbol(img, xPos, yPos+symbolYOffset, symbolWidth, symbolHeight, item.GeoType, symbolColor)

		// 绘制文字（垂直居中）
		textYOffset := itemHeight/2 + 5
		err = drawChineseText(img, xPos+textOffsetX, yPos+textYOffset, item.Property, 14, color.Black, ttfFont)
		if err != nil {
			log.Printf("绘制文字失败: %v", err)
		}
	}

	// 编码为PNG
	var buf bytes.Buffer
	err = png.Encode(&buf, img)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func calculateTextWidth(text string, fontSize float64, ttfFont *truetype.Font) int {
	opts := truetype.Options{
		Size: fontSize,
		DPI:  72,
	}
	face := truetype.NewFace(ttfFont, &opts)
	defer face.Close()

	width := 0
	for _, r := range text {
		advance, ok := face.GlyphAdvance(r)
		if !ok {
			width += int(fontSize)
			continue
		}
		width += advance.Round()
	}

	return width
}

func calculateOptimalColumns(numItems, itemWidth, itemHeight int) int {
	if numItems == 0 {
		return 1
	}

	optimalCols := int(math.Sqrt(float64(numItems)*float64(itemHeight)/float64(itemWidth) + 1))

	if optimalCols < 1 {
		optimalCols = 1
	}

	maxCols := 6
	if optimalCols > maxCols {
		optimalCols = maxCols
	}

	if optimalCols > numItems {
		optimalCols = numItems
	}

	return optimalCols
}
