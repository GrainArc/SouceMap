package ImgHandler

import (
	"bytes"
	"fmt"
	"image"
	"image/draw"
	"image/jpeg"
	"image/png"
)

// Position 定义鹰眼图的位置
type Position int

const (
	PositionTopLeft Position = iota
	PositionTopRight
	PositionBottomLeft
	PositionBottomRight
	PositionTop    // 正上（水平居中）
	PositionBottom // 正下（水平居中）
	PositionLeft   // 正左（垂直居中）
	PositionRight  // 正右（垂直居中）
)

// EmbedEagleEye 将A图片镶嵌在B图片的指定位置作为鹰眼图
// imageA: 要镶嵌的图片数据
// imageB: 主图片数据
// scale: A图片相对于B图片的缩放比例 (0-1之间，例如0.2表示A图片宽度为B图片宽度的20%)
// padding: 鹰眼图距离边角的内边距（像素）
// position: 鹰眼图的位置（左上、右上、左下、右下、正上、正下、正左、正右）
// 返回合成后的图片数据
func EmbedEagleEye(imageA, imageB []byte, scale float64, padding int, position Position) ([]byte, error) {
	// 解码图片A
	imgA, _, err := image.Decode(bytes.NewReader(imageA))
	if err != nil {
		return nil, fmt.Errorf("解码图片A失败: %v", err)
	}

	// 解码图片B
	imgB, formatB, err := image.Decode(bytes.NewReader(imageB))
	if err != nil {
		return nil, fmt.Errorf("解码图片B失败: %v", err)
	}

	// 获取图片B的尺寸
	boundsB := imgB.Bounds()
	widthB := boundsB.Dx()
	heightB := boundsB.Dy()

	// 计算图片A缩放后的尺寸
	boundsA := imgA.Bounds()
	originalWidthA := boundsA.Dx()
	originalHeightA := boundsA.Dy()

	newWidthA := int(float64(widthB) * scale)
	newHeightA := int(float64(originalHeightA) * float64(newWidthA) / float64(originalWidthA))

	// 缩放图片A
	scaledImgA := resizeImage(imgA, newWidthA, newHeightA)

	// 创建新的画布（基于图片B）
	canvas := image.NewRGBA(boundsB)

	// 将图片B绘制到画布上
	draw.Draw(canvas, boundsB, imgB, image.Point{}, draw.Src)

	// 根据位置参数计算图片A的坐标
	posX, posY := calculatePosition(widthB, heightB, newWidthA, newHeightA, padding, position)

	// 确保位置不会超出边界
	if posX < 0 {
		posX = 0
	}
	if posY < 0 {
		posY = 0
	}
	if posX+newWidthA > widthB {
		posX = widthB - newWidthA
	}
	if posY+newHeightA > heightB {
		posY = heightB - newHeightA
	}

	// 将缩放后的图片A绘制到画布的指定位置
	eagleEyeRect := image.Rect(posX, posY, posX+newWidthA, posY+newHeightA)
	draw.Draw(canvas, eagleEyeRect, scaledImgA, image.Point{}, draw.Over)

	// 将合成后的图片编码为字节数组
	var buf bytes.Buffer

	// 根据原图B的格式进行编码
	switch formatB {
	case "jpeg", "jpg":
		err = jpeg.Encode(&buf, canvas, &jpeg.Options{Quality: 95})
	case "png":
		err = png.Encode(&buf, canvas)
	default:
		// 默认使用PNG格式
		err = png.Encode(&buf, canvas)
	}

	if err != nil {
		return nil, fmt.Errorf("编码图片失败: %v", err)
	}

	return buf.Bytes(), nil
}

// calculatePosition 根据位置参数计算鹰眼图的坐标
func calculatePosition(widthB, heightB, widthA, heightA, padding int, position Position) (int, int) {
	var posX, posY int

	switch position {
	case PositionTopLeft:
		// 左上角
		posX = padding
		posY = padding
	case PositionTopRight:
		// 右上角
		posX = widthB - widthA - padding
		posY = padding
	case PositionBottomLeft:
		// 左下角
		posX = padding
		posY = heightB - heightA - padding
	case PositionBottomRight:
		// 右下角
		posX = widthB - widthA - padding
		posY = heightB - heightA - padding
	case PositionTop:
		// 正上（水平居中）
		posX = (widthB - widthA) / 2
		posY = padding
	case PositionBottom:
		// 正下（水平居中）
		posX = (widthB - widthA) / 2
		posY = heightB - heightA - padding
	case PositionLeft:
		// 正左（垂直居中）
		posX = padding
		posY = (heightB - heightA) / 2
	case PositionRight:
		// 正右（垂直居中）
		posX = widthB - widthA - padding
		posY = (heightB - heightA) / 2
	default:
		// 默认右下角
		posX = widthB - widthA - padding
		posY = heightB - heightA - padding
	}

	return posX, posY
}

// resizeImage 使用最近邻插值算法缩放图片
func resizeImage(img image.Image, newWidth, newHeight int) image.Image {
	bounds := img.Bounds()
	oldWidth := bounds.Dx()
	oldHeight := bounds.Dy()

	newImg := image.NewRGBA(image.Rect(0, 0, newWidth, newHeight))

	xRatio := float64(oldWidth) / float64(newWidth)
	yRatio := float64(oldHeight) / float64(newHeight)

	for y := 0; y < newHeight; y++ {
		for x := 0; x < newWidth; x++ {
			srcX := int(float64(x) * xRatio)
			srcY := int(float64(y) * yRatio)

			// 边界检查
			if srcX >= oldWidth {
				srcX = oldWidth - 1
			}
			if srcY >= oldHeight {
				srcY = oldHeight - 1
			}

			newImg.Set(x, y, img.At(srcX+bounds.Min.X, srcY+bounds.Min.Y))
		}
	}

	return newImg
}

// EmbedEagleEyeWithDefaults 使用默认参数的便捷函数
// 默认缩放比例为0.2，内边距为10像素，位置为右下角
func EmbedEagleEyeWithDefaults(imageA, imageB []byte) ([]byte, error) {
	return EmbedEagleEye(imageA, imageB, 0.2, 10, PositionBottomRight)
}
