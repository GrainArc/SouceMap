package services

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"image"
	"image/color"
	_ "image/gif"
	_ "image/jpeg"
	"image/png"
	_ "image/png"
	"io"
	"mime/multipart"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
)

type SymbolService struct {
	db *gorm.DB
}

func NewSymbolService() *SymbolService {
	return &SymbolService{
		db: models.GetDB(),
	}
}

// SymbolUploadRequest 上传请求参数
type SymbolUploadRequest struct {
	Name        string
	Description string
	Category    string
	File        *multipart.FileHeader
}

// SymbolListItem 列表项（不含图片数据）
type SymbolListItem struct {
	ID          uint   `json:"id"`
	Name        string `json:"name"`
	MimeType    string `json:"mime_type"`
	Width       int    `json:"width"`
	Height      int    `json:"height"`
	Description string `json:"description"`
	Category    string `json:"category"`
	ImageURL    string `json:"image_url"`
	CreatedAt   int64  `json:"created_at"`
	UpdatedAt   int64  `json:"updated_at"`
}

// SymbolDetail 详情（含Base64图片数据）
type SymbolDetail struct {
	ID          uint   `json:"id"`
	Name        string `json:"name"`
	MimeType    string `json:"mime_type"`
	Width       int    `json:"width"`
	Height      int    `json:"height"`
	Description string `json:"description"`
	Category    string `json:"category"`
	ImageBase64 string `json:"image_base64"`
	ImageURL    string `json:"image_url"`
	CreatedAt   int64  `json:"created_at"`
	UpdatedAt   int64  `json:"updated_at"`
}

// LayerSymbolSetting 图层图标设置
type LayerSymbolSetting struct {
	LayerName  string      `json:"layer_name"`
	AttName    string      `json:"att_name"`
	SymbolSets []SymbolSet `json:"symbol_sets"`
}

type SymbolSet struct {
	AttValue   string  `json:"att_value"`
	SymbolID   string  `json:"symbol_id"`
	SymbolName string  `json:"symbol_name"`
	Scale      float64 `json:"scale,omitempty"`    // 图标缩放比例
	Rotation   float64 `json:"rotation,omitempty"` // 图标旋转角度
	OffsetX    float64 `json:"offset_x,omitempty"` // X偏移
	OffsetY    float64 `json:"offset_y,omitempty"` // Y偏移
}

// adjustToEvenNumber 调整数字为2的倍数
func (s *SymbolService) adjustToEvenNumber(num int) int {
	if num <= 0 {
		return 2
	}
	if num%2 != 0 {
		num--
	}
	if num < 2 {
		num = 2
	}
	return num
}

// resizeImage 调整图片大小（使用双线性插值）
func (s *SymbolService) resizeImage(src image.Image, width, height int) image.Image {
	dst := image.NewRGBA(image.Rect(0, 0, width, height))

	srcBounds := src.Bounds()
	srcWidth := srcBounds.Max.X - srcBounds.Min.X
	srcHeight := srcBounds.Max.Y - srcBounds.Min.Y

	// 如果目标尺寸和源尺寸相同，直接返回
	if width == srcWidth && height == srcHeight {
		return src
	}

	// 计算缩放因子
	xRatio := float64(srcWidth) / float64(width)
	yRatio := float64(srcHeight) / float64(height)

	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			// 双线性插值
			srcX := float64(x) * xRatio
			srcY := float64(y) * yRatio

			// 获取四个相邻像素的坐标
			x1 := int(srcX)
			y1 := int(srcY)
			x2 := x1 + 1
			y2 := y1 + 1

			// 确保不超过源图片边界
			if x1 >= srcWidth {
				x1 = srcWidth - 1
			}
			if y1 >= srcHeight {
				y1 = srcHeight - 1
			}
			if x2 >= srcWidth {
				x2 = srcWidth - 1
			}
			if y2 >= srcHeight {
				y2 = srcHeight - 1
			}

			// 计算权重
			wx := srcX - float64(x1)
			wy := srcY - float64(y1)

			// 获取四个像素的颜色
			c1 := src.At(srcBounds.Min.X+x1, srcBounds.Min.Y+y1)
			c2 := src.At(srcBounds.Min.X+x2, srcBounds.Min.Y+y1)
			c3 := src.At(srcBounds.Min.X+x1, srcBounds.Min.Y+y2)
			c4 := src.At(srcBounds.Min.X+x2, srcBounds.Min.Y+y2)

			// 双线性插值计算
			r1, g1, b1, a1 := c1.RGBA()
			r2, g2, b2, a2 := c2.RGBA()
			r3, g3, b3, a3 := c3.RGBA()
			r4, g4, b4, a4 := c4.RGBA()

			// 插值计算
			r := uint8(((float64(r1>>8)*(1-wx)+float64(r2>>8)*wx)*(1-wy) +
				(float64(r3>>8)*(1-wx)+float64(r4>>8)*wx)*wy))
			g := uint8(((float64(g1>>8)*(1-wx)+float64(g2>>8)*wx)*(1-wy) +
				(float64(g3>>8)*(1-wx)+float64(g4>>8)*wx)*wy))
			b := uint8(((float64(b1>>8)*(1-wx)+float64(b2>>8)*wx)*(1-wy) +
				(float64(b3>>8)*(1-wx)+float64(b4>>8)*wx)*wy))
			a := uint8(((float64(a1>>8)*(1-wx)+float64(a2>>8)*wx)*(1-wy) +
				(float64(a3>>8)*(1-wx)+float64(a4>>8)*wx)*wy))

			dst.SetRGBA(x, y, color.RGBA{R: r, G: g, B: b, A: a})
		}
	}

	return dst
}

// processImageToPNG 处理图片并转换为PNG格式，确保尺寸为2的倍数且文件大小小于1MB
func (s *SymbolService) processImageToPNG(imageData []byte) ([]byte, int, int, error) {
	// 解码图片
	img, _, err := image.Decode(bytes.NewReader(imageData))
	if err != nil {
		return nil, 0, 0, errors.New("无法解析图片")
	}

	// 获取原始尺寸
	bounds := img.Bounds()
	originalWidth := bounds.Dx()
	originalHeight := bounds.Dy()

	// 调整尺寸为2的倍数
	adjustedWidth := s.adjustToEvenNumber(originalWidth)
	adjustedHeight := s.adjustToEvenNumber(originalHeight)

	// 如果尺寸需要调整，则重新调整图片大小
	var processedImg image.Image
	if adjustedWidth != originalWidth || adjustedHeight != originalHeight {
		processedImg = s.resizeImage(img, adjustedWidth, adjustedHeight)
	} else {
		processedImg = img
	}

	// 转换为PNG格式
	var buf bytes.Buffer
	if err := png.Encode(&buf, processedImg); err != nil {
		return nil, 0, 0, errors.New("转换为PNG格式失败")
	}

	pngBytes := buf.Bytes()
	currentWidth := adjustedWidth
	currentHeight := adjustedHeight

	// 检查文件大小，如果超过1MB则逐步压缩
	const maxSize = 1024 * 1024 // 1MB
	compressionRatio := 0.8     // 每次压缩到80%

	for len(pngBytes) > maxSize && (currentWidth > 2 || currentHeight > 2) {
		// 按比例缩小尺寸
		newWidth := int(float64(currentWidth) * compressionRatio)
		newHeight := int(float64(currentHeight) * compressionRatio)

		// 确保是2的倍数
		newWidth = s.adjustToEvenNumber(newWidth)
		newHeight = s.adjustToEvenNumber(newHeight)

		// 最小尺寸不能小于2
		if newWidth < 2 {
			newWidth = 2
		}
		if newHeight < 2 {
			newHeight = 2
		}

		// 如果尺寸没有变化，跳出循环避免无限循环
		if newWidth == currentWidth && newHeight == currentHeight {
			break
		}

		currentWidth = newWidth
		currentHeight = newHeight

		// 重新调整和编码
		resizedImg := s.resizeImage(processedImg, currentWidth, currentHeight)
		buf.Reset()
		if err := png.Encode(&buf, resizedImg); err != nil {
			return nil, 0, 0, errors.New("压缩PNG图片失败")
		}
		pngBytes = buf.Bytes()
	}

	// 如果仍然超过1MB，返回错误
	if len(pngBytes) > maxSize {
		return nil, 0, 0, errors.New("无法将文件压缩到1MB以内，请上传更小的原始图片")
	}

	return pngBytes, currentWidth, currentHeight, nil
}

// parseSVGDimensions 解析SVG文件的尺寸（简单实现）
func (s *SymbolService) parseSVGDimensions(svgData []byte) (int, int) {
	// 这里是一个简单的SVG尺寸解析实现
	// 实际项目中可能需要更复杂的SVG解析库
	svgContent := string(svgData)

	// 默认尺寸
	width, height := 64, 64

	// 简单的正则匹配width和height属性
	// 注意：这是一个简化的实现，实际使用中建议使用专门的SVG解析库
	if strings.Contains(svgContent, "width=") && strings.Contains(svgContent, "height=") {
		// 这里可以添加更复杂的解析逻辑
		// 为了简化，暂时使用默认值
	}

	return width, height
}

// Upload 上传图标
func (s *SymbolService) Upload(req *SymbolUploadRequest) (*models.Symbol, error) {
	// 验证文件类型
	ext := strings.ToLower(filepath.Ext(req.File.Filename))
	allowedExts := map[string]bool{
		".png":  true,
		".jpg":  true,
		".jpeg": true,
		".gif":  true,
		".svg":  true,
		".bmp":  true,
		".webp": true,
	}

	if !allowedExts[ext] {
		return nil, errors.New("仅支持 PNG、JPG、JPEG、GIF、BMP、WEBP、SVG 格式的图片")
	}

	// 检查文件大小（限制原始文件不超过10MB）
	const maxOriginalSize = 10 * 1024 * 1024 // 10MB
	if req.File.Size > maxOriginalSize {
		return nil, errors.New("文件大小不能超过10MB")
	}

	// 读取文件内容
	file, err := req.File.Open()
	if err != nil {
		return nil, errors.New("无法读取文件")
	}
	defer file.Close()

	imageData, err := io.ReadAll(file)
	if err != nil {
		return nil, errors.New("读取文件内容失败")
	}

	var finalImageData []byte
	var width, height int
	var mimeType string

	if ext == ".svg" {
		// SVG保持原格式
		finalImageData = imageData
		mimeType = "image/svg+xml"

		// 解析SVG尺寸
		width, height = s.parseSVGDimensions(imageData)

		// 确保SVG尺寸也是2的倍数
		width = s.adjustToEvenNumber(width)
		height = s.adjustToEvenNumber(height)

		// 检查SVG文件大小
		const maxSVGSize = 1024 * 1024 // 1MB
		if len(finalImageData) > maxSVGSize {
			return nil, errors.New("SVG文件大小不能超过1MB")
		}
	} else {
		// 其他格式转换为PNG并进行压缩优化
		var err error
		finalImageData, width, height, err = s.processImageToPNG(imageData)
		if err != nil {
			return nil, err
		}
		mimeType = "image/png"
	}

	// 创建记录
	symbol := &models.Symbol{
		Name:        req.Name,
		MimeType:    mimeType,
		Width:       width,
		Height:      height,
		ImageData:   finalImageData,
		Description: req.Description,
		Category:    req.Category,
	}

	// 使用 Upsert（如果存在则更新，不存在则创建）
	result := s.db.Where("name = ? AND category = ?", req.Name, req.Category).
		Assign(symbol).
		FirstOrCreate(symbol)

	if result.Error != nil {
		return nil, errors.New("保存图标失败: " + result.Error.Error())
	}

	return symbol, nil
}

// List 获取图标列表（分页）
func (s *SymbolService) List(page, pageSize int, category string) ([]SymbolListItem, int64, error) {
	var total int64
	var symbols []models.Symbol

	query := s.db.Model(&models.Symbol{})

	// 按分类筛选
	if category != "" {
		query = query.Where("category = ?", category)
	}

	// 获取总数
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	// 分页查询（不查询ImageData字段）
	offset := (page - 1) * pageSize
	if err := query.Select("id, name, mime_type, width, height, description, category, created_at, updated_at").
		Order("created_at DESC").
		Offset(offset).
		Limit(pageSize).
		Find(&symbols).Error; err != nil {
		return nil, 0, err
	}

	// 转换为列表项
	items := make([]SymbolListItem, len(symbols))
	for i, sym := range symbols {
		items[i] = SymbolListItem{
			ID:          sym.ID,
			Name:        sym.Name,
			MimeType:    sym.MimeType,
			Width:       sym.Width,
			Height:      sym.Height,
			Description: sym.Description,
			Category:    sym.Category,
			ImageURL:    fmt.Sprintf("/symbols/%d/image", sym.ID),
			CreatedAt:   sym.CreatedAt,
			UpdatedAt:   sym.UpdatedAt,
		}
	}

	return items, total, nil
}

// GetByID 根据ID获取图标详情
func (s *SymbolService) GetByID(id uint) (*SymbolDetail, error) {
	var symbol models.Symbol
	if err := s.db.First(&symbol, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("图标不存在")
		}
		return nil, err
	}

	// 转换为Base64
	base64Data := base64.StdEncoding.EncodeToString(symbol.ImageData)

	return &SymbolDetail{
		ID:          symbol.ID,
		Name:        symbol.Name,
		MimeType:    symbol.MimeType,
		Width:       symbol.Width,
		Height:      symbol.Height,
		Description: symbol.Description,
		Category:    symbol.Category,
		ImageBase64: fmt.Sprintf("data:%s;base64,%s", symbol.MimeType, base64Data),
		ImageURL:    fmt.Sprintf("/symbols/%d/image", symbol.ID),
		CreatedAt:   symbol.CreatedAt,
		UpdatedAt:   symbol.UpdatedAt,
	}, nil
}

// GetImageData 获取原始图片数据
func (s *SymbolService) GetImageData(id uint) ([]byte, string, error) {
	var symbol models.Symbol
	if err := s.db.Select("image_data, mime_type").First(&symbol, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, "", errors.New("图标不存在")
		}
		return nil, "", err
	}
	return symbol.ImageData, symbol.MimeType, nil
}

// Delete 删除图标
func (s *SymbolService) Delete(id uint) error {
	result := s.db.Delete(&models.Symbol{}, id)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return errors.New("图标不存在")
	}
	return nil
}

// Update 更新图标信息
func (s *SymbolService) Update(id uint, name, description, category string) error {
	updates := map[string]interface{}{}
	if name != "" {
		updates["name"] = name
	}
	if description != "" {
		updates["description"] = description
	}
	if category != "" {
		updates["category"] = category
	}

	if len(updates) == 0 {
		return errors.New("没有需要更新的字段")
	}

	result := s.db.Model(&models.Symbol{}).Where("id = ?", id).Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return errors.New("图标不存在")
	}
	return nil
}

// SetLayerSymbol 设置图层图标
func (s *SymbolService) SetLayerSymbol(layerName string, setting LayerSymbolSetting) error {
	SymbolSetJSON, err := json.Marshal(setting)
	if err != nil {
		return fmt.Errorf("数据序列化失败: %w", err)
	}
	DB := models.DB

	// 根据Main字段（图层名称）和Type字段（属性名称）来更新
	result := DB.Model(&models.MySchema{}).
		Where("en = ?", layerName).
		Update("symbol_set", datatypes.JSON(SymbolSetJSON))

	if result.Error != nil {
		return fmt.Errorf("更新数据库失败: %w", result.Error)
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("未找到匹配的图层或属性")
	}

	return nil
}

// GetLayerSymbol 获取图层图标设置
func (s *SymbolService) GetLayerSymbol(layerName string) (*LayerSymbolSetting, error) {
	var mySchema models.MySchema
	result := models.DB.Where("en = ?", layerName).First(&mySchema)
	if result.Error != nil {
		if result.Error.Error() == "record not found" {
			return nil, errors.New("未找到匹配的图层")
		}
		return nil, fmt.Errorf("数据库查询失败: %w", result.Error)
	}
	var SymbolSets LayerSymbolSetting
	if len(mySchema.SymbolSet) > 0 {
		if err := json.Unmarshal(mySchema.SymbolSet, &SymbolSets); err != nil {
			return nil, fmt.Errorf("纹理数据解析失败: %w", err)
		}
	}
	return &SymbolSets, nil
}

// GetUsedSymbols 获取所有已配置使用的图标
func (s *SymbolService) GetUsedSymbols(host string) ([]SymbolListItem, error) {
	var schemas []models.MySchema
	if err := models.DB.Where("symbol_set IS NOT NULL AND symbol_set != '[]' AND symbol_set != 'null'").
		Find(&schemas).Error; err != nil {
		return nil, fmt.Errorf("查询MySchema失败: %w", err)
	}
	symbolIDMap := make(map[uint]struct{})
	for _, schema := range schemas {
		if len(schema.SymbolSet) == 0 {
			continue
		}

		// 尝试解析为 LayerSymbolSetting 格式
		var setting LayerSymbolSetting
		if err := json.Unmarshal(schema.SymbolSet, &setting); err == nil && len(setting.SymbolSets) > 0 {
			for _, ts := range setting.SymbolSets {
				if ts.SymbolID != "" {
					if id, err := strconv.ParseUint(ts.SymbolID, 10, 64); err == nil {
						symbolIDMap[uint(id)] = struct{}{}
					}
				}
			}
			continue
		}
		var symbolSets []SymbolSet
		if err := json.Unmarshal(schema.SymbolSet, &symbolSets); err == nil {
			for _, ts := range symbolSets {
				if ts.SymbolID != "" {
					if id, err := strconv.ParseUint(ts.SymbolID, 10, 64); err == nil {
						symbolIDMap[uint(id)] = struct{}{}
					}
				}
			}
		}
	}

	// 如果没有找到任何纹理ID
	if len(symbolIDMap) == 0 {
		return []SymbolListItem{}, nil
	}

	// 转换为ID切片
	symbolIDs := make([]uint, 0, len(symbolIDMap))
	for id := range symbolIDMap {
		symbolIDs = append(symbolIDs, id)
	}

	// 查询symbol表中存在的记录
	var symbols []models.Symbol
	if err := models.GetDB().
		Select("id, name, mime_type, width, height, description").
		Where("id IN ?", symbolIDs).
		Order("id DESC").
		Find(&symbols).Error; err != nil {
		return nil, fmt.Errorf("查询失败: %w", err)
	}

	items := make([]SymbolListItem, len(symbols))
	for i, t := range symbols {
		url := &url.URL{
			Scheme: "http",
			Host:   host,
			Path:   fmt.Sprintf("/symbols/%d/image", t.ID),
		}
		items[i] = SymbolListItem{
			ID:          t.ID,
			Name:        t.Name,
			MimeType:    t.MimeType,
			Width:       t.Width,
			Height:      t.Height,
			Description: t.Description,
			ImageURL:    url.String(),
		}
	}

	return items, nil

}

// Search 搜索图标
func (s *SymbolService) Search(query string, page, pageSize int, host string, category string) ([]SymbolListItem, int64, error) {
	var total int64
	var symbols []models.Symbol

	// 构建查询
	dbQuery := s.db.Model(&models.Symbol{})

	// 尝试按ID精确匹配
	if id, err := strconv.ParseUint(query, 10, 32); err == nil {
		dbQuery = dbQuery.Where("id = ? OR name LIKE ? OR description LIKE ?", id, "%"+query+"%", "%"+query+"%")
	} else {
		dbQuery = dbQuery.Where("name LIKE ? OR description LIKE ?", "%"+query+"%", "%"+query+"%")
	}

	// 按分类筛选
	if category != "" {
		dbQuery = dbQuery.Where("category = ?", category)
	}

	// 获取总数
	if err := dbQuery.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	// 分页查询
	offset := (page - 1) * pageSize
	if err := dbQuery.Select("id, name, mime_type, width, height, description, category, created_at, updated_at").
		Order("created_at DESC").
		Offset(offset).
		Limit(pageSize).
		Find(&symbols).Error; err != nil {
		return nil, 0, err
	}

	// 转换为列表项
	items := make([]SymbolListItem, len(symbols))
	for i, sym := range symbols {
		items[i] = SymbolListItem{
			ID:          sym.ID,
			Name:        sym.Name,
			MimeType:    sym.MimeType,
			Width:       sym.Width,
			Height:      sym.Height,
			Description: sym.Description,
			Category:    sym.Category,
			ImageURL:    fmt.Sprintf("http://%s/symbols/%d/image", host, sym.ID),
			CreatedAt:   sym.CreatedAt,
			UpdatedAt:   sym.UpdatedAt,
		}
	}

	return items, total, nil
}

// GetCategories 获取所有图标分类
func (s *SymbolService) GetCategories() ([]string, error) {
	var categories []string
	if err := s.db.Model(&models.Symbol{}).
		Distinct("category").
		Where("category != ''").
		Pluck("category", &categories).Error; err != nil {
		return nil, err
	}
	return categories, nil
}

// BatchUpload 批量上传图标
func (s *SymbolService) BatchUpload(files []*multipart.FileHeader, category string) ([]models.Symbol, []error) {
	var results []models.Symbol
	var errs []error

	for _, file := range files {
		ext := filepath.Ext(file.Filename)
		name := strings.TrimSuffix(file.Filename, ext)

		symbol, err := s.Upload(&SymbolUploadRequest{
			Name:     name,
			Category: category,
			File:     file,
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("文件 %s 上传失败: %v", file.Filename, err))
		} else {
			results = append(results, *symbol)
		}
	}

	return results, errs
}
