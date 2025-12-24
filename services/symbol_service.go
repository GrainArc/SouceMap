package services

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"mime/multipart"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/GrainArc/SouceMap/models"
	"gorm.io/gorm"
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
	AttValue string  `json:"att_value"`
	SymbolID string  `json:"symbol_id"`
	Scale    float64 `json:"scale,omitempty"`    // 图标缩放比例
	Rotation float64 `json:"rotation,omitempty"` // 图标旋转角度
	OffsetX  float64 `json:"offset_x,omitempty"` // X偏移
	OffsetY  float64 `json:"offset_y,omitempty"` // Y偏移
}

// Upload 上传图标
func (s *SymbolService) Upload(req *SymbolUploadRequest) (*models.Symbol, error) {
	// 验证文件类型
	ext := strings.ToLower(filepath.Ext(req.File.Filename))
	allowedExts := map[string]string{
		".png":  "image/png",
		".jpg":  "image/jpeg",
		".jpeg": "image/jpeg",
		".gif":  "image/gif",
		".svg":  "image/svg+xml",
	}

	mimeType, ok := allowedExts[ext]
	if !ok {
		return nil, errors.New("仅支持 PNG、JPG、GIF、SVG 格式的图片")
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

	// 获取图片尺寸（SVG特殊处理）
	var width, height int
	if ext == ".svg" {
		// SVG默认尺寸，实际可以解析SVG获取
		width, height = 64, 64
	} else {
		img, _, err := image.DecodeConfig(bytes.NewReader(imageData))
		if err != nil {
			return nil, errors.New("无法解析图片尺寸")
		}
		width, height = img.Width, img.Height
	}

	// 创建记录
	symbol := &models.Symbol{
		Name:        req.Name,
		MimeType:    mimeType,
		Width:       width,
		Height:      height,
		ImageData:   imageData,
		Description: req.Description,
		Category:    req.Category,
	}

	if err := s.db.Create(symbol).Error; err != nil {
		return nil, errors.New("保存图标失败")
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
	// 这里实现保存图层图标配置的逻辑
	// 可以保存到数据库或配置文件中
	// 示例：保存到JSON配置文件或数据库表

	// TODO: 根据实际需求实现配置存储逻辑
	return nil
}

// GetLayerSymbol 获取图层图标设置
func (s *SymbolService) GetLayerSymbol(layerName string) (*LayerSymbolSetting, error) {
	// 这里实现获取图层图标配置的逻辑
	// TODO: 根据实际需求实现配置读取逻辑

	return nil, errors.New("未找到匹配的图层")
}

// GetUsedSymbols 获取所有已配置使用的图标
func (s *SymbolService) GetUsedSymbols(host string) ([]SymbolListItem, error) {
	// 这里实现获取已使用图标的逻辑
	// TODO: 根据实际需求实现

	var symbols []models.Symbol
	if err := s.db.Select("id, name, mime_type, width, height, description, category, created_at, updated_at").
		Find(&symbols).Error; err != nil {
		return nil, err
	}

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
