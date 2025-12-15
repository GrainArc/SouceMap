package services

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"gorm.io/datatypes"
	"image"
	"image/png"
	"io"
	"mime/multipart"
	"strconv"
)

type TextureService struct{}

func NewTextureService() *TextureService {
	return &TextureService{}
}

// UploadRequest 上传请求参数
type UploadRequest struct {
	Name        string
	Description string
	File        *multipart.FileHeader
}

// TextureListItem 列表项（不包含图片数据）
type TextureListItem struct {
	ID          uint   `json:"id"`
	Name        string `json:"name"`
	MimeType    string `json:"mime_type"`
	Width       int    `json:"width"`
	Height      int    `json:"height"`
	Description string `json:"description"`
}

// ValidatePNG 验证文件是否为PNG格式
func (s *TextureService) ValidatePNG(file *multipart.FileHeader) error {
	// 检查文件扩展名
	if file.Filename[len(file.Filename)-4:] != ".png" {
		return errors.New("文件扩展名必须为.png")
	}

	// 打开文件验证PNG魔数
	f, err := file.Open()
	if err != nil {
		return errors.New("无法打开文件")
	}
	defer f.Close()

	// PNG文件魔数: 89 50 4E 47 0D 0A 1A 0A
	header := make([]byte, 8)
	if _, err := f.Read(header); err != nil {
		return errors.New("无法读取文件头")
	}

	pngMagic := []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}
	if !bytes.Equal(header, pngMagic) {
		return errors.New("文件不是有效的PNG格式")
	}

	return nil
}

// Upload 上传纹理图片
func (s *TextureService) Upload(req *UploadRequest) (*models.Texture, error) {
	// 验证PNG格式
	if err := s.ValidatePNG(req.File); err != nil {
		return nil, err
	}

	// 打开文件
	file, err := req.File.Open()
	if err != nil {
		return nil, errors.New("无法打开上传文件")
	}
	defer file.Close()

	// 读取文件内容
	imageBytes, err := io.ReadAll(file)
	if err != nil {
		return nil, errors.New("无法读取文件内容")
	}

	// 解码PNG获取尺寸
	img, err := png.Decode(bytes.NewReader(imageBytes))
	if err != nil {
		return nil, errors.New("无法解析PNG图片")
	}

	bounds := img.Bounds()
	width := bounds.Max.X - bounds.Min.X
	height := bounds.Max.Y - bounds.Min.Y

	// 创建纹理数据
	texture := &models.Texture{
		Name:        req.Name,
		MimeType:    "image/png",
		Width:       width,
		Height:      height,
		ImageData:   imageBytes,
		Description: req.Description,
	}

	// 使用 Upsert（如果存在则更新，不存在则创建）
	result := models.GetDB().
		Where("name = ?", req.Name).
		Assign(texture).
		FirstOrCreate(texture)

	if result.Error != nil {
		return nil, errors.New("保存纹理失败: " + result.Error.Error())
	}

	return texture, nil
}

// List 获取纹理列表（不包含图片数据）
func (s *TextureService) List(page, pageSize int) ([]TextureListItem, int64, error) {
	var textures []models.Texture
	var total int64

	db := models.GetDB()

	// 获取总数
	if err := db.Model(&models.Texture{}).Count(&total).Error; err != nil {
		return nil, 0, err
	}

	// 分页查询，只选择需要的字段（不包含image_data）
	offset := (page - 1) * pageSize
	if err := db.Select("id, name, mime_type, width, height, description").
		Order("id DESC").
		Offset(offset).
		Limit(pageSize).
		Find(&textures).Error; err != nil {
		return nil, 0, err
	}

	// 转换为列表项
	items := make([]TextureListItem, len(textures))
	for i, t := range textures {
		items[i] = TextureListItem{
			ID:          t.ID,
			Name:        t.Name,
			MimeType:    t.MimeType,
			Width:       t.Width,
			Height:      t.Height,
			Description: t.Description,
		}
	}

	return items, total, nil
}

// GetByID 根据ID获取纹理（包含图片数据）
func (s *TextureService) GetByID(id uint) (*models.Texture, error) {
	var texture models.Texture

	if err := models.GetDB().First(&texture, id).Error; err != nil {
		return nil, errors.New("纹理不存在")
	}

	return &texture, nil
}

// GetImageData 获取原始图片数据
func (s *TextureService) GetImageData(id uint) ([]byte, error) {
	texture, err := s.GetByID(id)
	if err != nil {
		return nil, err
	}
	return texture.ImageData, nil
}

// Delete 删除纹理
func (s *TextureService) Delete(id uint) error {
	result := models.GetDB().Delete(&models.Texture{}, id)
	if result.Error != nil {
		return errors.New("删除失败: " + result.Error.Error())
	}
	if result.RowsAffected == 0 {
		return errors.New("纹理不存在")
	}
	return nil
}

// GetRawImage 获取可直接显示的图片（返回PNG解码后的image.Image）
func (s *TextureService) GetRawImage(id uint) (image.Image, error) {
	imageData, err := s.GetImageData(id)
	if err != nil {
		return nil, err
	}

	img, err := png.Decode(bytes.NewReader(imageData))
	if err != nil {
		return nil, errors.New("解码PNG图片失败")
	}

	return img, nil
}

// SetLayerTexture 设置图层纹理

type LayerTextureSetting struct {
	LayerName   string       `json:"layer_name"`
	AttName     string       `json:"attribute_name"`
	TextureSets []TextureSet `json:"texture_sets"`
}
type TextureSet struct {
	Property    string `json:"property"`
	TextureID   string `json:"texture_id"`
	TextureName string `json:"texture_name"`
}

func (s *TextureService) SetLayerTexture(layerName string, textureSets LayerTextureSetting) error {
	// 构建TextureSet JSON数据
	textureSetJSON, err := json.Marshal(textureSets)
	if err != nil {
		return fmt.Errorf("纹理数据序列化失败: %w", err)
	}
	DB := models.DB
	// 更新MySchema表中的TextureSet字段
	// 根据Main字段（图层名称）和Type字段（属性名称）来更新
	result := DB.Model(&models.MySchema{}).
		Where("en = ?", layerName).
		Update("texture_set", datatypes.JSON(textureSetJSON))

	if result.Error != nil {
		return fmt.Errorf("更新数据库失败: %w", result.Error)
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("未找到匹配的图层或属性")
	}

	return nil
}

// GetLayerTexture 获取图层纹理设置
func (s *TextureService) GetLayerTexture(layerName string) (*LayerTextureSetting, error) {
	var mySchema models.MySchema

	// 查询MySchema表
	result := models.DB.Where("en = ?", layerName).First(&mySchema)

	if result.Error != nil {
		if result.Error.Error() == "record not found" {
			return nil, errors.New("未找到匹配的图层")
		}
		return nil, fmt.Errorf("数据库查询失败: %w", result.Error)
	}

	// 解析TextureSet JSON数据
	var textureSets []TextureSet
	if len(mySchema.TextureSet) > 0 {
		if err := json.Unmarshal(mySchema.TextureSet, &textureSets); err != nil {
			return nil, fmt.Errorf("纹理数据解析失败: %w", err)
		}
	}

	response := &LayerTextureSetting{
		LayerName:   layerName,
		TextureSets: textureSets,
	}

	return response, nil
}

// 在 services/texture.go 中添加以下方法

// GetUsedTextures 获取所有已配置的纹理（从MySchema表的TextureSet中提取并去重）
func (s *TextureService) GetUsedTextures() ([]TextureListItem, error) {
	var schemas []models.MySchema

	// 查询所有有TextureSet的记录
	if err := models.DB.Where("texture_set IS NOT NULL AND texture_set != '[]' AND texture_set != 'null'").
		Find(&schemas).Error; err != nil {
		return nil, fmt.Errorf("查询MySchema失败: %w", err)
	}

	// 使用map去重TextureID
	textureIDMap := make(map[uint]struct{})

	for _, schema := range schemas {
		if len(schema.TextureSet) == 0 {
			continue
		}

		// 尝试解析为 LayerTextureSetting 格式
		var setting LayerTextureSetting
		if err := json.Unmarshal(schema.TextureSet, &setting); err == nil && len(setting.TextureSets) > 0 {
			for _, ts := range setting.TextureSets {
				if ts.TextureID != "" {
					if id, err := strconv.ParseUint(ts.TextureID, 10, 64); err == nil {
						textureIDMap[uint(id)] = struct{}{}
					}
				}
			}
			continue
		}

		// 尝试解析为 []TextureSet 格式
		var textureSets []TextureSet
		if err := json.Unmarshal(schema.TextureSet, &textureSets); err == nil {
			for _, ts := range textureSets {
				if ts.TextureID != "" {
					if id, err := strconv.ParseUint(ts.TextureID, 10, 64); err == nil {
						textureIDMap[uint(id)] = struct{}{}
					}
				}
			}
		}
	}

	// 如果没有找到任何纹理ID
	if len(textureIDMap) == 0 {
		return []TextureListItem{}, nil
	}

	// 转换为ID切片
	textureIDs := make([]uint, 0, len(textureIDMap))
	for id := range textureIDMap {
		textureIDs = append(textureIDs, id)
	}

	// 查询Texture表中存在的记录
	var textures []models.Texture
	if err := models.GetDB().
		Select("id, name, mime_type, width, height, description").
		Where("id IN ?", textureIDs).
		Order("id DESC").
		Find(&textures).Error; err != nil {
		return nil, fmt.Errorf("查询Texture失败: %w", err)
	}

	// 转换为TextureListItem
	items := make([]TextureListItem, len(textures))
	for i, t := range textures {
		items[i] = TextureListItem{
			ID:          t.ID,
			Name:        t.Name,
			MimeType:    t.MimeType,
			Width:       t.Width,
			Height:      t.Height,
			Description: t.Description,
		}
	}

	return items, nil
}
