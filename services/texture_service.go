package services

import (
	"bytes"
	"encoding/base64"
	"errors"
	"image"
	"image/png"
	"io"
	"mime/multipart"

	"github.com/GrainArc/SouceMap/config"
	"github.com/GrainArc/SouceMap/models"

	"gorm.io/datatypes"
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

	// 检查MIME类型
	if file.Header.Get("Content-Type") != "image/png" {
		return errors.New("文件类型必须为image/png")
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

	// Base64编码
	base64Data := base64.StdEncoding.EncodeToString(imageBytes)

	// 创建纹理记录
	texture := &models.Texture{
		Name:        req.Name,
		MimeType:    "image/png",
		Width:       width,
		Height:      height,
		ImageData:   datatypes.JSON([]byte(`"` + base64Data + `"`)),
		Description: req.Description,
	}

	// 保存到数据库
	if err := config.GetDB().Create(texture).Error; err != nil {
		return nil, errors.New("保存纹理失败: " + err.Error())
	}

	return texture, nil
}

// List 获取纹理列表（不包含图片数据）
func (s *TextureService) List(page, pageSize int) ([]TextureListItem, int64, error) {
	var textures []models.Texture
	var total int64

	db := config.GetDB()

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

	if err := config.GetDB().First(&texture, id).Error; err != nil {
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

	// 解析JSON中的Base64字符串
	var base64Str string
	if err := texture.ImageData.UnmarshalJSON([]byte(texture.ImageData)); err != nil {
		// 直接尝试解码
		base64Str = string(texture.ImageData)
		// 去除可能的引号
		if len(base64Str) >= 2 && base64Str[0] == '"' {
			base64Str = base64Str[1 : len(base64Str)-1]
		}
	} else {
		base64Str = string(texture.ImageData)
		if len(base64Str) >= 2 && base64Str[0] == '"' {
			base64Str = base64Str[1 : len(base64Str)-1]
		}
	}

	// Base64解码
	imageData, err := base64.StdEncoding.DecodeString(base64Str)
	if err != nil {
		return nil, errors.New("解码图片数据失败")
	}

	return imageData, nil
}

// Delete 删除纹理
func (s *TextureService) Delete(id uint) error {
	result := config.GetDB().Delete(&models.Texture{}, id)
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
