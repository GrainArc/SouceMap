package views

import (
	"github.com/GrainArc/SouceMap/response"
	"github.com/GrainArc/SouceMap/services"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

type TextureHandler struct {
	service *services.TextureService
}

func NewTextureHandler() *TextureHandler {
	return &TextureHandler{
		service: services.NewTextureService(),
	}
}

// Upload 上传纹理图片
// @Summary 上传PNG纹理图片
// @Accept multipart/form-data
// @Param file formData file true "PNG图片文件"
// @Param name formData string true "纹理名称"
// @Param description formData string false "纹理描述"
func (h *TextureHandler) Upload(c *gin.Context) {
	// 获取上传文件
	file, err := c.FormFile("file")
	if err != nil {
		response.BadRequest(c, "请选择要上传的文件")
		return
	}
	ext := filepath.Ext(file.Filename)
	name := strings.TrimSuffix(file.Filename, ext)

	description := c.PostForm("description")

	// 调用服务层上传
	texture, err := h.service.Upload(&services.UploadRequest{
		Name:        name,
		Description: description,
		File:        file,
	})
	if err != nil {
		response.BadRequest(c, err.Error())
		return
	}

	// 返回时不包含图片数据
	response.SuccessWithMessage(c, "上传成功", gin.H{
		"id":          texture.ID,
		"name":        texture.Name,
		"mime_type":   texture.MimeType,
		"width":       texture.Width,
		"height":      texture.Height,
		"description": texture.Description,
	})
}

// List 获取纹理列表
// @Summary 获取纹理列表（分页）
// @Param page query int false "页码" default(1)
// @Param page_size query int false "每页数量" default(10)
func (h *TextureHandler) List(c *gin.Context) {
	// 获取分页参数
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 10
	}

	// 调用服务层获取列表
	items, total, err := h.service.List(page, pageSize)
	if err != nil {
		response.InternalError(c, "获取列表失败")
		return
	}

	response.Success(c, gin.H{
		"list":      items,
		"total":     total,
		"page":      page,
		"page_size": pageSize,
	})
}

// Get 获取单个纹理详情（包含图片数据）
// @Summary 获取纹理详情
// @Param id path int true "纹理ID"
func (h *TextureHandler) Get(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		response.BadRequest(c, "无效的ID")
		return
	}

	texture, err := h.service.GetByID(uint(id))
	if err != nil {
		response.NotFound(c, err.Error())
		return
	}

	response.Success(c, texture)
}

// GetImage 获取原始图片（直接返回PNG二进制数据）
// @Summary 获取原始PNG图片
// @Param id path int true "纹理ID"
// @Produce image/png
func (h *TextureHandler) GetImage(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		response.BadRequest(c, "无效的ID")
		return
	}

	imageData, err := h.service.GetImageData(uint(id))
	if err != nil {
		response.NotFound(c, err.Error())
		return
	}

	c.Header("Content-Type", "image/png")
	c.Header("Content-Disposition", "inline")
	c.Data(200, "image/png", imageData)
}

// Delete 删除纹理
// @Summary 删除纹理
// @Param id path int true "纹理ID"
func (h *TextureHandler) Delete(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		response.BadRequest(c, "无效的ID")
		return
	}

	if err := h.service.Delete(uint(id)); err != nil {
		response.NotFound(c, err.Error())
		return
	}

	response.SuccessWithMessage(c, "删除成功", nil)
}

// SetLayerTexture 设置图层纹理
// @Summary 设置图层纹理
// @Accept json
// @Produce json
// @Param request body LayerTextureSetting true "纹理设置参数"
// @Success 200 {object} response.Response
// @Failure 400 {object} response.Response
// @Failure 500 {object} response.Response
func (h *TextureHandler) SetLayerTexture(c *gin.Context) {
	var req services.LayerTextureSetting

	// 解析请求体
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "请求参数格式错误: "+err.Error())
		return
	}

	// 参数验证
	if req.LayerName == "" {
		response.BadRequest(c, "图层名称不能为空")
		return
	}
	if req.AttName == "" {
		response.BadRequest(c, "属性名称不能为空")
		return
	}
	if len(req.TextureSets) == 0 {
		response.BadRequest(c, "纹理集合不能为空")
		return
	}

	// 验证TextureID是否存在
	for _, ts := range req.TextureSets {
		if ts.TextureID == "" {
			response.BadRequest(c, "纹理ID不能为空")
			return
		}
		textureID, err := strconv.ParseUint(ts.TextureID, 10, 64)
		if err != nil {
			response.BadRequest(c, "无效的纹理ID: "+ts.TextureID)
			return
		}
		// 检查纹理是否存在
		_, err = h.service.GetByID(uint(textureID))
		if err != nil {
			response.NotFound(c, "纹理不存在: "+ts.TextureID)
			return
		}
	}

	// 调用服务层设置纹理
	err := h.service.SetLayerTexture(req.LayerName, req)
	if err != nil {
		response.InternalError(c, "设置纹理失败: "+err.Error())
		return
	}

	response.Success(c, gin.H{
		"message":        "纹理设置成功",
		"layer_name":     req.LayerName,
		"attribute_name": req.AttName,
	})
}

// GetLayerTexture 获取图层纹理设置
// @Summary 获取图层纹理设置
// @Accept json
// @Produce json
// @Param layer_name query string true "图层名称"

func (h *TextureHandler) GetLayerTexture(c *gin.Context) {
	layerName := c.Query("layer_name")

	// 参数验证
	if layerName == "" {
		response.BadRequest(c, "图层名称不能为空")
		return
	}

	// 调用服务层获取纹理设置
	textureSetting, err := h.service.GetLayerTexture(layerName)
	if err != nil {
		if err.Error() == "未找到匹配的图层" {
			response.NotFound(c, err.Error())
			return
		}
		response.InternalError(c, "获取纹理设置失败: "+err.Error())
		return
	}

	response.Success(c, textureSetting)
}

// 在 views/texture.go 中添加以下方法

// GetUsedTextures 获取所有已配置使用的纹理
// @Summary 获取所有已配置使用的纹理（从图层配置中提取并去重）
// @Produce json
func (h *TextureHandler) GetUsedTextures(c *gin.Context) {
	items, err := h.service.GetUsedTextures()
	if err != nil {
		response.InternalError(c, "获取已配置纹理失败: "+err.Error())
		return
	}

	response.Success(c, gin.H{
		"list":  items,
		"total": len(items),
	})
}
