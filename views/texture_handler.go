package views

import (
	"strconv"

	"github.com/GrainArc/SouceMap/response"
	"github.com/GrainArc/SouceMap/services"

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

	// 获取表单参数
	name := c.PostForm("name")
	if name == "" {
		name = file.Filename // 默认使用文件名
	}
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
