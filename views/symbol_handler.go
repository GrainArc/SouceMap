package views

import (
	"path/filepath"
	"strconv"
	"strings"

	"github.com/GrainArc/SouceMap/response"
	"github.com/GrainArc/SouceMap/services"
	"github.com/gin-gonic/gin"
)

type SymbolHandler struct {
	service *services.SymbolService
}

func NewSymbolHandler() *SymbolHandler {
	return &SymbolHandler{
		service: services.NewSymbolService(),
	}
}

// Upload 上传图标图片
// @Summary 上传图标图片
// @Accept multipart/form-data
// @Param file formData file true "图片文件(PNG/JPG/GIF/SVG)"
// @Param name formData string false "图标名称(默认使用文件名)"
// @Param description formData string false "图标描述"
// @Param category formData string false "图标分类"
func (h *SymbolHandler) Upload(c *gin.Context) {
	// 获取上传文件
	file, err := c.FormFile("file")
	if err != nil {
		response.BadRequest(c, "请选择要上传的文件")
		return
	}

	ext := filepath.Ext(file.Filename)
	name := c.PostForm("name")
	if name == "" {
		name = strings.TrimSuffix(file.Filename, ext)
	}

	description := c.PostForm("description")
	category := c.PostForm("category")

	// 调用服务层上传
	symbol, err := h.service.Upload(&services.SymbolUploadRequest{
		Name:        name,
		Description: description,
		Category:    category,
		File:        file,
	})
	if err != nil {
		response.BadRequest(c, err.Error())
		return
	}

	response.SuccessWithMessage(c, "上传成功", gin.H{
		"id":          symbol.ID,
		"name":        symbol.Name,
		"mime_type":   symbol.MimeType,
		"width":       symbol.Width,
		"height":      symbol.Height,
		"description": symbol.Description,
		"category":    symbol.Category,
	})
}

// BatchUpload 批量上传图标
// @Summary 批量上传图标图片
// @Accept multipart/form-data
// @Param files formData file true "图片文件(支持多个)"
// @Param category formData string false "图标分类"
func (h *SymbolHandler) BatchUpload(c *gin.Context) {
	form, err := c.MultipartForm()
	if err != nil {
		response.BadRequest(c, "请选择要上传的文件")
		return
	}

	files := form.File["files"]
	if len(files) == 0 {
		response.BadRequest(c, "请选择要上传的文件")
		return
	}

	category := c.PostForm("category")

	results, errs := h.service.BatchUpload(files, category)

	var errMessages []string
	for _, e := range errs {
		errMessages = append(errMessages, e.Error())
	}

	response.Success(c, gin.H{
		"success_count": len(results),
		"error_count":   len(errs),
		"errors":        errMessages,
		"symbols":       results,
	})
}

// List 获取图标列表
// @Summary 获取图标列表（分页）
// @Param page query int false "页码" default(1)
// @Param page_size query int false "每页数量" default(10)
// @Param category query string false "图标分类"
func (h *SymbolHandler) List(c *gin.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))
	category := c.Query("category")

	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 10
	}

	items, total, err := h.service.List(page, pageSize, category)
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

// Get 获取单个图标详情（包含图片数据）
// @Summary 获取图标详情
// @Param id path int true "图标ID"
func (h *SymbolHandler) Get(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		response.BadRequest(c, "无效的ID")
		return
	}

	symbol, err := h.service.GetByID(uint(id))
	if err != nil {
		response.NotFound(c, err.Error())
		return
	}

	response.Success(c, symbol)
}

// GetImage 获取原始图片（直接返回二进制数据）
// @Summary 获取原始图片
// @Param id path int true "图标ID"
// @Produce image/png
func (h *SymbolHandler) GetImage(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		response.BadRequest(c, "无效的ID")
		return
	}

	imageData, mimeType, err := h.service.GetImageData(uint(id))
	if err != nil {
		response.NotFound(c, err.Error())
		return
	}

	c.Header("Content-Type", mimeType)
	c.Header("Content-Disposition", "inline")
	c.Header("Cache-Control", "public, max-age=86400") // 缓存1天
	c.Data(200, mimeType, imageData)
}

// Delete 删除图标
// @Summary 删除图标
// @Param id path int true "图标ID"
func (h *SymbolHandler) Delete(c *gin.Context) {
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

// Update 更新图标信息
// @Summary 更新图标信息
// @Accept json
// @Param id path int true "图标ID"
// @Param request body object true "更新参数"
func (h *SymbolHandler) Update(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 32)
	if err != nil {
		response.BadRequest(c, "无效的ID")
		return
	}

	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Category    string `json:"category"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "请求参数格式错误")
		return
	}

	if err := h.service.Update(uint(id), req.Name, req.Description, req.Category); err != nil {
		response.BadRequest(c, err.Error())
		return
	}

	response.SuccessWithMessage(c, "更新成功", nil)
}

// SetLayerSymbol 设置图层图标
// @Summary 设置图层图标
// @Accept json
// @Produce json
// @Param request body services.LayerSymbolSetting true "图标设置参数"
func (h *SymbolHandler) SetLayerSymbol(c *gin.Context) {
	var req services.LayerSymbolSetting

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
	if len(req.SymbolSets) == 0 {
		response.BadRequest(c, "图标集合不能为空")
		return
	}

	// 验证SymbolID是否存在
	for _, ss := range req.SymbolSets {
		if ss.SymbolID == "" {
			response.BadRequest(c, "图标ID不能为空")
			return
		}
		symbolID, err := strconv.ParseUint(ss.SymbolID, 10, 64)
		if err != nil {
			response.BadRequest(c, "无效的图标ID: "+ss.SymbolID)
			return
		}
		_, err = h.service.GetByID(uint(symbolID))
		if err != nil {
			response.NotFound(c, "图标不存在: "+ss.SymbolID)
			return
		}
	}

	err := h.service.SetLayerSymbol(req.LayerName, req)
	if err != nil {
		response.InternalError(c, "设置图标失败: "+err.Error())
		return
	}

	response.Success(c, gin.H{
		"message":        "图标设置成功",
		"layer_name":     req.LayerName,
		"attribute_name": req.AttName,
	})
}

// GetLayerSymbol 获取图层图标设置
// @Summary 获取图层图标设置
// @Param layer_name query string true "图层名称"
func (h *SymbolHandler) GetLayerSymbol(c *gin.Context) {
	layerName := c.Query("layer_name")

	if layerName == "" {
		response.BadRequest(c, "图层名称不能为空")
		return
	}

	symbolSetting, err := h.service.GetLayerSymbol(layerName)
	if err != nil {
		if err.Error() == "未找到匹配的图层" {
			response.NotFound(c, err.Error())
			return
		}
		response.InternalError(c, "获取图标设置失败: "+err.Error())
		return
	}

	response.Success(c, symbolSetting)
}

// GetUsedSymbols 获取所有已配置使用的图标
// @Summary 获取所有已配置使用的图标
func (h *SymbolHandler) GetUsedSymbols(c *gin.Context) {
	host := c.Request.Host
	items, err := h.service.GetUsedSymbols(host)
	if err != nil {
		response.InternalError(c, "获取已配置图标失败: "+err.Error())
		return
	}

	response.Success(c, gin.H{
		"list":  items,
		"total": len(items),
	})
}

// Search 搜索图标
// @Summary 搜索图标（支持ID、名称、描述的模糊匹配）
// @Param q query string true "搜索关键词"
// @Param page query int false "页码" default(1)
// @Param page_size query int false "每页数量" default(10)
// @Param category query string false "图标分类"
func (h *SymbolHandler) Search(c *gin.Context) {
	query := strings.TrimSpace(c.Query("q"))
	if query == "" {
		response.BadRequest(c, "搜索关键词不能为空")
		return
	}

	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))
	category := c.Query("category")

	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 10
	}

	items, total, err := h.service.Search(query, page, pageSize, c.Request.Host, category)
	if err != nil {
		response.InternalError(c, "搜索失败: "+err.Error())
		return
	}

	response.Success(c, gin.H{
		"list":      items,
		"total":     total,
		"page":      page,
		"page_size": pageSize,
		"query":     query,
	})
}

// GetCategories 获取所有图标分类
// @Summary 获取所有图标分类
func (h *SymbolHandler) GetCategories(c *gin.Context) {
	categories, err := h.service.GetCategories()
	if err != nil {
		response.InternalError(c, "获取分类失败: "+err.Error())
		return
	}

	response.Success(c, gin.H{
		"categories": categories,
	})
}
