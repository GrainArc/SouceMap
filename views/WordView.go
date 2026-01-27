package views

import (
	"embed"
	"encoding/json"
	"fmt"
	"gitee.com/gooffice/gooffice/document"
	"github.com/GrainArc/SouceMap/WordGenerator"
	"github.com/GrainArc/SouceMap/config"
	"github.com/GrainArc/SouceMap/models"
	"github.com/GrainArc/SouceMap/services"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/paulmach/orb/geojson"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

//go:embed fonts/template.docx
var templateFS embed.FS

func (uc *UserController) QSReport(c *gin.Context) {
	geo := c.PostForm("geo")
	var XM struct {
		Features []*geojson.Feature `json:"features"`
	}
	json.Unmarshal([]byte(geo), &XM)
	features := geojson.NewFeatureCollection()
	features.Features = XM.Features

	doc, _ := document.Open("./word/权属说明.docx")
	defer doc.Close()
	//制作界址点成果表
	WordGenerator.BoundaryPointsTable(doc, features)
	//输出word
	host := c.Request.Host
	taskid := uuid.New().String()
	homeDir, _ := os.UserHomeDir()
	OutFilePath := filepath.Join(homeDir, "BoundlessMap", "OutFile")
	path := filepath.Join(OutFilePath, taskid)
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		// 处理文件夹创建错误
		log.Println("创建文件夹失败：", err)
		c.String(http.StatusInternalServerError, "创建文件夹失败")
		return
	}
	doc.SaveToFile(config.Download + "/权属调查报告.docx")
	doc.SaveToFile(path + "/权属调查报告.docx")
	url := &url.URL{
		Scheme: "http",
		Host:   host,
		Path:   "/geo/OutFile/" + taskid + "/权属调查报告.docx",
	}
	outurl := url.String()

	c.String(http.StatusOK, outurl)

}

// 保存报告配置
type SaveReportConfigRequest struct {
	ReportName string               `json:"report_name" binding:"required"`
	Content    []models.ContentItem `json:"content" binding:"required"`
}

// SaveReportConfigResponse 保存报告配置响应
type SaveReportConfigResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		ReportID int64    `json:"report_id"`
		Layers   []string `json:"layers"`
	} `json:"data"`
}

// GenerateReportRequest 生成报告请求
type GenerateReportRequest struct {
	ReportID int64                      `json:"report_id" binding:"required"`
	GeoJSON  *geojson.FeatureCollection `json:"geojson" binding:"required"`
}

// GenerateReportResponse 生成报告响应
type GenerateReportResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		FilePath    string `json:"file_path"`
		DownloadURL string `json:"download_url"`
		FileName    string `json:"file_name"`
	} `json:"data"`
}

// extractLayersFromContent 从ContentItem中提取所有涉及的图层
func extractLayersFromContent(contentItems []models.ContentItem) []string {
	layerMap := make(map[string]bool)
	var layers []string

	for _, item := range contentItems {
		var sourceLayer string

		switch item.Type {
		case "image":
			configJSON, err := json.Marshal(item.Config)
			if err != nil {
				continue
			}
			var imgConfig models.ImageConfig
			if err := json.Unmarshal(configJSON, &imgConfig); err != nil {
				continue
			}
			sourceLayer = imgConfig.SourceLayer

		}

		// 如果图层不为空且未添加过，则添加到列表
		if sourceLayer != "" && !layerMap[sourceLayer] {
			layerMap[sourceLayer] = true
			layers = append(layers, sourceLayer)
		}
	}

	return layers
}

// 新增报告
func (uc *UserController) SaveReportConfig(c *gin.Context) {
	var req SaveReportConfigRequest
	// 绑定并验证请求参数
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, SaveReportConfigResponse{
			Code:    400,
			Message: fmt.Sprintf("请求参数错误: %v", err),
		})
		return
	}

	// 验证内容配置
	if len(req.Content) == 0 {
		c.JSON(http.StatusBadRequest, SaveReportConfigResponse{
			Code:    400,
			Message: "报告内容不能为空",
		})
		return
	}

	// 从内容中提取图层
	layers := extractLayersFromContent(req.Content)

	// 序列化图层数据
	layersJSON, err := json.Marshal(layers)
	if err != nil {
		c.JSON(http.StatusInternalServerError, SaveReportConfigResponse{
			Code:    500,
			Message: fmt.Sprintf("图层数据序列化失败: %v", err),
		})
		return
	}

	// 序列化内容数据
	contentJSON, err := json.Marshal(req.Content)
	if err != nil {
		c.JSON(http.StatusInternalServerError, SaveReportConfigResponse{
			Code:    500,
			Message: fmt.Sprintf("内容数据序列化失败: %v", err),
		})
		return
	}

	// 创建报告记录
	report := models.Report{
		ReportName: req.ReportName,
		Layers:     layersJSON,
		Content:    contentJSON,
	}

	// 保存到数据库
	DB := models.DB
	if err := DB.Create(&report).Error; err != nil {
		c.JSON(http.StatusInternalServerError, SaveReportConfigResponse{
			Code:    500,
			Message: fmt.Sprintf("保存报告配置失败: %v", err),
		})
		return
	}

	// 返回成功响应
	var resp SaveReportConfigResponse
	resp.Code = 200
	resp.Message = "报告配置保存成功"
	resp.Data.ReportID = report.ID
	resp.Data.Layers = layers

	c.JSON(http.StatusOK, resp)
}

// 更新报告
type UpdateReportConfigRequest struct {
	ID         int64                `json:"id" binding:"required"`
	ReportName string               `json:"report_name" binding:"required"`
	Content    []models.ContentItem `json:"content" binding:"required"`
}

func (uc *UserController) UpdateReportConfig(c *gin.Context) {
	var req UpdateReportConfigRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, SaveReportConfigResponse{
			Code:    400,
			Message: fmt.Sprintf("请求参数错误: %v", err),
		})
		return
	}

	DB := models.DB

	// 查询报告是否存在
	var report models.Report
	if err := DB.First(&report, req.ID).Error; err != nil {
		c.JSON(http.StatusNotFound, SaveReportConfigResponse{
			Code:    404,
			Message: "报告配置不存在",
		})
		return
	}

	// 从内容中提取图层
	layers := extractLayersFromContent(req.Content)

	// 序列化数据
	layersJSON, err := json.Marshal(layers)
	if err != nil {
		c.JSON(http.StatusInternalServerError, SaveReportConfigResponse{
			Code:    500,
			Message: fmt.Sprintf("图层数据序列化失败: %v", err),
		})
		return
	}

	contentJSON, err := json.Marshal(req.Content)
	if err != nil {
		c.JSON(http.StatusInternalServerError, SaveReportConfigResponse{
			Code:    500,
			Message: fmt.Sprintf("内容数据序列化失败: %v", err),
		})
		return
	}

	// 更新报告
	report.ReportName = req.ReportName
	report.Layers = layersJSON
	report.Content = contentJSON

	if err := DB.Save(&report).Error; err != nil {
		c.JSON(http.StatusInternalServerError, SaveReportConfigResponse{
			Code:    500,
			Message: fmt.Sprintf("更新报告配置失败: %v", err),
		})
		return
	}

	var resp SaveReportConfigResponse
	resp.Code = 200
	resp.Message = "报告配置更新成功"
	resp.Data.ReportID = report.ID
	resp.Data.Layers = layers

	c.JSON(http.StatusOK, resp)
}

// 制作报告
func (uc *UserController) GenerateReport(c *gin.Context) {
	// 1. 获取报告ID
	reportIDStr := c.PostForm("id")
	if reportIDStr == "" {
		c.JSON(http.StatusBadRequest, GenerateReportResponse{
			Code:    400,
			Message: "缺少报告ID",
		})
		return
	}

	reportID, err := strconv.ParseInt(reportIDStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, GenerateReportResponse{
			Code:    400,
			Message: "无效的报告ID",
		})
		return
	}

	// 2. 获取GeoJSON数据
	geojsonStr := c.PostForm("geojson")
	if geojsonStr == "" {
		c.JSON(http.StatusBadRequest, GenerateReportResponse{
			Code:    400,
			Message: "缺少GeoJSON数据",
		})
		return
	}

	// 解析GeoJSON
	var geoJSON geojson.FeatureCollection
	if err := json.Unmarshal([]byte(geojsonStr), &geoJSON); err != nil {
		c.JSON(http.StatusBadRequest, GenerateReportResponse{
			Code:    400,
			Message: fmt.Sprintf("GeoJSON解析失败: %v", err),
		})
		return
	}

	DB := models.DB

	// 3. 查询报告配置
	var report models.Report
	if err := DB.First(&report, reportID).Error; err != nil {
		c.JSON(http.StatusNotFound, GenerateReportResponse{
			Code:    404,
			Message: "报告配置不存在",
		})
		return
	}

	// 4. 解析图层配置
	var layers []string
	if err := json.Unmarshal(report.Layers, &layers); err != nil {
		c.JSON(http.StatusInternalServerError, GenerateReportResponse{
			Code:    500,
			Message: fmt.Sprintf("解析图层配置失败: %v", err),
		})
		return
	}

	// 5. 解析内容配置
	var contentItems []models.ContentItem
	if err := json.Unmarshal(report.Content, &contentItems); err != nil {
		c.JSON(http.StatusInternalServerError, GenerateReportResponse{
			Code:    500,
			Message: fmt.Sprintf("解析内容配置失败: %v", err),
		})
		return
	}

	// 6. 遍历图层，接收图片文件并构建imgMap
	var imgMap []services.ImgMap
	for _, layerName := range layers {
		// 使用图层名称作为表单字段名接收文件
		file, err := c.FormFile(layerName)
		if err != nil {
			// 如果某个图层没有对应的图片，记录警告但继续处理
			fmt.Printf("警告: 图层 %s 没有对应的图片文件: %v\n", layerName, err)
			continue
		}

		// 打开文件
		src, err := file.Open()
		if err != nil {
			c.JSON(http.StatusInternalServerError, GenerateReportResponse{
				Code:    500,
				Message: fmt.Sprintf("打开图层 %s 的图片文件失败: %v", layerName, err),
			})
			return
		}

		// 读取文件内容到字节数组
		imgBytes, err := io.ReadAll(src)
		src.Close()
		if err != nil {
			c.JSON(http.StatusInternalServerError, GenerateReportResponse{
				Code:    500,
				Message: fmt.Sprintf("读取图层 %s 的图片数据失败: %v", layerName, err),
			})
			return
		}

		// 添加到imgMap
		imgMap = append(imgMap, services.ImgMap{
			IMG:       imgBytes,
			LayerName: layerName,
		})
	}

	// 7. 检查模板文件是否存在
	templateData, err := templateFS.ReadFile("fonts/template.docx")
	if err != nil {
		c.JSON(http.StatusInternalServerError, GenerateReportResponse{
			Code:    500,
			Message: fmt.Sprintf("读取模板文件失败: %v", err),
		})
		return
	}

	// 8. 创建文档构建器
	builder, err := services.NewDocumentBuilderFromBytes(templateData)
	if err != nil {
		c.JSON(http.StatusInternalServerError, GenerateReportResponse{
			Code:    500,
			Message: fmt.Sprintf("创建文档构建器失败: %v", err),
		})
		return
	}
	defer builder.Close()

	// 9. 设置GeoJSON和图片映射
	builder.Geo = &geoJSON
	builder.IMGMap = imgMap

	// 10. 添加内容
	if err := builder.AddContentItems(contentItems); err != nil {
		c.JSON(http.StatusInternalServerError, GenerateReportResponse{
			Code:    500,
			Message: fmt.Sprintf("生成报告内容失败: %v", err),
		})
		return
	}

	// 11. 确保输出目录存在

	// 12. 生成文件名（添加时间戳避免冲突）

	timestamp := time.Now().Format("200601021504")
	FileName := fmt.Sprintf("%s_%s.docx", report.ReportName, timestamp)
	filePath := config.Download + fmt.Sprintf("/%s_%s.docx", report.ReportName, timestamp)

	// 13. 保存文档
	if err := builder.Save(filePath); err != nil {
		c.JSON(http.StatusInternalServerError, GenerateReportResponse{
			Code:    500,
			Message: fmt.Sprintf("保存文档失败: %v", err),
		})
		return
	}
	host := c.Request.Host
	taskid := uuid.New().String()
	homeDir, _ := os.UserHomeDir()
	OutFilePath := filepath.Join(homeDir, "BoundlessMap", "OutFile")
	path := filepath.Join(OutFilePath, taskid)
	os.MkdirAll(path, os.ModePerm)
	builder.Save(path + fmt.Sprintf("/%s_%s.docx", report.ReportName, timestamp))
	url := &url.URL{
		Scheme: "http",
		Host:   host,
		Path:   "/geo/OutFile/" + taskid + fmt.Sprintf("/%s_%s.docx", report.ReportName, timestamp),
	}

	// 14. 返回成功响应
	var resp GenerateReportResponse
	resp.Code = 200
	resp.Message = "报告生成成功"
	resp.Data.FilePath = filePath
	resp.Data.DownloadURL = url.String()
	resp.Data.FileName = FileName

	c.JSON(http.StatusOK, resp)
}

// 获取报告配置
func (uc *UserController) GetReportConfig(c *gin.Context) {
	reportID, err := strconv.ParseInt(c.Query("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "无效的报告ID",
		})
		return
	}

	DB := models.DB

	var report models.Report
	if err := DB.First(&report, reportID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"code":    404,
			"message": "报告配置不存在",
		})
		return
	}

	// 解析JSON数据
	var layers []string
	var content []models.ContentItem

	json.Unmarshal(report.Layers, &layers)
	json.Unmarshal(report.Content, &content)

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "获取成功",
		"data": gin.H{
			"id":          report.ID,
			"report_name": report.ReportName,
			"layers":      layers,
			"content":     content,
		},
	})
}

// 查询报告类型
func (uc *UserController) ListReportConfigs(c *gin.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 10
	}

	DB := models.DB

	var reports []models.Report
	var total int64

	offset := (page - 1) * pageSize

	DB.Model(&models.Report{}).Count(&total)
	DB.Offset(offset).Limit(pageSize).Order("id DESC").Find(&reports)

	// 解析每个报告的layers和content
	type ReportListItem struct {
		ID         int64                `json:"id"`
		ReportName string               `json:"report_name"`
		Layers     []string             `json:"layers"`
		Content    []models.ContentItem `json:"content"`
	}

	var list []ReportListItem
	for _, report := range reports {
		var layers []string
		var content []models.ContentItem

		json.Unmarshal(report.Layers, &layers)
		json.Unmarshal(report.Content, &content)

		list = append(list, ReportListItem{
			ID:         report.ID,
			ReportName: report.ReportName,
			Layers:     layers,
			Content:    content,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "获取成功",
		"data": gin.H{
			"total":     total,
			"page":      page,
			"page_size": pageSize,
			"list":      list,
		},
	})
}

// 删除报告配置
func (uc *UserController) DeleteReportConfig(c *gin.Context) {
	reportID, err := strconv.ParseInt(c.Query("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "无效的报告ID",
		})
		return
	}

	DB := models.DB

	result := DB.Delete(&models.Report{}, reportID)
	if result.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": fmt.Sprintf("删除失败: %v", result.Error),
		})
		return
	}

	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{
			"code":    404,
			"message": "报告配置不存在",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "删除成功",
	})
}

// 同步报告配置到目标数据库
func SyncLayerReportToDB(id string, sourceDB *gorm.DB, targetDB *gorm.DB) bool {
	// 1. 从源数据库查询 LayerHeader
	var ReportHeader models.Report
	targetDB.NamingStrategy = schema.NamingStrategy{
		SingularTable: true,
	}
	if err := sourceDB.Where("id = ?", id).First(&ReportHeader).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			log.Printf("未找到 MXDUid=%s 的 Report 记录", id)
			return false
		}
		log.Printf("查询 LayerHeader 失败: %v", err)
		return false
	}

	// 2. 从源数据库查询所有相关的 LayerMXD
	var layerMXDs models.Report
	if err := sourceDB.Where("id = ?", id).Find(&layerMXDs).Error; err != nil {
		log.Printf("查询 Report 失败: %v", err)
		return false
	}

	// 3. 开启事务进行同步
	tx := targetDB.Begin()
	if tx.Error != nil {
		log.Printf("开启事务失败: %v", tx.Error)
		return false
	}

	// 4. 删除目标数据库中已存在的 LayerHeader
	if err := tx.Where("id = ?", id).Delete(&models.Report{}).Error; err != nil {
		tx.Rollback()
		log.Printf("删除目标数据库 Report 失败: %v", err)
		return false
	}

	// 5. 删除目标数据库中已存在的 LayerMXD
	if err := tx.Where("id = ?", id).Delete(&models.Report{}).Error; err != nil {
		tx.Rollback()
		log.Printf("删除目标数据库 LayerMXD 失败: %v", err)
		return false
	}

	// 6. 插入新的 LayerHeader
	newHeader := models.Report{
		ReportName: ReportHeader.ReportName,
		Layers:     ReportHeader.Layers,
		Content:    ReportHeader.Content,
	}
	if err := tx.Create(&newHeader).Error; err != nil {
		tx.Rollback()
		log.Printf("创建 LayerHeader 失败: %v", err)
		return false
	}

	// 8. 提交事务
	if err := tx.Commit().Error; err != nil {
		log.Printf("提交事务失败: %v", err)
		return false
	}

	return true
}

// 同步工程数据到其他数据库
func (uc *UserController) SyncReport(c *gin.Context) {
	IP := c.Query("IP")
	ID := c.Query("ID")
	DB := models.DB

	// 连接目标数据库
	deviceDB, err := ConnectToDeviceDB(IP)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "连接目标数据库失败: " + err.Error(),
		})
		return
	}

	// 执行同步
	success := SyncLayerReportToDB(ID, DB, deviceDB)

	if success {
		c.JSON(http.StatusOK, gin.H{
			"code":    200,
			"message": "同步成功",
		})
	} else {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "同步失败",
		})
	}
}
