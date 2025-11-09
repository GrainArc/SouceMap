package views

import "C"
import (
	"encoding/json"
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"github.com/gin-gonic/gin"
	"gorm.io/datatypes"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"log"
	"net/http"
	"strconv"
)

// 请求体结构
type AddLayerMXDRequest struct {
	MXDName     string `json:"MXDName" binding:"required"`
	MXDUid      string `json:"MXDUid" binding:"required"`
	LayerStyles []LayerStyle
}

type LayerStyle struct {
	EN          string     `json:"EN"`
	Main        string     `json:"Main"`
	CN          string     `json:"CN"`
	LineWidth   string     `json:"LineWidth"`
	LayerSortID int64      `json:"LayerSortID"`
	Opacity     string     `json:"Opacity"`
	FillType    string     `json:"FillType"`
	LineColor   string     `json:"LineColor"`
	ColorSet    *ColorData `json:"ColorSet"`
}

// 新增图层工程
func (uc *UserController) AddUpdateLayerMXD(c *gin.Context) {
	var req AddLayerMXDRequest

	// 绑定请求参数
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "参数错误: " + err.Error(),
		})
		return
	}

	DB := models.DB
	tx := DB.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	// 检查 MXDUid 是否已存在
	var existingCount int64
	if err := tx.Model(&models.LayerMXD{}).Where("mxd_uid = ?", req.MXDUid).Count(&existingCount).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "查询数据失败: " + err.Error(),
		})
		return
	}

	// 如果存在，删除旧数据
	if existingCount > 0 {
		// 删除 LayerMXD 表中的记录
		if err := tx.Where("mxd_uid = ?", req.MXDUid).Delete(&models.LayerMXD{}).Error; err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{
				"code":    500,
				"message": "删除旧图层数据失败: " + err.Error(),
			})
			return
		}

		// 删除 LayerHeader 表中的记录
		if err := tx.Where("mxd_uid = ?", req.MXDUid).Delete(&models.LayerHeader{}).Error; err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{
				"code":    500,
				"message": "删除旧工程头数据失败: " + err.Error(),
			})
			return
		}
	}

	// 批量创建 LayerMXD 记录
	layerMXDs := make([]models.LayerMXD, 0, len(req.LayerStyles))
	for _, layerStyle := range req.LayerStyles {
		layerMXD := models.LayerMXD{
			EN:          layerStyle.EN,
			Main:        layerStyle.Main,
			CN:          layerStyle.CN,
			MXDName:     req.MXDName,
			MXDUid:      req.MXDUid,
			LineWidth:   layerStyle.LineWidth,
			LayerSortID: layerStyle.LayerSortID,
			Opacity:     layerStyle.Opacity,
			FillType:    layerStyle.FillType,
			LineColor:   layerStyle.LineColor,
		}

		// 处理 ColorSet
		if layerStyle.ColorSet != nil {
			jsonData, err := json.Marshal(layerStyle.ColorSet)
			if err != nil {
				// 处理错误
				return
			}
			layerMXD.ColorSet = datatypes.JSON(jsonData)
		}

		layerMXDs = append(layerMXDs, layerMXD)
	}

	// 批量插入 LayerMXD
	if len(layerMXDs) > 0 {
		if err := tx.Create(&layerMXDs).Error; err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{
				"code":    500,
				"message": "创建图层数据失败: " + err.Error(),
			})
			return
		}
	}

	// 创建 LayerHeader
	layerHeader := models.LayerHeader{
		MXDName: req.MXDName,
		MXDUid:  req.MXDUid,
	}
	if err := tx.Create(&layerHeader).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "创建工程头失败: " + err.Error(),
		})
		return
	}

	// 提交事务
	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "提交事务失败: " + err.Error(),
		})
		return
	}

	message := "创建成功"
	if existingCount > 0 {
		message = "更新成功"
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": message,
	})
}

// 获取工程头文件列表
func (uc *UserController) GetLayerMXDHeaderList(c *gin.Context) {
	DB := models.DB
	// 获取工程列表（去重）
	var headers []models.LayerHeader
	if err := DB.Find(&headers).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "查询工程列表失败: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "查询成功",
		"data":    headers,
	})
}

// 请求当前的工程列表
func (uc *UserController) GetLayerMXDList(c *gin.Context) {
	mxdUid := c.Query("MXDUid")
	DB := models.DB
	// 构建查询
	query := DB.Model(&models.LayerMXD{})

	if mxdUid != "" {
		query = query.Where("mxd_uid = ?", mxdUid)
	}

	// 获取列表
	var layerMXDs []models.LayerMXD
	if err := query.Order("layer_sort_id ASC, id ASC").
		Find(&layerMXDs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "查询失败: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "查询成功",
		"data":    layerMXDs,
	})
}

// 删除工程
func (uc *UserController) DelLayerMXD(c *gin.Context) {
	idStr := c.Param("ID")
	mxdUid := c.Query("MXDUid") // 如果要删除整个工程的所有图层
	DB := models.DB
	tx := DB.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	// 根据 ID 删除单个图层
	if idStr != "" {
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"code":    400,
				"message": "ID格式错误",
			})
			return
		}

		var layerMXD models.LayerMXD
		if err := tx.First(&layerMXD, id).Error; err != nil {
			tx.Rollback()
			if err == gorm.ErrRecordNotFound {
				c.JSON(http.StatusNotFound, gin.H{
					"code":    404,
					"message": "记录不存在",
				})
			} else {
				c.JSON(http.StatusInternalServerError, gin.H{
					"code":    500,
					"message": "查询失败: " + err.Error(),
				})
			}
			return
		}

		deletedMxdUid := layerMXD.MXDUid

		// 删除图层
		if err := tx.Delete(&layerMXD).Error; err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{
				"code":    500,
				"message": "删除失败: " + err.Error(),
			})
			return
		}

		// 检查该工程下是否还有其他图层
		var count int64
		tx.Model(&models.LayerMXD{}).Where("mxd_uid = ?", deletedMxdUid).Count(&count)

		// 如果没有图层了，删除 LayerHeader
		if count == 0 {
			tx.Where("mxd_uid = ?", deletedMxdUid).Delete(&models.LayerHeader{})
		}

	} else if mxdUid != "" {
		// 删除整个工程的所有图层
		if err := tx.Where("mxd_uid = ?", mxdUid).Delete(&models.LayerMXD{}).Error; err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{
				"code":    500,
				"message": "删除图层失败: " + err.Error(),
			})
			return
		}

		// 删除工程头
		if err := tx.Where("mxd_uid = ?", mxdUid).Delete(&models.LayerHeader{}).Error; err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{
				"code":    500,
				"message": "删除工程头失败: " + err.Error(),
			})
			return
		}
	} else {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "请提供ID或mxd_uid参数",
		})
		return
	}

	// 提交事务
	if err := tx.Commit().Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "提交事务失败: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "删除成功",
	})
}

// 同步工程数据到其他数据库
func (uc *UserController) SyncLayerMXD(c *gin.Context) {
	IP := c.Query("IP")
	MXDUid := c.Query("MXDUid")
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
	success := SyncLayerMXDToDB(MXDUid, DB, deviceDB)

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

// 同步工程数据到目标数据库（存在则删除后重新插入）
func SyncLayerMXDToDB(MXDUid string, sourceDB *gorm.DB, targetDB *gorm.DB) bool {
	// 1. 从源数据库查询 LayerHeader
	var layerHeader models.LayerHeader
	targetDB.NamingStrategy = schema.NamingStrategy{
		SingularTable: true,
	}
	if err := sourceDB.Where("mxd_uid = ?", MXDUid).First(&layerHeader).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			log.Printf("未找到 MXDUid=%s 的 LayerHeader 记录", MXDUid)
			return false
		}
		log.Printf("查询 LayerHeader 失败: %v", err)
		return false
	}

	// 2. 从源数据库查询所有相关的 LayerMXD
	var layerMXDs []models.LayerMXD
	if err := sourceDB.Where("mxd_uid = ?", MXDUid).Find(&layerMXDs).Error; err != nil {
		log.Printf("查询 LayerMXD 失败: %v", err)
		return false
	}

	// 3. 开启事务进行同步
	tx := targetDB.Begin()
	if tx.Error != nil {
		log.Printf("开启事务失败: %v", tx.Error)
		return false
	}

	// 4. 删除目标数据库中已存在的 LayerHeader
	if err := tx.Where("mxd_uid = ?", MXDUid).Delete(&models.LayerHeader{}).Error; err != nil {
		tx.Rollback()
		log.Printf("删除目标数据库 LayerHeader 失败: %v", err)
		return false
	}

	// 5. 删除目标数据库中已存在的 LayerMXD
	if err := tx.Where("mxd_uid = ?", MXDUid).Delete(&models.LayerMXD{}).Error; err != nil {
		tx.Rollback()
		log.Printf("删除目标数据库 LayerMXD 失败: %v", err)
		return false
	}

	// 6. 插入新的 LayerHeader
	newHeader := models.LayerHeader{
		MXDName: layerHeader.MXDName,
		MXDUid:  layerHeader.MXDUid,
	}
	if err := tx.Create(&newHeader).Error; err != nil {
		tx.Rollback()
		log.Printf("创建 LayerHeader 失败: %v", err)
		return false
	}

	// 7. 批量插入新的 LayerMXD 数据
	if len(layerMXDs) > 0 {
		// 准备新数据（不包含ID，让数据库自动生成）
		newMXDs := make([]models.LayerMXD, len(layerMXDs))
		for i, mxd := range layerMXDs {
			newMXDs[i] = models.LayerMXD{
				EN:          mxd.EN,
				Main:        mxd.Main,
				CN:          mxd.CN,
				MXDName:     mxd.MXDName,
				MXDUid:      mxd.MXDUid,
				LineWidth:   mxd.LineWidth,
				LayerSortID: mxd.LayerSortID,
				Opacity:     mxd.Opacity,
				FillType:    mxd.FillType,
				LineColor:   mxd.LineColor,
				ColorSet:    mxd.ColorSet,
			}
		}

		// 批量插入
		if err := tx.Create(&newMXDs).Error; err != nil {
			tx.Rollback()
			log.Printf("批量创建 LayerMXD 失败: %v", err)
			return false
		}
	}

	// 8. 提交事务
	if err := tx.Commit().Error; err != nil {
		log.Printf("提交事务失败: %v", err)
		return false
	}

	log.Printf("成功同步 MXDUid=%s 的数据，共 %d 条 LayerMXD 记录", MXDUid, len(layerMXDs))
	return true
}

// 连接到目标数据库的辅助函数（需要根据实际情况实现）
func ConnectToDeviceDB(ip string) (*gorm.DB, error) {
	// 这里需要根据你的实际数据库配置来实现
	// 示例：
	dsn := fmt.Sprintf("host=%s user=postgres password=1 dbname=GL port=5432 sslmode=disable", ip)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	return db, nil
}
