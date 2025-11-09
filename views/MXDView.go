package views

import (
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"github.com/gin-gonic/gin"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"net/http"
	"strconv"
	"sync"
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
			colorSetMap := make(models.JSONB)
			colorSetMap["LayerName"] = layerStyle.ColorSet.LayerName
			colorSetMap["AttName"] = layerStyle.ColorSet.AttName

			colorMapList := make([]map[string]interface{}, 0, len(layerStyle.ColorSet.ColorMap))
			for _, cm := range layerStyle.ColorSet.ColorMap {
				colorMapList = append(colorMapList, map[string]interface{}{
					"Property": cm.Property,
					"Color":    cm.Color,
				})
			}
			colorSetMap["ColorMap"] = colorMapList
			layerMXD.ColorSet = colorSetMap
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

//获取工程头文件列表
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
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	offset := (page - 1) * pageSize
	DB := models.DB
	// 构建查询
	query := DB.Model(&models.LayerMXD{})

	if mxdUid != "" {
		query = query.Where("mxd_uid = ?", mxdUid)
	}

	// 获取总数
	var total int64
	if err := query.Count(&total).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "查询失败: " + err.Error(),
		})
		return
	}

	// 获取列表
	var layerMXDs []models.LayerMXD
	if err := query.Order("layer_sort_id ASC, id ASC").
		Offset(offset).
		Limit(pageSize).
		Find(&layerMXDs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "查询失败: " + err.Error(),
		})
		return
	}

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
		"data": gin.H{
			"list":     layerMXDs,
			"total":    total,
			"page":     page,
			"pageSize": pageSize,
			"headers":  headers,
		},
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
	var req UpdateData
	DB := models.DB
	// 绑定请求参数
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "参数错误: " + err.Error(),
		})
		return
	}

	// 验证参数
	if req.IP == "" || req.TableName == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "IP和TableName不能为空",
		})
		return
	}

	// 连接目标数据库
	deviceDB, err := ConnectToDeviceDB(req.IP)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "连接目标数据库失败: " + err.Error(),
		})
		return
	}

	// 执行同步
	success := SyncLayerMXDToDB(req.TableName, DB, deviceDB)

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

// 同步LayerMXD数据到目标数据库（存在则更新）
func SyncLayerMXDToDB(tableName string, sourceDB *gorm.DB, targetDB *gorm.DB) bool {
	// 查询源数据库的所有记录
	var records []models.LayerMXD
	if err := sourceDB.Table(tableName).Find(&records).Error; err != nil {
		fmt.Printf("查询源数据失败: %v\n", err)
		return false
	}

	if len(records) == 0 {
		fmt.Println("没有需要同步的数据")
		return true
	}

	const batchSize = 2000
	const maxConcurrency = 8
	totalRecords := len(records)
	numBatches := (totalRecords + batchSize - 1) / batchSize

	var wg sync.WaitGroup
	errChan := make(chan error, numBatches)
	concurrencyLimiter := make(chan struct{}, maxConcurrency)
	successCount := int64(0)
	updateCount := int64(0)
	insertCount := int64(0)
	var countMutex sync.Mutex

	for i := 0; i < numBatches; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > totalRecords {
			end = totalRecords
		}

		wg.Add(1)
		concurrencyLimiter <- struct{}{}

		go func(batch []models.LayerMXD, batchIndex int) {
			defer wg.Done()
			defer func() { <-concurrencyLimiter }()

			tx := targetDB.Begin()
			defer func() {
				if r := recover(); r != nil {
					tx.Rollback()
					errChan <- fmt.Errorf("批次 %d 发生panic: %v", batchIndex, r)
				}
			}()

			batchSuccess := 0
			batchUpdate := 0
			batchInsert := 0

			for _, record := range batch {
				// 检查目标数据库中是否存在相同 MXDUid 的记录
				var existingRecord models.LayerMXD
				err := tx.Table(tableName).Where("mxd_uid = ? AND id = ?", record.MXDUid, record.ID).First(&existingRecord).Error

				if err == gorm.ErrRecordNotFound {
					// 不存在，执行插入
					if err := tx.Table(tableName).Create(&record).Error; err != nil {
						tx.Rollback()
						errChan <- fmt.Errorf("批次 %d 插入数据失败 (ID: %d, MXDUid: %s): %v",
							batchIndex, record.ID, record.MXDUid, err)
						return
					}
					batchInsert++
				} else if err != nil {
					// 查询出错
					tx.Rollback()
					errChan <- fmt.Errorf("批次 %d 查询数据失败 (MXDUid: %s): %v",
						batchIndex, record.MXDUid, err)
					return
				} else {
					// 存在，执行更新
					updateData := map[string]interface{}{
						"EN":          record.EN,
						"Main":        record.Main,
						"CN":          record.CN,
						"MXDName":     record.MXDName,
						"LineWidth":   record.LineWidth,
						"LayerSortID": record.LayerSortID,
						"Opacity":     record.Opacity,
						"FillType":    record.FillType,
						"LineColor":   record.LineColor,
						"ColorSet":    record.ColorSet,
					}

					if err := tx.Table(tableName).Where("id = ?", existingRecord.ID).Updates(updateData).Error; err != nil {
						tx.Rollback()
						errChan <- fmt.Errorf("批次 %d 更新数据失败 (ID: %d, MXDUid: %s): %v",
							batchIndex, existingRecord.ID, record.MXDUid, err)
						return
					}
					batchUpdate++
				}
				batchSuccess++
			}

			if err := tx.Commit().Error; err != nil {
				errChan <- fmt.Errorf("批次 %d 提交事务失败: %v", batchIndex, err)
				return
			}

			// 更新计数
			countMutex.Lock()
			successCount += int64(batchSuccess)
			updateCount += int64(batchUpdate)
			insertCount += int64(batchInsert)
			countMutex.Unlock()

			fmt.Printf("批次 %d 完成: 成功 %d 条 (插入 %d, 更新 %d)\n",
				batchIndex, batchSuccess, batchInsert, batchUpdate)

		}(records[start:end], i)
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	hasError := false
	for err := range errChan {
		if err != nil {
			fmt.Println(err.Error())
			hasError = true
		}
	}

	if hasError {
		return false
	}

	fmt.Printf("同步完成: 总计 %d 条记录, 插入 %d 条, 更新 %d 条\n",
		successCount, insertCount, updateCount)
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
