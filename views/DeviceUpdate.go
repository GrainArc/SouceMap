package views

import (
	"encoding/json"
	"fmt"
	"github.com/fmecool/Gogeo"
	"github.com/fmecool/SouceMap/Transformer"
	"github.com/fmecool/SouceMap/config"
	"github.com/fmecool/SouceMap/methods"
	"github.com/fmecool/SouceMap/models"
	"github.com/fmecool/SouceMap/pgmvt"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// 获取当前能更新的设备
type DeviceData struct {
	IP         string
	DeviceName string
}

func (uc *UserController) GetAllDeviceName(c *gin.Context) {
	ips := methods.GetIP(config.UpdateIP)
	var data []DeviceData
	for _, ip := range ips {
		if ip != strings.Split(config.MainRouter, ":")[0] {
			url := fmt.Sprintf("http://%s:8181/geo/GetDeviceName", ip)
			resp, _ := http.Get(url)
			var aa DeviceData
			aa.IP = ip
			body, _ := io.ReadAll(resp.Body)
			aa.DeviceName = string(body)
			defer resp.Body.Close()
			if resp.StatusCode == 200 {
				data = append(data, aa)
			}
		}

	}
	c.JSON(http.StatusOK, data)
}

// 更新设备
type UpdateData struct {
	IP        string
	TableName string
}
type ColumnIfo struct {
	ColumnName             string `gorm:"column:column_name"`
	DataType               string `gorm:"column:data_type"`
	CharacterMaximumLength string `gorm:"column:character_maximum_length"`
}

func UpdateDB(TableName string, DB *gorm.DB, DeviceDB *gorm.DB) bool {

	var records []map[string]interface{}
	DB.Table(TableName).Find(&records)

	const batchSize = 2000
	const maxConcurrency = 8 // 限制并发数量为 4
	totalRecords := len(records)
	numBatches := (totalRecords + batchSize - 1) / batchSize // 计算批次数

	var wg sync.WaitGroup
	errChan := make(chan error, numBatches)
	concurrencyLimiter := make(chan struct{}, maxConcurrency) // 用于限制并发数

	for i := 0; i < numBatches; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > totalRecords {
			end = totalRecords
		}

		wg.Add(1)
		concurrencyLimiter <- struct{}{} // 获取一个令牌，限制并发数
		go func(batch []map[string]interface{}) {
			defer wg.Done()
			defer func() { <-concurrencyLimiter }() // 释放令牌

			tx := DeviceDB.Begin()
			defer func() {
				if r := recover(); r != nil {
					tx.Rollback()
					errChan <- fmt.Errorf("通道错误: %v", r)
				}
			}()

			for _, item := range batch {
				err := tx.Table(TableName).Create(&item).Error
				if err != nil {
					tx.Rollback()
					errChan <- fmt.Errorf("数据插入错误: %v", err)
					return
				}
			}
			tx.Commit()
		}(records[start:end])
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	for err := range errChan {
		if err != nil {
			fmt.Println(err.Error())
			return false
		}
	}

	return true
}

func UpdateDeviceSingle(jsonData UpdateData) bool {
	ip := jsonData.IP
	TableName := jsonData.TableName
	//获取更新对象的所有表

	url := fmt.Sprintf("http://%s:8181/geo/GetSchema", ip)
	resp, _ := http.Get(url)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var Tablejson []map[string]interface{}
	json.Unmarshal(body, &Tablejson)
	isok := false
	for _, item := range Tablejson {
		if TableName == item["EN"].(string) {
			isok = true
		}
	}
	DB := models.DB
	DSN := fmt.Sprintf("host=%s user=postgres password=1 dbname=GL port=5432 sslmode=disable TimeZone=Asia/Shanghai", ip)
	DeviceDB, _ := gorm.Open(postgres.Open(DSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	// 确保数据库连接被正确释放
	defer func() {
		if sqlDB, err := DeviceDB.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	if isok == true { //先删除再更新
		aa := DeviceDB.Table(TableName).Where("id != ?", 0).Delete(nil).Error
		if aa != nil {
			fmt.Println(aa.Error())
		}
		//删除瓦片缓存信息
		if isEndWithNumber(TableName) {
			DeviceDB.Table(TableName+"_mvt").Where("id != ?", 0).Delete(nil)
		} else {
			DeviceDB.Table(TableName+"mvt").Where("id != ?", 0).Delete(nil)
		}
		// 数据迁移
		pd := UpdateDB(TableName, DB, DeviceDB)
		if pd == false {
			return false
		}

	} else {
		//创建表
		var Schemas map[string]interface{}
		DB.Table("my_schema").Where("EN = ?", TableName).Find(&Schemas)

		sql := fmt.Sprintf(`SELECT MAX(id) AS max_id FROM my_schema;`)
		var maxid int
		DeviceDB.Raw(sql).Scan(&maxid)
		Schemas["id"] = maxid + 1
		DeviceDB.Table("my_schema").Create(&Schemas)
		//创建几何表
		var Geo string
		switch Schemas["type"].(string) {
		case "line":
			Geo = "LINESTRING"
		case "polygon":
			Geo = "MULTIPOLYGON"
		case "point":
			Geo = "POINT"
		}
		sesql := fmt.Sprintf(`SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_name = '%s'`, TableName)
		var aa []ColumnIfo
		DB.Raw(sesql).Scan(&aa)

		var ff []string
		for _, item := range aa {
			fieldname := item.ColumnName
			if fieldname != "id" && fieldname != "geom" {
				var dtype string
				var bb string
				switch item.DataType {
				case "character varying":
					dtype = "VARCHAR"
					bb = fmt.Sprintf(`%s %s(%s)`, fieldname, dtype, item.CharacterMaximumLength)
				case "integer":
					dtype = "INT"
					bb = fmt.Sprintf(`%s %s`, fieldname, dtype)
				case "boolean":
					dtype = "BOOLEAN"
					bb = fmt.Sprintf(`%s %s`, fieldname, dtype)
				case "real":
					dtype = "FLOAT"
					bb = fmt.Sprintf(`%s %s`, fieldname, dtype)
				case "double precision":
					dtype = "FLOAT"
					bb = fmt.Sprintf(`%s %s`, fieldname, dtype)
				case "time without time zone":
					dtype = "TIME"
					bb = fmt.Sprintf(`%s %s`, fieldname, dtype)
				case "bigint":
					dtype = "BIGINT"
					bb = fmt.Sprintf(`%s %s`, fieldname, dtype)
				case "numeric":
					dtype = "NUMERIC"
					bb = fmt.Sprintf(`%s %s`, fieldname, dtype)
				case "text":
					dtype = "TEXT"
					bb = fmt.Sprintf(`%s %s`, fieldname, dtype)
				case "bytea":
					dtype = "bytea"
					bb = fmt.Sprintf(`%s %s`, fieldname, dtype)
				case "date":
					dtype = "DATE"
					bb = fmt.Sprintf(`%s %s`, fieldname, dtype)
				default: // 处理其他未知数据类型
					dtype = item.DataType
					bb = fmt.Sprintf(`%s %s`, fieldname, dtype)
				}

				if bb != "" {
					ff = append(ff, bb)
				}

			}
		}
		ff = append(ff, "id"+" SERIAL PRIMARY KEY")
		ff = removeDuplicates(ff)
		result := strings.Join(ff, ",")
		//保险起见删除表
		sqldel := fmt.Sprintf("DROP  TABLE  IF  EXISTS  %s", TableName)
		if err := DeviceDB.Exec(sqldel).Error; err != nil {
			fmt.Println(err.Error())
		}
		query1 := fmt.Sprintf("CREATE TABLE  IF  NOT  EXISTS %s (%s,geom GEOMETRY(%s,  4326))", TableName, result, Geo)
		err := DeviceDB.Exec(query1).Error
		if err != nil {
			fmt.Println(err.Error())
		}
		//新建对应的mvt表
		var query2 string
		if isEndWithNumber(TableName) {
			query2 = fmt.Sprintf("CREATE TABLE  IF  NOT  EXISTS %s (ID  SERIAL  PRIMARY  KEY,X INT8,Y INT8,Z INT8,Byte  BYTEA)", TableName+"_mvt")
		} else {
			query2 = fmt.Sprintf("CREATE TABLE  IF  NOT  EXISTS %s (ID  SERIAL  PRIMARY  KEY,X INT8,Y INT8,Z INT8,Byte  BYTEA)", TableName+"mvt")
		}
		DeviceDB.Exec(query2)
		// 数据迁移
		pd := UpdateDB(TableName, DB, DeviceDB)
		if pd == false {
			return false
		}

	}
	return true
}
func UpdateConfigSingle(jsonData UpdateData) bool {
	ip := jsonData.IP
	TableName := jsonData.TableName

	DB := models.DB
	DSN := fmt.Sprintf("host=%s user=postgres password=1 dbname=GL port=5432 sslmode=disable TimeZone=Asia/Shanghai", ip)
	DeviceDB, _ := gorm.Open(postgres.Open(DSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	DeviceDB.NamingStrategy = schema.NamingStrategy{
		SingularTable: true,
	}
	// 确保数据库连接被正确释放
	defer func() {
		if sqlDB, err := DeviceDB.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	var SD []models.AttColor
	DB.Where("layer_name = ? ", TableName).Find(&SD)

	if len(SD) > 0 {
		var SD2 []models.AttColor
		DeviceDB.Where("layer_name = ? ", TableName).Find(&SD2)
		if len(SD2) > 0 {
			DeviceDB.Delete(&SD2)
		}
		DeviceDB.Create(&SD)
	}

	var AD []models.ChineseProperty
	DB.Where("layer_name = ? ", TableName).Find(&AD)
	if len(AD) > 0 {
		var AD2 []models.ChineseProperty
		DeviceDB.Where("layer_name = ? ", TableName).Find(&AD2)
		if len(AD2) > 0 {
			DeviceDB.Delete(&AD2)
		}
		DeviceDB.Create(&AD)
	}

	return true
}

func (uc *UserController) UpdateDevice(c *gin.Context) {
	var jsonData UpdateData
	c.BindJSON(&jsonData)
	bb := UpdateDeviceSingle(jsonData)
	UpdateConfigSingle(jsonData)
	if bb == true {
		c.JSON(http.StatusOK, "ok")
		return
	}
	c.JSON(http.StatusBadRequest, "err")

}

func removeDuplicates(input []string) []string {
	m := make(map[string]bool)
	var result []string

	for _, item := range input {
		if _, found := m[item]; !found {
			m[item] = true
			result = append(result, item)
		}
	}

	return result
}

// 矢量数据导出接口
func DownLayer(tablename string) string {
	SD := searchData{
		TableName: tablename,
	}

	DB := models.DB
	var Schema models.MySchema
	DB.Where("en = ?", tablename).First(&Schema)
	taskid := Schema.EN
	existingZipPath := "OutFile/" + taskid + "/" + tablename + ".zip"
	if _, err := os.Stat(existingZipPath); err == nil {
		// 文件存在，直接返回路径
		return existingZipPath
	}

	result, _ := queryTable(DB, SD)

	outdir := "OutFile/" + taskid
	os.MkdirAll(outdir, os.ModePerm)
	outshp := "OutFile/" + taskid + "/" + Schema.CN + ".shp"

	// 直接从查询结果转换为Shapefile

	Gogeo.ConvertPostGISToShapefileWithStructure(DB, result.Data, outshp, tablename)

	methods.ZipFolder(outdir, tablename)

	return "OutFile/" + taskid + "/" + tablename + ".zip"
}

func (uc *UserController) OutLayer(c *gin.Context) {
	table := c.Query("tablename")
	path := DownLayer(table)
	host := c.Request.Host
	url := &url.URL{
		Scheme: "http",
		Host:   host,
		Path:   "/geo/" + path,
	}
	c.String(http.StatusOK, url.String())

}

func shpToLayer(tablename string, shp string, addType string) bool {
	DB := models.DB
	var Schema models.MySchema
	DB.Where("en = ?", tablename).First(&Schema)
	pgmvt.UpdateSHPDirectly(DB, shp, Schema.EN, Schema.CN, Schema.Main, Schema.Color, Schema.Opacity, Schema.Userunits, addType)
	return true
}

func GDBToLayer(tablename string, gdbs []string, addType string) bool {
	DB := models.DB
	var Schema models.MySchema
	DB.Where("en = ?", tablename).First(&Schema)
	for _, gdb := range gdbs {
		pgmvt.UpdateGDBDirectly(DB, gdb, Schema.EN, Schema.CN, Schema.Main, Schema.Color, Schema.Opacity, Schema.Userunits, addType)
	}

	return true
}
func (uc *UserController) UpdateLayer(c *gin.Context) {
	tablename := c.PostForm("tablename")
	file, err := c.FormFile("file")
	if err != nil {
		c.String(400, "Bad request")
		return
	}
	taskid := uuid.New().String()
	path, _ := filepath.Abs("./TempFile/" + taskid + "/" + "/" + file.Filename)
	dirpath := filepath.Dir(path)
	err = c.SaveUploadedFile(file, path)
	if err != nil {
		c.String(500, "Internal server error")
		return
	}
	if filepath.Ext(path) == ".zip" || filepath.Ext(path) == ".rar" {
		methods.Unzip(path)
	}
	shpfiles := Transformer.FindFiles(dirpath, "shp")

	if len(shpfiles) != 0 {
		aa := shpToLayer(tablename, shpfiles[0], "覆盖")
		//更新数据库的更新时间
		DB := models.DB
		var layerSchema models.MySchema
		DB.Where("en = ?", tablename).First(&layerSchema)
		layerSchema.UpdatedDate = time.Now().Format("2006-01-02 15:04:05")
		//创建消息
		var MSG models.UpdateMessage
		MSG.LayerNameEN = layerSchema.EN
		MSG.LayerNameCN = layerSchema.CN
		MSG.Date = time.Now().Format("2006-01-02 15:04:05")
		MSG.UpdatedUser = c.PostForm("username")
		MSG.MSG = fmt.Sprintf(`%s 在 %s 时间，更新了数据: %s,请及时完成移动端的数据同步`, MSG.UpdatedUser, MSG.Date, MSG.LayerNameCN)
		DB.Create(&MSG)
		if aa == true {
			c.String(http.StatusOK, "ok")
		} else {
			c.String(http.StatusBadRequest, "矢量更新失败")
		}
	}
	gdbfiles := Transformer.FindFiles(dirpath, "gdb")
	if len(gdbfiles) != 0 {
		aa := GDBToLayer(tablename, gdbfiles, "覆盖")
		//更新数据库的更新时间
		DB := models.DB
		var layerSchema models.MySchema
		DB.Where("en = ?", tablename).First(&layerSchema)
		layerSchema.UpdatedDate = time.Now().Format("2006-01-02 15:04:05")
		//创建消息
		var MSG models.UpdateMessage
		MSG.LayerNameEN = layerSchema.EN
		MSG.LayerNameCN = layerSchema.CN
		MSG.Date = time.Now().Format("2006-01-02 15:04:05")
		MSG.UpdatedUser = c.PostForm("username")
		MSG.MSG = fmt.Sprintf(`%s 在 %s 时间，更新了数据: %s,请及时完成移动端的数据同步`, MSG.UpdatedUser, MSG.Date, MSG.LayerNameCN)
		DB.Create(&MSG)
		if aa == true {
			c.String(http.StatusOK, "ok")
		} else {
			c.String(http.StatusBadRequest, "矢量更新失败")
		}
	}

}

func (uc *UserController) AppendLayer(c *gin.Context) {
	tablename := c.PostForm("tablename")
	file, err := c.FormFile("file")
	if err != nil {
		c.String(400, "Bad request")
		return
	}
	taskid := uuid.New().String()
	path, _ := filepath.Abs("./TempFile/" + taskid + "/" + "/" + file.Filename)
	dirpath := filepath.Dir(path)
	err = c.SaveUploadedFile(file, path)
	if err != nil {
		c.String(500, "Internal server error")
		return
	}
	if filepath.Ext(path) == ".zip" || filepath.Ext(path) == ".rar" {
		methods.Unzip(path)
	}
	shpfiles := Transformer.FindFiles(dirpath, "shp")

	if len(shpfiles) != 0 {
		aa := shpToLayer(tablename, shpfiles[0], "追加")
		//更新数据库的更新时间
		DB := models.DB
		var layerSchema models.MySchema
		DB.Where("en = ?", tablename).First(&layerSchema)
		layerSchema.UpdatedDate = time.Now().Format("2006-01-02 15:04:05")
		//创建消息
		var MSG models.UpdateMessage
		MSG.LayerNameEN = layerSchema.EN
		MSG.LayerNameCN = layerSchema.CN
		MSG.Date = time.Now().Format("2006-01-02 15:04:05")
		MSG.UpdatedUser = c.PostForm("username")
		MSG.MSG = fmt.Sprintf(`%s 在 %s 时间，追加了数据: %s,请及时完成移动端的数据同步`, MSG.UpdatedUser, MSG.Date, MSG.LayerNameCN)
		DB.Create(&MSG)
		if aa == true {
			c.String(http.StatusOK, "ok")
		} else {
			c.String(http.StatusBadRequest, "矢量添加失败")
		}
	}
	gdbfiles := Transformer.FindFiles(dirpath, "gdb")
	if len(gdbfiles) != 0 {
		aa := GDBToLayer(tablename, gdbfiles, "覆盖")
		//更新数据库的更新时间
		DB := models.DB
		var layerSchema models.MySchema
		DB.Where("en = ?", tablename).First(&layerSchema)
		layerSchema.UpdatedDate = time.Now().Format("2006-01-02 15:04:05")
		//创建消息
		var MSG models.UpdateMessage
		MSG.LayerNameEN = layerSchema.EN
		MSG.LayerNameCN = layerSchema.CN
		MSG.Date = time.Now().Format("2006-01-02 15:04:05")
		MSG.UpdatedUser = c.PostForm("username")
		MSG.MSG = fmt.Sprintf(`%s 在 %s 时间，添加了数据: %s,请及时完成移动端的数据同步`, MSG.UpdatedUser, MSG.Date, MSG.LayerNameCN)
		DB.Create(&MSG)
		if aa == true {
			c.String(http.StatusOK, "ok")
		} else {
			c.String(http.StatusBadRequest, "矢量添加失败")
		}
	}

}

// 获取更新消息
func (uc *UserController) GetUpdateMSG(c *gin.Context) {
	DB := models.DB
	var MSG []models.UpdateMessage
	DB.Order("id desc").Limit(10).Find(&MSG)

	// 返回结果
	c.JSON(200, gin.H{
		"data": MSG,
	})
}
