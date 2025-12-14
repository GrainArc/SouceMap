package views

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"os/exec"
	"runtime"

	"fmt"
	"github.com/GrainArc/Gogeo"
	"github.com/GrainArc/SouceMap/Transformer"
	"github.com/GrainArc/SouceMap/config"
	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/models"
	"github.com/GrainArc/SouceMap/pgmvt"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// isValidInterface 判断接口名称是否为 WiFi 或以太网
// isValidInterface 判断接口名称是否为 WiFi 或以太网
func isValidInterface(name string) bool {
	name = strings.ToLower(name)

	switch runtime.GOOS {
	case "windows":
		validPrefixes := []string{
			"ethernet",
			"eth",
			"以太网",
			"wi-fi",
			"wlan",
			"local area connection",
		}
		for _, prefix := range validPrefixes {
			if strings.Contains(name, prefix) {
				return true
			}
		}

	case "linux":
		validPrefixes := []string{
			"eth", "ens", "enp", "eno",
			"wlan", "inet", "wlp", "wlo",
			"以太网",
		}
		for _, prefix := range validPrefixes {
			if strings.HasPrefix(name, prefix) {
				return true
			}
		}

	case "darwin": // macOS
		if strings.HasPrefix(name, "en") {
			return true
		}
	}

	return false
}

// getIPv4FromInterfaces 从网络接口获取 IPv4 地址前缀
func getIPv4FromInterfaces() ([]string, error) {
	var ipPrefixes []string
	seenPrefixes := make(map[string]bool)

	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("获取网络接口失败: %v", err)
	}

	for _, iface := range interfaces {
		// 跳过未启用和回环接口
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		if !isValidInterface(iface.Name) {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip != nil && ip.To4() != nil {
				ipStr := ip.String()
				parts := strings.Split(ipStr, ".")
				if len(parts) == 4 {
					prefix := strings.Join(parts[:3], ".")
					if !seenPrefixes[prefix] {
						seenPrefixes[prefix] = true
						ipPrefixes = append(ipPrefixes, prefix)
					}
				}
			}
		}
	}

	if len(ipPrefixes) > 0 {
		return ipPrefixes, nil
	}

	return nil, fmt.Errorf("未找到有效的 IPv4 地址")
}

// getIPv4FromIfconfig 通过 ifconfig 命令获取 IPv4 地址前缀（备选方案）
func getIPv4FromIfconfig() ([]string, error) {
	cmd := exec.Command("ifconfig")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("执行 ifconfig 失败: %v", err)
	}

	var ipPrefixes []string
	seenPrefixes := make(map[string]bool)

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		// 查找 inet 行，排除本地回环地址
		if strings.Contains(line, "inet ") && !strings.Contains(line, "127.0.0.1") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				ip := parts[1]
				ipParts := strings.Split(ip, ".")
				if len(ipParts) == 4 {
					prefix := strings.Join(ipParts[:3], ".")
					if !seenPrefixes[prefix] {
						seenPrefixes[prefix] = true
						ipPrefixes = append(ipPrefixes, prefix)
					}
				}
			}
		}
	}

	if len(ipPrefixes) > 0 {
		return ipPrefixes, nil
	}

	return nil, fmt.Errorf("未找到有效的 IPv4 地址")
}

// GetAllLocalIPv4 获取所有符合条件的 IPv4 地址，支持备选方案
func GetAllLocalIPv4() ([]string, error) {
	// 首先尝试使用标准库方法
	ipPrefixes, _ := getIPv4FromInterfaces()
	if len(ipPrefixes) != 0 {
		return ipPrefixes, nil
	}

	ipPrefixes, err := getIPv4FromIfconfig()
	if err == nil {
		return ipPrefixes, nil
	}

	// 如果都失败，尝试 Windows 的 ipconfig 命令
	if runtime.GOOS == "windows" {
		ipPrefixes, err := getIPv4FromIpconfig()
		if err == nil {
			return ipPrefixes, nil
		}
	}

	return nil, fmt.Errorf("无法获取有效的 IPv4 地址，已尝试所有可用方法")
}

// getIPv4FromIpconfig Windows 专用方法
func getIPv4FromIpconfig() ([]string, error) {
	cmd := exec.Command("ipconfig")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("执行 ipconfig 失败: %v", err)
	}

	var ipPrefixes []string
	seenPrefixes := make(map[string]bool)

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		// 查找 IPv4 地址行
		if strings.Contains(line, "IPv4 Address") || strings.Contains(line, "IPv4 地址") {
			parts := strings.Split(line, ":")
			if len(parts) >= 2 {
				ip := strings.TrimSpace(parts[1])
				ipParts := strings.Split(ip, ".")
				if len(ipParts) == 4 {
					prefix := strings.Join(ipParts[:3], ".")
					if !seenPrefixes[prefix] {
						seenPrefixes[prefix] = true
						ipPrefixes = append(ipPrefixes, prefix)
					}
				}
			}
		}
	}

	if len(ipPrefixes) > 0 {
		return ipPrefixes, nil
	}

	return nil, fmt.Errorf("未找到有效的 IPv4 地址")
}

// 获取当前能更新的设备
type DeviceData struct {
	IP         string
	DeviceName string
}

func (uc *UserController) GetAllDeviceName(c *gin.Context) {
	// 获取所有IP地址列表
	Mainips, _ := GetAllLocalIPv4()
	var ips []string
	for _, Mainip := range Mainips {
		ipsa := methods.GetIP(Mainip)
		ips = append(ips, ipsa...)
	}

	// 输入验证：检查IP列表是否为空
	if len(ips) == 0 {
		c.JSON(http.StatusOK, []DeviceData{}) // 返回空数组而不是nil
		return
	}

	// 创建带超时的上下文，防止请求无限期阻塞
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel() // 确保资源被释放

	// 创建通道和等待组用于并发控制和数据收集
	dataChan := make(chan DeviceData, len(ips)) // 缓冲通道，容量等于IP数量
	var wg sync.WaitGroup                       // 等待组，用于等待所有goroutine完成

	// 限制并发数量，避免过多的goroutine占用系统资源
	const maxConcurrency = 10                        // 定义最大并发数为常量
	semaphore := make(chan struct{}, maxConcurrency) // 信号量通道，控制并发数

	// 获取主路由器IP，用于过滤自身IP
	mainRouterIP := strings.Split(config.MainRouter, ":")[0]

	// 遍历所有IP地址，为每个非主路由器IP启动goroutine
	for _, ip := range ips {
		// 跳过主路由器IP，避免自己请求自己
		if ip == mainRouterIP {
			continue // 跳过当前循环，处理下一个IP
		}

		wg.Add(1) // 增加等待组计数
		go func(ip string) { // 启动goroutine处理单个IP
			defer wg.Done() // goroutine结束时减少等待组计数

			// 获取信号量，控制并发数量
			select {
			case semaphore <- struct{}{}: // 尝试获取信号量
				defer func() { <-semaphore }() // 确保释放信号量
			case <-ctx.Done(): // 检查上下文是否已取消
				return // 如果上下文取消，直接返回
			}

			// 调用辅助函数获取设备名称
			if deviceData, ok := uc.fetchDeviceName(ctx, ip); ok {
				// 尝试将数据发送到通道
				select {
				case dataChan <- deviceData: // 发送数据到通道
					// 数据发送成功
				case <-ctx.Done(): // 检查上下文是否已取消
					return // 如果上下文取消，直接返回
				}
			}
		}(ip) // 传递IP参数到goroutine
	}

	// 启动goroutine等待所有请求完成并关闭通道
	go func() {
		wg.Wait()       // 等待所有goroutine完成
		close(dataChan) // 关闭数据通道，通知接收方没有更多数据
	}()

	// 收集结果，从通道中读取所有设备数据
	var data []DeviceData              // 初始化结果切片
	for deviceData := range dataChan { // 从通道中读取数据直到通道关闭
		data = append(data, deviceData) // 将设备数据添加到结果切片
	}

	// 返回JSON响应，包含所有收集到的设备数据
	c.JSON(http.StatusOK, data)
}

// fetchDeviceName 辅助函数：获取指定IP的设备名称
// 参数：ctx - 上下文，ip - 目标IP地址
// 返回：DeviceData - 设备数据，bool - 是否成功获取
func (uc *UserController) fetchDeviceName(ctx context.Context, ip string) (DeviceData, bool) {
	// 创建带超时的HTTP客户端，设置合理的超时时间
	client := &http.Client{
		Timeout: 5 * time.Second, // HTTP请求超时时间为5秒
		Transport: &http.Transport{ // 自定义传输配置
			DialContext: (&net.Dialer{
				Timeout: 2 * time.Second, // 连接超时时间为2秒
			}).DialContext,
			TLSHandshakeTimeout: 2 * time.Second, // TLS握手超时时间
		},
	}

	// 构造请求URL，使用配置的端口号
	url := fmt.Sprintf("http://%s:8181/geo/GetDeviceName", ip)

	// 创建HTTP GET请求，并绑定上下文
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("Failed to create request for %s: %v", ip, err) // 记录请求创建失败的错误
		return DeviceData{}, false                                 // 返回空数据和失败标志
	}

	// 发送HTTP请求
	resp, err := client.Do(req)
	if err != nil {
		// 检查是否是上下文取消导致的错误
		if ctx.Err() != nil {
			log.Printf("Request to %s cancelled due to timeout", ip) // 记录超时取消的日志
		} else {
			log.Printf("Failed to get device name from %s: %v", ip, err) // 记录其他网络错误
		}
		return DeviceData{}, false // 返回空数据和失败标志
	}
	defer func() { // 确保响应体被正确关闭，防止资源泄漏
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Printf("Failed to close response body for %s: %v", ip, closeErr) // 记录关闭失败的错误
		}
	}()

	// 检查HTTP响应状态码是否表示成功
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("Received non-success status %d from %s", resp.StatusCode, ip) // 记录非成功状态码
		return DeviceData{}, false                                                // 返回空数据和失败标志
	}

	// 读取响应体内容
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Failed to read response body from %s: %v", ip, err) // 记录读取响应体失败的错误
		return DeviceData{}, false                                      // 返回空数据和失败标志
	}

	// 验证响应体内容不为空
	if len(body) == 0 {
		log.Printf("Received empty response from %s", ip) // 记录空响应的日志
		return DeviceData{}, false                        // 返回空数据和失败标志
	}

	// 构造并返回设备数据
	deviceData := DeviceData{
		IP:         ip,           // 设备IP地址
		DeviceName: string(body), // 设备名称（从响应体转换而来）
	}

	log.Printf("Successfully retrieved device name '%s' from %s", deviceData.DeviceName, ip) // 记录成功获取的日志
	return deviceData, true                                                                  // 返回设备数据和成功标志
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
	DSN := fmt.Sprintf("host=%s user=postgres password=1 dbname=GL port=5432 sslmode=disable TimeZone=UTC", ip)
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
			Geo = "MULTILINESTRING"
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
	DSN := fmt.Sprintf("host=%s user=postgres password=1 dbname=GL port=5432 sslmode=disable TimeZone=UTC", ip)
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
	copyFile("./OutFile/"+taskid+"/"+tablename+".zip", config.MainConfig.Download)

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
	pgmvt.UpdateSHPDirectly(DB, shp, Schema.EN, Schema.CN, Schema.Main, Schema.Color, Schema.Opacity, Schema.Userunits, addType, Schema.LineWidth)
	return true
}

func GDBToLayer(tablename string, gdbs []string, addType string) bool {
	DB := models.DB
	var Schema models.MySchema
	DB.Where("en = ?", tablename).First(&Schema)
	for _, gdb := range gdbs {
		pgmvt.UpdateGDBDirectly(DB, gdb, Schema.EN, Schema.CN, Schema.Main, Schema.Color, Schema.Opacity, Schema.Userunits, addType, Schema.LineWidth)
	}

	return true
}
func (uc *UserController) UpdateLayer(c *gin.Context) {
	tablename := c.PostForm("tablename")
	file, err := c.FormFile("file")
	VectorPath := c.PostForm("VectorPath")
	if err == nil {
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
	if VectorPath != "" {
		ext := strings.ToLower(filepath.Ext(VectorPath))
		if ext == ".shp" {
			aa := shpToLayer(tablename, VectorPath, "覆盖")
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
		if ext == ".gdb" {
			aa := GDBToLayer(tablename, []string{VectorPath}, "覆盖")
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

}
func (uc *UserController) AppendLayer(c *gin.Context) {
	tablename := c.PostForm("tablename")
	file, err := c.FormFile("file")
	VectorPath := c.PostForm("VectorPath")
	if err == nil {
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
			aa := GDBToLayer(tablename, gdbfiles, "追加")
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
	if VectorPath != "" {
		ext := strings.ToLower(filepath.Ext(VectorPath))
		if ext == ".shp" {
			aa := shpToLayer(tablename, VectorPath, "追加")
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
		if ext == ".gdb" {
			aa := GDBToLayer(tablename, []string{VectorPath}, "追加")
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

//离线更新

// TableBackupData 表备份数据结构
type TableBackupData struct {
	TableName    string                   `json:"table_name"`
	Schema       models.MySchema          `json:"my_schema"`    // my_schema表中的记录
	TableData    []map[string]interface{} `json:"table_data"`   // 几何表数据
	AttColors    []models.AttColor        `json:"att_color"`    // 属性颜色配置
	ChineseProps []models.ChineseProperty `json:"chinese_prop"` // 中文属性配置
	Columns      []ColumnIfo              `json:"columns"`      // 表结构信息
	BackupTime   string                   `json:"backup_time"`  // 备份时间
}

// ExportTableToFile 将指定表的数据导出为静态文件
// tableName: 要导出的表名
// outputDir: 输出目录
// 返回: 生成的文件路径和错误信息
func ExportTableToFile(tableName string, outputDir string) (string, error) {
	DB := models.DB

	// 创建输出目录
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", fmt.Errorf("创建输出目录失败: %v", err)
	}

	// 1. 获取schema信息
	var schema models.MySchema
	if err := DB.Where("EN = ?", tableName).First(&schema).Error; err != nil {
		return "", fmt.Errorf("获取schema信息失败: %v", err)
	}

	// 2. 获取表数据
	var tableData []map[string]interface{}
	if err := DB.Table(tableName).Find(&tableData).Error; err != nil {
		return "", fmt.Errorf("获取表数据失败: %v", err)
	}

	// 3. 获取属性颜色配置
	var attColors []models.AttColor
	DB.Where("layer_name = ?", tableName).Find(&attColors)

	// 4. 获取中文属性配置
	var chineseProps []models.ChineseProperty
	DB.Where("layer_name = ?", tableName).Find(&chineseProps)

	// 5. 获取表结构信息
	columnSQL := fmt.Sprintf(`SELECT column_name, data_type, character_maximum_length
		FROM information_schema.columns
		WHERE table_name = '%s'`, tableName)
	var columns []ColumnIfo
	DB.Raw(columnSQL).Scan(&columns)

	// 6. 构建备份数据
	backupData := TableBackupData{
		TableName:    tableName,
		Schema:       schema,
		TableData:    tableData,
		AttColors:    attColors,
		ChineseProps: chineseProps,
		Columns:      columns,
		BackupTime:   time.Now().Format("2006-01-02 15:04:05"),
	}

	// 7. 序列化为JSON
	jsonData, err := json.Marshal(backupData)
	if err != nil {
		return "", fmt.Errorf("JSON序列化失败: %v", err)
	}

	// 8. 生成文件名（包含时间戳）
	timestamp := time.Now().Format("20060102_150405")
	fileName := fmt.Sprintf("%s_backup_%s.json.gz", tableName, timestamp)
	filePath := filepath.Join(outputDir, fileName)

	// 9. 创建压缩文件
	file, err := os.Create(filePath)
	if err != nil {
		return "", fmt.Errorf("创建文件失败: %v", err)
	}
	defer file.Close()

	// 10. 使用gzip压缩写入
	gzWriter := gzip.NewWriter(file)
	defer gzWriter.Close()

	if _, err := gzWriter.Write(jsonData); err != nil {
		return "", fmt.Errorf("写入压缩文件失败: %v", err)
	}

	fmt.Printf("表 %s 数据已成功导出到: %s\n", tableName, filePath)
	fmt.Printf("备份包含 %d 条记录\n", len(tableData))

	return filePath, nil
}

// ImportTableFromFile 从静态文件恢复表数据到数据库
// filePath: 备份文件路径
// targetDB: 目标数据库连接
// 返回: 错误信息
func ImportTableFromFile(filePath string, targetDB *gorm.DB) error {
	// 1. 打开并读取压缩文件
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("打开文件失败: %v", err)
	}
	defer file.Close()

	// 2. 创建gzip读取器
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("创建gzip读取器失败: %v", err)
	}
	defer gzReader.Close()

	// 3. 读取并解压数据
	var backupData TableBackupData
	decoder := json.NewDecoder(gzReader)
	if err := decoder.Decode(&backupData); err != nil {
		return fmt.Errorf("JSON反序列化失败: %v", err)
	}

	tableName := backupData.TableName
	fmt.Printf("开始恢复表: %s (备份时间: %s)\n", tableName, backupData.BackupTime)

	// 4. 开始数据库事务
	tx := targetDB.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			fmt.Printf("恢复过程中发生错误，已回滚: %v\n", r)
		}
	}()

	// 5. 检查并更新my_schema表
	// 使用 Save 方法，如果记录存在则更新，不存在则创建
	schemaToSave := backupData.Schema

	// 先查询是否存在
	var existingSchema models.MySchema
	err = tx.Table("my_schema").Where("EN = ?", tableName).First(&existingSchema).Error

	if err != nil {
		fmt.Println(err.Error())
		// 记录不存在，创建新记录
		schemaToSave.ID = 0 // 清除ID让数据库自动分配
		if err := tx.Table("my_schema").Create(&schemaToSave).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("创建schema记录失败: %v", err)
		}
		fmt.Printf("已创建schema记录\n")

	} else {
		// 记录存在，保留原有ID进行更新
		schemaToSave.ID = existingSchema.ID
		if err := tx.Table("my_schema").Save(&schemaToSave).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("更新schema记录失败: %v", err)
		}
		fmt.Printf("已更新schema记录\n")
	}

	// 6. 创建或重建几何表
	if err := createTableFromColumns(tx, tableName, backupData.Columns, backupData.Schema); err != nil {
		tx.Rollback()
		return fmt.Errorf("创建几何表失败: %v", err)
	}

	// 7. 清空现有数据并插入新数据
	if err := tx.Table(tableName).Where("id != ?", 0).Delete(nil).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("清空表数据失败: %v", err)
	}

	// 8. 批量插入数据
	if len(backupData.TableData) > 0 {
		if err := batchInsertData(tx, tableName, backupData.TableData); err != nil {
			tx.Rollback()
			return fmt.Errorf("插入表数据失败: %v", err)
		}
		fmt.Printf("已插入 %d 条记录\n", len(backupData.TableData))
	}

	// 9. 清空并重建MVT缓存表
	mvtTableName := tableName + "mvt"
	if isEndWithNumber(tableName) {
		mvtTableName = tableName + "_mvt"
	}
	tx.Table(mvtTableName).Where("id != ?", 0).Delete(nil)

	// 10. 更新配置表 - AttColor
	if len(backupData.AttColors) > 0 {
		// 删除现有配置
		if err := tx.Where("layer_name = ?", tableName).Delete(&models.AttColor{}).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("删除现有AttColor配置失败: %v", err)
		}

		// 逐条插入，便于定位问题
		for i, attColor := range backupData.AttColors {
			attColor.ID = 0 // 清除主键
			if err := tx.Create(&attColor).Error; err != nil {
				tx.Rollback()
				return fmt.Errorf("插入第 %d 条AttColor配置失败: %v", i+1, err)
			}
		}
		fmt.Printf("已更新 %d 条AttColor配置\n", len(backupData.AttColors))
	}

	// 11. 更新配置表 - ChineseProperty
	if len(backupData.ChineseProps) > 0 {
		// 删除现有配置
		if err := tx.Where("layer_name = ?", tableName).Delete(&models.ChineseProperty{}).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("删除现有ChineseProperty配置失败: %v", err)
		}

		// 逐条插入，便于定位问题
		for i, chineseProp := range backupData.ChineseProps {
			chineseProp.ID = 0 // 清除主键
			if err := tx.Create(&chineseProp).Error; err != nil {
				tx.Rollback()
				return fmt.Errorf("插入第 %d 条ChineseProperty配置失败: %v", i+1, err)
			}
		}
		fmt.Printf("已更新 %d 条ChineseProperty配置\n", len(backupData.ChineseProps))
	}

	// 12. 提交事务
	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("提交事务失败: %v", err)
	}

	fmt.Printf("表 %s 数据恢复完成\n", tableName)
	return nil
}

// createTableFromColumns 根据列信息创建表
func createTableFromColumns(db *gorm.DB, tableName string, columns []ColumnIfo, schema models.MySchema) error {
	// 确定几何类型
	var geoType string
	schemaType := schema.Type
	switch schemaType {
	case "line":
		geoType = "MULTILINESTRING"
	case "polygon":
		geoType = "MULTIPOLYGON"
	case "point":
		geoType = "POINT"
	default:
		geoType = "GEOMETRY"
	}

	// 构建字段定义
	var fields []string
	for _, col := range columns {
		if col.ColumnName != "id" && col.ColumnName != "geom" {
			var fieldDef string
			switch col.DataType {
			case "character varying":
				fieldDef = fmt.Sprintf(`%s VARCHAR(%s)`, col.ColumnName, col.CharacterMaximumLength)
			case "integer":
				fieldDef = fmt.Sprintf(`%s INT`, col.ColumnName)
			case "boolean":
				fieldDef = fmt.Sprintf(`%s BOOLEAN`, col.ColumnName)
			case "real", "double precision":
				fieldDef = fmt.Sprintf(`%s FLOAT`, col.ColumnName)
			case "time without time zone":
				fieldDef = fmt.Sprintf(`%s TIME`, col.ColumnName)
			case "bigint":
				fieldDef = fmt.Sprintf(`%s BIGINT`, col.ColumnName)
			case "numeric":
				fieldDef = fmt.Sprintf(`%s NUMERIC`, col.ColumnName)
			case "text":
				fieldDef = fmt.Sprintf(`%s TEXT`, col.ColumnName)
			case "bytea":
				fieldDef = fmt.Sprintf(`%s BYTEA`, col.ColumnName)
			case "date":
				fieldDef = fmt.Sprintf(`%s DATE`, col.ColumnName)
			default:
				fieldDef = fmt.Sprintf(`%s %s`, col.ColumnName, col.DataType)
			}
			fields = append(fields, fieldDef)
		}
	}

	// 添加主键
	fields = append(fields, "id SERIAL PRIMARY KEY")

	// 去重
	fields = removeDuplicates(fields)
	fieldDefs := strings.Join(fields, ",")

	// 删除现有表
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	if err := db.Exec(dropSQL).Error; err != nil {
		return fmt.Errorf("删除现有表失败: %v", err)
	}

	// 创建新表
	createSQL := fmt.Sprintf("CREATE TABLE %s (%s, geom GEOMETRY(%s, 4326))", tableName, fieldDefs, geoType)
	if err := db.Exec(createSQL).Error; err != nil {
		return fmt.Errorf("创建表失败: %v", err)
	}

	// 创建MVT缓存表
	mvtTableName := tableName + "mvt"
	if isEndWithNumber(tableName) {
		mvtTableName = tableName + "_mvt"
	}

	mvtSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (ID SERIAL PRIMARY KEY, X INT8, Y INT8, Z INT8, Byte BYTEA)", mvtTableName)
	if err := db.Exec(mvtSQL).Error; err != nil {
		return fmt.Errorf("创建MVT表失败: %v", err)
	}

	return nil
}

// batchInsertData 批量插入数据
func batchInsertData(db *gorm.DB, tableName string, data []map[string]interface{}) error {
	const batchSize = 1000
	totalRecords := len(data)

	for i := 0; i < totalRecords; i += batchSize {
		end := i + batchSize
		if end > totalRecords {
			end = totalRecords
		}

		batch := data[i:end]
		if err := db.Table(tableName).Create(&batch).Error; err != nil {
			return fmt.Errorf("批量插入第 %d-%d 条记录失败: %v", i+1, end, err)
		}
	}

	return nil
}

// 导出表数据到文件
func ExportTable(tableName string, outputDir string) (string, error) {
	filePath, err := ExportTableToFile(tableName, outputDir)
	if err != nil {
		log.Printf("导出失败: %v", err)
		return "", err
	}

	// 将路径中的反斜杠替换为正斜杠
	normalizedPath := strings.ReplaceAll(filePath, "\\", "/")

	log.Printf("导出成功，文件路径: %s", normalizedPath)
	return normalizedPath, nil
}

// 从文件恢复表数据
func ExampleImport(filePath string, ip string) error {

	// 连接目标数据库
	DSN := fmt.Sprintf("host=%s user=postgres password=1 dbname=GL port=5432 sslmode=disable TimeZone=UTC", ip)
	targetDB, err := gorm.Open(postgres.Open(DSN), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Printf("连接数据库失败: %v", err)
		return err
	}

	// 确保数据库连接被正确释放
	defer func() {
		if sqlDB, err := targetDB.DB(); err == nil {
			sqlDB.Close()
		}
	}()
	targetDB.NamingStrategy = schema.NamingStrategy{
		SingularTable: true,
	}
	if err := ImportTableFromFile(filePath, targetDB); err != nil {
		log.Printf("恢复失败: %v", err)
		return err
	}

	log.Printf("恢复成功")
	return nil
}

// 数据导出
func (uc *UserController) DownloadOfflineLayer(c *gin.Context) {
	table := c.Query("tablename")
	taskid := uuid.New().String()
	existingZipPath := "OutFile/" + taskid + "/" + table
	outpath, err := ExportTable(table, existingZipPath)
	if err != nil {
		c.String(500, "数据无法导出")
	}
	host := c.Request.Host
	url := &url.URL{
		Scheme: "http",
		Host:   host,
		Path:   "/geo/" + outpath,
	}
	c.String(http.StatusOK, url.String())

}

// 数据回复
func (uc *UserController) RestoreOfflineLayer(c *gin.Context) {
	filePath := c.Query("filePath")
	err := ExampleImport(filePath, "127.0.0.1")
	if err != nil {
		c.String(500, err.Error())
	}
	c.String(http.StatusOK, "ok")

}

// GetReatoreFile 读取固定路径中的tar文件，并返回为[]string格式数据
func (uc *UserController) GetReatoreFile(c *gin.Context) {
	// 固定路径
	dirPath := "/storage/emulated/0/地图更新"

	// 递归遍历目录获取tar文件
	tarFiles, err := traverseDirectory(dirPath)
	if err != nil {
		c.JSON(http.StatusOK, []string{})
		return
	}

	c.JSON(http.StatusOK, tarFiles)
}

// traverseDirectory 递归遍历目录，查找tar文件
func traverseDirectory(dirPath string) ([]string, error) {
	var tarFiles []string
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// 如果遇到错误，跳过该文件/目录
			return nil
		}
		// 如果是文件且扩展名匹配
		if !info.IsDir() {
			ext := strings.ToLower(filepath.Ext(info.Name()))
			if ext == ".tar" || ext == ".gz" || ext == ".tgz" {
				// 可以选择返回相对路径或绝对路径

				tarFiles = append(tarFiles, path)
			}
		}
		return nil
	})

	return tarFiles, err
}
