package views

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"gitee.com/gooffice/gooffice/document"
	"github.com/GrainArc/Gogeo"
	"github.com/GrainArc/SouceMap/Transformer"
	"github.com/GrainArc/SouceMap/WordGenerator"
	"github.com/GrainArc/SouceMap/config"
	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/paulmach/orb/geojson"
	"gorm.io/gorm"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// 临时数据导入
func (uc *UserController) InTempLayer(c *gin.Context) {
	Layername := c.PostForm("layername")
	MAC := c.PostForm("mac")
	DB := models.DB
	VectorPath := c.PostForm("VectorPath")
	GeoJSONStr := c.PostForm("geojson") // 新增：获取geojson字符串
	taskid := uuid.New().String()
	file, err := c.FormFile("file")
	currentTime := time.Now().Format("2006-01-02 15:04:05")

	// 处理GeoJSON字符串上传
	if GeoJSONStr != "" {
		if err := uc.processGeoJSONString(GeoJSONStr, Layername, MAC, taskid, currentTime, DB); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, map[string]string{"bsm": taskid})
		return
	}

	// 处理文件上传
	if err == nil {
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

		// 检查各种文件类型
		if uc.processFileType(dirpath, Layername, MAC, taskid, currentTime, DB) {
			c.JSON(http.StatusOK, map[string]string{"bsm": taskid})
			return
		}
	}

	// 处理VectorPath
	if VectorPath != "" {
		if uc.processVectorPath(VectorPath, Layername, MAC, taskid, currentTime, DB) {
			c.JSON(http.StatusOK, map[string]string{"bsm": taskid})
			return
		}
	}

	c.JSON(http.StatusBadRequest, gin.H{"error": "请上传文件或提供有效的geojson数据"})
}

// 处理GeoJSON字符串
func (uc *UserController) processGeoJSONString(geoJSONStr, layername, mac, taskid, currentTime string, DB *gorm.DB) error {
	var data geojson.FeatureCollection
	if err := json.Unmarshal([]byte(geoJSONStr), &data); err != nil {
		return fmt.Errorf("无效的GeoJSON格式: %v", err)
	}

	if len(data.Features) == 0 {
		return fmt.Errorf("GeoJSON中没有特征数据")
	}

	// 保存特征数据
	for index, feature := range data.Features {
		tbid := uuid.New().String()
		feature.ID = tbid

		// 获取名称
		name := uc.getFeatureName(feature, index)
		feature.Properties["name"] = name

		// 创建FeatureCollection并保存
		featureCollection := geojson.NewFeatureCollection()
		featureCollection.Append(feature)
		geoJSONData, _ := json.MarshalIndent(featureCollection, "", "  ")

		result := models.TempLayer{
			Layername: layername,
			Name:      name,
			MAC:       mac,
			BSM:       taskid,
			TBID:      tbid,
			Geojson:   geoJSONData,
		}
		result_att := models.TempLayerAttribute{
			TBID:      tbid,
			Layername: layername,
		}

		if err := DB.Create(&result_att).Error; err != nil {
			fmt.Println("保存属性失败:", err.Error())
		}
		if err := DB.Create(&result).Error; err != nil {
			fmt.Println("保存图层失败:", err.Error())
		}
	}

	// 保存头部信息
	header := models.TempLayHeader{
		Layername: layername,
		MAC:       mac,
		Date:      currentTime,
		BSM:       taskid,
	}
	DB.Create(&header)

	return nil
}

// 处理文件类型
func (uc *UserController) processFileType(dirpath, layername, mac, taskid, currentTime string, DB *gorm.DB) bool {
	// DXF文件
	if uc.processDXFFiles(dirpath, layername, mac, taskid, currentTime, DB) {
		return true
	}

	// SHP文件
	if uc.processSHPFiles(dirpath, layername, mac, taskid, currentTime, DB) {
		return true
	}

	// KML文件
	if uc.processKMLFiles(dirpath, layername, mac, taskid, currentTime, DB) {
		return true
	}

	// OVKML文件
	if uc.processOVKMLFiles(dirpath, layername, mac, taskid, currentTime, DB) {
		return true
	}

	// GDB文件
	if uc.processGDBFiles(dirpath, layername, mac, taskid, currentTime, DB) {
		return true
	}

	// DAT文件
	if uc.processDATFiles(dirpath, layername, mac, taskid, currentTime, DB) {
		return true
	}

	// TXT文件
	if uc.processTXTFiles(dirpath, layername, mac, taskid, currentTime, DB) {
		return true
	}

	return false
}

// 处理DXF文件
func (uc *UserController) processDXFFiles(dirpath, layername, mac, taskid, currentTime string, DB *gorm.DB) bool {
	dxffiles := Transformer.FindFiles(dirpath, "dxf")
	if len(dxffiles) == 0 {
		return false
	}

	for _, item := range dxffiles {
		data, isTransform := Transformer.ConvertDXFToGeoJSON2(item)
		if isTransform != "" {
			data, _ = Transformer.GeoJsonTransformTo4326(data, isTransform)
		}

		for _, feature := range data.Features {
			uc.saveFeature(feature, layername, mac, taskid, DB)
		}
	}

	uc.saveHeader(layername, mac, taskid, currentTime, DB)
	return true
}

// 处理SHP文件
func (uc *UserController) processSHPFiles(dirpath, layername, mac, taskid, currentTime string, DB *gorm.DB) bool {
	shpfiles := Transformer.FindFiles(dirpath, "shp")
	if len(shpfiles) == 0 {
		return false
	}

	for _, item := range shpfiles {
		Gdata, err := Gogeo.ReadShapeFileLayer(item)
		if err != nil {
			return false
		}
		data, err := Gogeo.LayerToGeoJSON(Gdata)
		if err != nil {
			return false
		}

		for index, feature := range data.Features {
			name := uc.getCleanedName(feature, index)
			feature.Properties["name"] = name
			uc.saveFeatureWithName(feature, name, layername, mac, taskid, DB)
		}
	}

	uc.saveHeader(layername, mac, taskid, currentTime, DB)
	return true
}

// 处理KML文件
func (uc *UserController) processKMLFiles(dirpath, layername, mac, taskid, currentTime string, DB *gorm.DB) bool {
	kmlfiles := Transformer.FindFiles(dirpath, "kml")
	if len(kmlfiles) == 0 {
		return false
	}

	for _, item := range kmlfiles {
		data, isTransform := Transformer.KmlToGeojson(item)
		if isTransform != "4326" {
			data, _ = Transformer.GeoJsonTransformTo4326(data, isTransform)
		}

		for index, feature := range data.Features {
			name := uc.getFeatureName(feature, index)
			feature.Properties["name"] = name
			uc.saveFeatureWithName(feature, name, layername, mac, taskid, DB)
		}
	}

	uc.saveHeader(layername, mac, taskid, currentTime, DB)
	return true
}

// 处理OVKML文件
func (uc *UserController) processOVKMLFiles(dirpath, layername, mac, taskid, currentTime string, DB *gorm.DB) bool {
	ovkmlfiles := Transformer.FindFiles(dirpath, "ovkml")
	if len(ovkmlfiles) == 0 {
		return false
	}

	for _, item := range ovkmlfiles {
		data, isTransform := Transformer.KmlToGeojson(item)
		if isTransform != "4326" {
			data, _ = Transformer.GeoJsonTransformTo4326(data, isTransform)
		}

		for index, feature := range data.Features {
			name := uc.getFeatureName(feature, index)
			feature.Properties["name"] = name
			uc.saveFeatureWithName(feature, name, layername, mac, taskid, DB)
		}
	}

	uc.saveHeader(layername, mac, taskid, currentTime, DB)
	return true
}

// 处理GDB文件
func (uc *UserController) processGDBFiles(dirpath, layername, mac, taskid, currentTime string, DB *gorm.DB) bool {
	gdbfiles := Transformer.FindFiles(dirpath, "gdb")
	if len(gdbfiles) == 0 {
		return false
	}

	for _, item := range gdbfiles {
		Layers, _ := Gogeo.GDBToGeoJSON(item)
		for _, layer := range Layers {
			for index, feature := range layer.Layer.Features {
				name := uc.getFeatureName(feature, index)
				feature.Properties["name"] = name
				uc.saveFeatureWithName(feature, name, layername, mac, taskid, DB)
			}
		}
	}

	uc.saveHeader(layername, mac, taskid, currentTime, DB)
	return true
}

// 处理DAT文件
func (uc *UserController) processDATFiles(dirpath, layername, mac, taskid, currentTime string, DB *gorm.DB) bool {
	datfiles := Transformer.FindFiles(dirpath, "dat")
	if len(datfiles) == 0 {
		return false
	}

	for _, item := range datfiles {
		data, isTransform := Transformer.DatToGeojson(item)
		if isTransform != "4326" {
			data, _ = Transformer.GeoJsonTransformTo4326(data, isTransform)
		}

		for index, feature := range data.Features {
			name := uc.getFeatureName(feature, index)
			feature.Properties["name"] = name
			uc.saveFeatureWithName(feature, name, layername, mac, taskid, DB)
		}
	}

	uc.saveHeader(layername, mac, taskid, currentTime, DB)
	return true
}

// 处理TXT文件
func (uc *UserController) processTXTFiles(dirpath, layername, mac, taskid, currentTime string, DB *gorm.DB) bool {
	txtfiles := Transformer.FindFiles(dirpath, "txt")
	if len(txtfiles) == 0 {
		return false
	}

	for _, item := range txtfiles {
		data, isTransform := Transformer.TxtToGeojson(item)
		if isTransform != "4326" {
			data, _ = Transformer.GeoJsonTransformTo4326(data, isTransform)
		}

		for index, feature := range data.Features {
			name := uc.getFeatureName(feature, index)
			feature.Properties["name"] = name
			uc.saveFeatureWithName(feature, name, layername, mac, taskid, DB)
		}
	}

	uc.saveHeader(layername, mac, taskid, currentTime, DB)
	return true
}

// 处理VectorPath
func (uc *UserController) processVectorPath(vectorPath, layername, mac, taskid, currentTime string, DB *gorm.DB) bool {
	ext := strings.ToLower(filepath.Ext(vectorPath))

	switch ext {
	case ".dxf":
		return uc.processDXFPath(vectorPath, layername, mac, taskid, currentTime, DB)
	case ".shp":
		return uc.processSHPPath(vectorPath, layername, mac, taskid, currentTime, DB)
	case ".kml":
		return uc.processKMLPath(vectorPath, layername, mac, taskid, currentTime, DB)
	case ".ovkml":
		return uc.processOVKMLPath(vectorPath, layername, mac, taskid, currentTime, DB)
	case ".gdb":
		return uc.processGDBPath(vectorPath, layername, mac, taskid, currentTime, DB)
	case ".dat":
		return uc.processDATPath(vectorPath, layername, mac, taskid, currentTime, DB)
	case ".txt":
		return uc.processTXTPath(vectorPath, layername, mac, taskid, currentTime, DB)
	}

	return false
}

// DXF路径处理
func (uc *UserController) processDXFPath(vectorPath, layername, mac, taskid, currentTime string, DB *gorm.DB) bool {
	data, isTransform := Transformer.ConvertDXFToGeoJSON2(vectorPath)
	if isTransform != "" {
		data, _ = Transformer.GeoJsonTransformTo4326(data, isTransform)
	}

	for _, feature := range data.Features {
		uc.saveFeature(feature, layername, mac, taskid, DB)
	}

	uc.saveHeader(layername, mac, taskid, currentTime, DB)
	return true
}

// SHP路径处理
func (uc *UserController) processSHPPath(vectorPath, layername, mac, taskid, currentTime string, DB *gorm.DB) bool {
	data, isTransform := Transformer.ConvertSHPToGeoJSON2(vectorPath)
	if isTransform != "4326" {
		data, _ = Transformer.GeoJsonTransformTo4326(data, isTransform)
	}

	for index, feature := range data.Features {
		name := uc.getCleanedName(feature, index)
		feature.Properties["name"] = name
		uc.saveFeatureWithName(feature, name, layername, mac, taskid, DB)
	}

	uc.saveHeader(layername, mac, taskid, currentTime, DB)
	return true
}

// KML路径处理
func (uc *UserController) processKMLPath(vectorPath, layername, mac, taskid, currentTime string, DB *gorm.DB) bool {
	data, isTransform := Transformer.KmlToGeojson(vectorPath)
	if isTransform != "4326" {
		data, _ = Transformer.GeoJsonTransformTo4326(data, isTransform)
	}

	for index, feature := range data.Features {
		name := uc.getFeatureName(feature, index)
		feature.Properties["name"] = name
		uc.saveFeatureWithName(feature, name, layername, mac, taskid, DB)
	}

	uc.saveHeader(layername, mac, taskid, currentTime, DB)
	return true
}

// OVKML路径处理
func (uc *UserController) processOVKMLPath(vectorPath, layername, mac, taskid, currentTime string, DB *gorm.DB) bool {
	data, isTransform := Transformer.KmlToGeojson(vectorPath)
	if isTransform != "4326" {
		data, _ = Transformer.GeoJsonTransformTo4326(data, isTransform)
	}

	for index, feature := range data.Features {
		name := uc.getFeatureName(feature, index)
		feature.Properties["name"] = name
		uc.saveFeatureWithName(feature, name, layername, mac, taskid, DB)
	}

	uc.saveHeader(layername, mac, taskid, currentTime, DB)
	return true
}

// GDB路径处理
func (uc *UserController) processGDBPath(vectorPath, layername, mac, taskid, currentTime string, DB *gorm.DB) bool {
	Layers, _ := Gogeo.GDBToGeoJSON(vectorPath)
	for _, layer := range Layers {
		for index, feature := range layer.Layer.Features {
			name := uc.getFeatureName(feature, index)
			feature.Properties["name"] = name
			uc.saveFeatureWithName(feature, name, layername, mac, taskid, DB)
		}
	}

	uc.saveHeader(layername, mac, taskid, currentTime, DB)
	return true
}

// DAT路径处理
func (uc *UserController) processDATPath(vectorPath, layername, mac, taskid, currentTime string, DB *gorm.DB) bool {
	data, isTransform := Transformer.DatToGeojson(vectorPath)
	if isTransform != "4326" {
		data, _ = Transformer.GeoJsonTransformTo4326(data, isTransform)
	}

	for index, feature := range data.Features {
		name := uc.getFeatureName(feature, index)
		feature.Properties["name"] = name
		uc.saveFeatureWithName(feature, name, layername, mac, taskid, DB)
	}

	uc.saveHeader(layername, mac, taskid, currentTime, DB)
	return true
}

// TXT路径处理
func (uc *UserController) processTXTPath(vectorPath, layername, mac, taskid, currentTime string, DB *gorm.DB) bool {
	data, isTransform := Transformer.TxtToGeojson(vectorPath)
	if isTransform != "4326" {
		data, _ = Transformer.GeoJsonTransformTo4326(data, isTransform)
	}

	for index, feature := range data.Features {
		name := uc.getFeatureName(feature, index)
		feature.Properties["name"] = name
		uc.saveFeatureWithName(feature, name, layername, mac, taskid, DB)
	}

	uc.saveHeader(layername, mac, taskid, currentTime, DB)
	return true
}

// 获取特征名称
func (uc *UserController) getFeatureName(feature *geojson.Feature, index int) string {
	if _, exists := feature.Properties["name"]; exists {
		return feature.Properties["name"].(string)
	}
	return strconv.Itoa(index)
}

// 获取清理后的名称（处理UTF-8）
func (uc *UserController) getCleanedName(feature *geojson.Feature, index int) string {
	if _, exists := feature.Properties["name"]; exists {
		rawName := feature.Properties["name"].(string)

		// 清理空字符和其他非法字符
		cleanedName := strings.Map(func(r rune) rune {
			if r == 0x00 || !utf8.ValidRune(r) {
				return -1
			}
			return r
		}, rawName)

		// 检查并修复 UTF-8 编码
		if !utf8.ValidString(cleanedName) {
			validName := string([]rune(cleanedName))
			log.Printf("检测到非 UTF-8 编码字符串，已修复: %s -> %s", cleanedName, validName)
			cleanedName = validName
		}

		return cleanedName
	}
	return strconv.Itoa(index)
}

// 保存特征（无名称）
func (uc *UserController) saveFeature(feature *geojson.Feature, layername, mac, taskid string, DB *gorm.DB) {
	tbid := uuid.New().String()
	feature.ID = tbid

	featureCollection := geojson.NewFeatureCollection()
	featureCollection.Append(feature)
	geoJSONData, _ := json.MarshalIndent(featureCollection, "", "  ")

	result := models.TempLayer{
		Layername: layername,
		MAC:       mac,
		BSM:       taskid,
		TBID:      tbid,
		Geojson:   geoJSONData,
	}
	result_att := models.TempLayerAttribute{
		TBID:      tbid,
		Layername: layername,
	}

	if err := DB.Create(&result_att).Error; err != nil {
		fmt.Println("保存属性失败:", err.Error())
	}
	if err := DB.Create(&result).Error; err != nil {
		fmt.Println("保存图层失败:", err.Error())
	}
}

// 保存特征（带名称）
func (uc *UserController) saveFeatureWithName(feature *geojson.Feature, name, layername, mac, taskid string, DB *gorm.DB) {
	tbid := uuid.New().String()
	feature.ID = tbid

	featureCollection := geojson.NewFeatureCollection()
	featureCollection.Append(feature)
	geoJSONData, _ := json.MarshalIndent(featureCollection, "", "  ")

	result := models.TempLayer{
		Layername: layername,
		Name:      name,
		MAC:       mac,
		BSM:       taskid,
		TBID:      tbid,
		Geojson:   geoJSONData,
	}
	result_att := models.TempLayerAttribute{
		TBID:      tbid,
		Layername: layername,
	}

	if err := DB.Create(&result_att).Error; err != nil {
		fmt.Println("保存属性失败:", err.Error())
	}
	if err := DB.Create(&result).Error; err != nil {
		fmt.Println("保存图层失败:", err.Error())
	}
}

// 保存头部信息
func (uc *UserController) saveHeader(layername, mac, taskid, currentTime string, DB *gorm.DB) {
	header := models.TempLayHeader{
		Layername: layername,
		MAC:       mac,
		Date:      currentTime,
		BSM:       taskid,
	}
	DB.Create(&header)
}

// 临时数据获取
func (uc *UserController) ShowTempLayer(c *gin.Context) {
	var jsonData map[string]interface{}
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		// 处理JSON数据绑定错误
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	layernames := jsonData["bsm"].([]interface{})

	var mytable []models.TempLayer
	DB := models.DB
	// 构建存放 layername 的切片
	layernameSlice := make([]string, len(layernames))
	for i, layername := range layernames {
		layernameSlice[i] = layername.(string)
	}
	// 使用IN条件一次性查询所有数据
	DB.Where("bsm IN ? ", layernameSlice).Find(&mytable)
	var data []*geojson.Feature
	for index, item := range mytable {
		var featureCollection struct {
			Features []*geojson.Feature `json:"features"`
		}

		error := json.Unmarshal(item.Geojson, &featureCollection)
		if error == nil {
			if _, exists := featureCollection.Features[0].Properties["name"]; exists {
			} else {
				featureCollection.Features[0].Properties["name"] = strconv.Itoa(index)
			}
			featureCollection.Features[0].ID = item.TBID
			featureCollection.Features[0].Properties["zt"] = item.ZT
			featureCollection.Features[0].Properties["TBID"] = item.TBID
			data = append(data, featureCollection.Features...)
		} else {
			str, _ := hex.DecodeString(string(item.Geojson))
			finalDecodedString := string(str)
			json.Unmarshal([]byte(finalDecodedString), &featureCollection)
			if _, exists := featureCollection.Features[0].Properties["name"]; exists {
			} else {
				featureCollection.Features[0].Properties["name"] = strconv.Itoa(index)
			}
			featureCollection.Features[0].ID = item.TBID
			featureCollection.Features[0].Properties["zt"] = item.ZT
			featureCollection.Features[0].Properties["TBID"] = item.TBID
			data = append(data, featureCollection.Features...)
		}

	}
	features := geojson.NewFeatureCollection()
	features.Features = data
	c.JSON(http.StatusOK, features)
}

func GetSingleGeo(TBID string) *geojson.FeatureCollection {
	var mytable []models.TempLayer
	DB := models.DB
	//图斑查询
	var table []models.TempGeo
	var data []*geojson.Feature
	DB.Where("tb_id = ?", TBID).Find(&table)
	features := geojson.NewFeatureCollection()
	if len(table) != 0 {
		for _, item := range table {
			var featureCollection struct {
				Features []*geojson.Feature `json:"features"`
			}
			json.Unmarshal(item.Geojson, &featureCollection)
			featureCollection.Features[0].Properties["TBID"] = item.TBID
			data = append(data, featureCollection.Features...)
		}

		features.Features = data

	} else {
		//图层查询
		DB.Where("tb_id = ? ", TBID).Find(&mytable)
		for _, item := range mytable {
			var featureCollection struct {
				Features []*geojson.Feature `json:"features"`
			}

			error := json.Unmarshal(item.Geojson, &featureCollection)
			if error == nil {
				featureCollection.Features[0].ID = item.ID
				featureCollection.Features[0].Properties["zt"] = item.ZT
				featureCollection.Features[0].Properties["TBID"] = item.TBID
				data = append(data, featureCollection.Features...)
			} else {
				str, _ := hex.DecodeString(string(item.Geojson))
				finalDecodedString := string(str)
				json.Unmarshal([]byte(finalDecodedString), &featureCollection)
				featureCollection.Features[0].ID = item.ID
				featureCollection.Features[0].Properties["zt"] = item.ZT
				featureCollection.Features[0].Properties["TBID"] = item.TBID
				data = append(data, featureCollection.Features...)
			}

		}
		features.Features = data

	}
	return features
}

// 临时数据单图斑获取
func (uc *UserController) ShowSingleGeo(c *gin.Context) {
	TBID := c.Query("TBID")
	data := GetSingleGeo(TBID)
	c.JSON(http.StatusOK, data)
}

// 坐标传入数据查询
func (uc *UserController) ShowSingleGeoByXY(c *gin.Context) {
	var jsonData map[string]interface{}
	c.BindJSON(&jsonData)
	X := jsonData["x"].(float64)
	Y := jsonData["y"].(float64)
	LayerName := jsonData["layername"].(string)
	MakeGeoIndex(LayerName)
	DB := models.DB
	sql := fmt.Sprintf(`
WITH input_point AS (
    SELECT ST_SetSRID(ST_MakePoint(%f, %f), 4326) AS geom
),
intersecting_areas AS (
    SELECT *
    FROM %s
    WHERE ST_Intersects(%s.geom, (SELECT geom FROM input_point))
)
SELECT 
    json_build_object(
        'type', 'FeatureCollection',
        'features', COALESCE(
            json_agg(
                json_build_object(
                    'type', 'Feature',
                    'geometry', ST_AsGeoJSON(geom)::json,
                    'properties', to_jsonb(intersecting_areas.*) - 'geom'
                )
            ),
            '[]'::json
        )
    ) AS geojson
FROM intersecting_areas;
    `, X, Y, LayerName, LayerName)

	// 执行SQL查询
	var result struct {
		GeoJSON []byte `gorm:"column:geojson"`
	}

	if err := DB.Raw(sql).Scan(&result).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	feature := geojson.NewFeatureCollection()
	json.Unmarshal(result.GeoJSON, &feature)
	if len(feature.Features) == 0 {
		c.JSON(400, "no data")
	}
	c.JSON(http.StatusOK, feature)
}

type BoxData struct {
	Box       []float64 `json:"box"`
	LayerName string    `json:"LayerName"`
}

// 通过Box坐标范围查询GeoJSON
func (uc *UserController) ShowGeoByBox(c *gin.Context) {
	var boxData BoxData
	if err := c.BindJSON(&boxData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request format"})
		return
	}

	// 验证box数据格式 [minX, minY, maxX, maxY]
	if len(boxData.Box) != 4 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Box must contain 4 coordinates [minX, minY, maxX, maxY]"})
		return
	}

	minX := boxData.Box[0]
	minY := boxData.Box[1]
	maxX := boxData.Box[2]
	maxY := boxData.Box[3]
	LayerName := boxData.LayerName

	MakeGeoIndex(LayerName)
	DB := models.DB

	sql := fmt.Sprintf(`
WITH input_box AS (
    SELECT ST_SetSRID(ST_MakeEnvelope(%f, %f, %f, %f), 4326) AS geom
),
intersecting_areas AS (
    SELECT *
    FROM %s
    WHERE ST_Intersects(%s.geom, (SELECT geom FROM input_box))
)
SELECT 
    json_build_object(
        'type', 'FeatureCollection',
        'features', COALESCE(
            json_agg(
                json_build_object(
                    'type', 'Feature',
                    'geometry', ST_AsGeoJSON(geom)::json,
                    'properties', to_jsonb(intersecting_areas.*) - 'geom'
                )
            ),
            '[]'::json
        )
    ) AS geojson
FROM intersecting_areas;
	`, minX, minY, maxX, maxY, LayerName, LayerName)

	// 执行SQL查询
	var result struct {
		GeoJSON []byte `gorm:"column:geojson"`
	}

	if err := DB.Raw(sql).Scan(&result).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	feature := geojson.NewFeatureCollection()
	if err := json.Unmarshal(result.GeoJSON, &feature); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse GeoJSON"})
		return
	}

	if len(feature.Features) == 0 {
		c.JSON(http.StatusOK, gin.H{"message": "no data"})
		return
	}

	c.JSON(http.StatusOK, feature)
}

// 临时表单数据获取
func (uc *UserController) ShowTempLayerHeader(c *gin.Context) {
	var jsonData map[string]interface{}
	c.BindJSON(&jsonData)
	DB := models.DB
	//查询用户模板权限
	query := DB.Model(&models.TempLayHeader{})
	mytable := methods.DataSearch(query, jsonData)
	for i, item := range mytable {
		var countTotal, countZT1 int64
		if err := DB.Model(&models.TempLayer{}).Where("bsm = ?", item.BSM).Count(&countTotal).Error; err != nil {
		}
		// 计算BSM为"1"且ZT为"1"的要素数量
		if err := DB.Model(&models.TempLayer{}).Where("bsm = ? AND zt = ?", item.BSM, "1").Count(&countZT1).Error; err != nil {
		}
		// 计算比例（百分比）
		var percentage float64
		if countTotal > 0 {
			percentage = float64(countZT1) / float64(countTotal) * 100
		} else {
			percentage = 0
		}
		mytable[i].Progress = percentage
	}
	DB.Updates(&mytable)
	if len(mytable) == 0 {
		c.JSON(200, []interface{}{})
		return
	}
	data := methods.LowerJSONTransform(mytable)
	c.JSON(http.StatusOK, data)
}

// 举证照片上传
func (uc *UserController) PicUpload(c *gin.Context) {
	TBID := c.PostForm("TBID")

	x := c.PostForm("X")
	y := c.PostForm("Y")
	angel := c.PostForm("Angel")

	picbsm := uuid.New().String()
	file, err := c.FormFile("file")
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	if err != nil {
		c.String(400, "Bad request")
		return
	}
	path, _ := filepath.Abs("./PIC/" + TBID + "/" + picbsm + ".jpg")
	url := config.MainOutRouter + "/Survey/PIC/" + TBID + "/" + picbsm + ".jpg"
	err = c.SaveUploadedFile(file, path)
	if err != nil {
		c.String(500, "Internal server error")
		return
	}
	//图斑查询
	var table []models.TempGeo
	DB := models.DB
	DB.Where("tb_id = ?", TBID).Find(&table)
	if len(table) != 0 {
		table[0].ZT = "1"
		DB.Updates(&table[0])
		pic := models.GeoPic{X: x, Y: y, Angel: angel, TBID: TBID, Date: currentTime, Pic_bsm: picbsm, Url: url, BSM: table[0].BSM}
		DB.Save(&pic)
		c.String(200, "ok")
	} else {
		//图层查询
		var mytable models.TempLayer
		DB.Where("tb_id = ? ", TBID).Find(&mytable)
		mytable.ZT = "1"
		DB.Updates(&mytable)
		pic := models.GeoPic{X: x, Y: y, Angel: angel, TBID: TBID, Date: currentTime, Pic_bsm: picbsm, Url: url, BSM: mytable.BSM}
		DB.Save(&pic)
		c.String(200, "ok")
	}
}

// 保存宗地图接口
func (uc *UserController) ZDTUpload(c *gin.Context) {
	TBID := c.PostForm("TBID")
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	file, err := c.FormFile("file")

	if err != nil {
		c.String(400, "Bad request")
		return
	}
	path, _ := filepath.Abs("./ZDT/" + TBID + ".jpg")
	url := config.MainOutRouter + "/Survey/ZDT/" + TBID + ".jpg"
	err = c.SaveUploadedFile(file, path)
	if err != nil {
		c.String(500, "Internal server error")
		return
	}
	//图斑查询

	DB := models.DB

	var table []models.TempGeo
	DB.Where("tb_id = ?", TBID).Find(&table)
	if len(table) != 0 {
		pic := models.ZDTPic{TBID: TBID, Date: currentTime, Url: url, BSM: table[0].BSM}
		DB.Save(&pic)
		c.String(200, "ok")
	} else {
		//图层查询
		var mytable models.TempLayer
		DB.Where("tb_id = ? ", TBID).Find(&mytable)
		pic := models.ZDTPic{TBID: TBID, Date: currentTime, Url: url, BSM: mytable.BSM}
		DB.Save(&pic)
		c.String(200, "ok")
	}

}

// 举证照片删除
func (uc *UserController) PicDel(c *gin.Context) {
	pic_bsm := c.Query("pic_bsm")
	var mytable []models.GeoPic
	DB := models.DB
	DB.Where("pic_bsm  =  ?", pic_bsm).Find(&mytable)
	if len(mytable) == 0 {
		c.JSON(http.StatusNotFound, "Record not found")
		return
	}
	var picBs []string
	for _, record := range mytable {
		picBs = append(picBs, record.Pic_bsm)
		outDir := filepath.Join("PIC", record.TBID, record.Pic_bsm+".jpg")
		os.Remove(outDir)
	}
	DB.Where("pic_bsm IN (?)", picBs).Delete(&models.GeoPic{})

	c.JSON(http.StatusOK, "OK")

}

// 举证信息上传
func (uc *UserController) MsgUpload(c *gin.Context) {
	var TempLayerAttribute map[string]interface{}
	if err := c.BindJSON(&TempLayerAttribute); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	DB := models.DB
	var table []models.TempLayerAttribute
	TBID := TempLayerAttribute["TBID"].(string)
	DB.Where("tb_id = ?", TBID).Find(&table)
	var Data models.TempLayerAttribute
	if len(table) != 0 {
		Data.ID = table[0].ID
		Data.BZ = TempLayerAttribute["BZ"].(string)
		Data.D = TempLayerAttribute["D"].(string)
		Data.N = TempLayerAttribute["N"].(string)
		Data.X = TempLayerAttribute["X"].(string)
		Data.B = TempLayerAttribute["B"].(string)
		Data.QKSM = TempLayerAttribute["QKSM"].(string)
		DCRimgData, err := base64.StdEncoding.DecodeString(TempLayerAttribute["DCR"].(string))
		if err != nil {
			log.Println("解码失败: %v", err)
		}
		Data.DCR = DCRimgData
		ZJRimgData, err := base64.StdEncoding.DecodeString(TempLayerAttribute["ZJR"].(string))
		if err != nil {
			log.Println("解码失败: %v", err)
		}
		Data.ZJR = ZJRimgData
		DB.Updates(&Data)
		c.JSON(200, TempLayerAttribute)
	}
}

type SurveyData struct {
	MainData models.TempLayerAttribute
	PicList  []models.GeoPic
	ZDTUrl   string
}

// 图斑信息回显
func (uc *UserController) SurveyDataGet(c *gin.Context) {
	TBID := c.Query("TBID")
	DB := models.DB
	var TempLayerAttribute models.TempLayerAttribute
	DB.Where("tb_id = ? ", TBID).Find(&TempLayerAttribute)

	var PicList []models.GeoPic
	DB.Where("tb_id = ? ", TBID).Find(&PicList)
	var ZDT models.ZDTPic
	DB.Where("tb_id = ? ", TBID).Find(&ZDT)
	var data = SurveyData{
		MainData: TempLayerAttribute,
		PicList:  PicList,
		ZDTUrl:   ZDT.Url,
	}

	c.JSON(200, data)
}

func (uc *UserController) InTempGeo(c *gin.Context) {
	var jsonData geojson.FeatureCollection
	c.BindJSON(&jsonData) //将前端geojson转换为geo对象
	TBID := uuid.New().String()
	DB := models.DB

	jsonData.Features[0].ID = TBID
	jsonData.Features[0].Properties["tb_id"] = TBID
	var mac string
	if mac2, ok := jsonData.Features[0].Properties["mac"].(string); ok {
		mac = mac2
	}
	bsm := jsonData.Features[0].Properties["bsm"].(string)
	date := jsonData.Features[0].Properties["date"].(string)
	name := jsonData.Features[0].Properties["name"].(string)
	geoJSONData, err := json.MarshalIndent(jsonData, "", "  ")
	if err == nil {
		result := models.TempGeo{TBID: TBID, Geojson: geoJSONData, MAC: mac, BSM: bsm, Date: date, Name: name, ZT: "0"}
		DB.Create(&result)
	}
	//创建举证信息表格数据
	att := models.TempLayerAttribute{TBID: TBID, Layername: name}
	DB.Create(&att)
	c.JSON(http.StatusOK, jsonData)
}

func (uc *UserController) ShowTempGeo(c *gin.Context) {
	bsm := c.Query("bsm")
	var mytable []models.TempGeo
	DB := models.DB
	DB.Where("bsm = ?", bsm).Find(&mytable)
	var data []*geojson.Feature
	for _, item := range mytable {
		var featureCollection struct {
			Features []*geojson.Feature `json:"features"`
		}
		json.Unmarshal(item.Geojson, &featureCollection)
		featureCollection.Features[0].Properties["zt"] = item.ZT
		featureCollection.Features[0].Properties["TBID"] = item.TBID
		data = append(data, featureCollection.Features...)
	}
	features := geojson.NewFeatureCollection()
	features.Features = data
	c.JSON(http.StatusOK, features)
}

// 删除临时数据
func (uc *UserController) DelTempGeo(c *gin.Context) {
	bsm := c.Query("bsm")
	var mytable []models.TempGeo
	DB := models.DB
	DB.Where("bsm  =  ?", bsm).Find(&mytable)
	if len(mytable) == 0 {
		c.JSON(http.StatusNotFound, "Record  not  found")
		return
	}
	for _, record := range mytable {
		DB.Where("tb_id  =  ?", record.TBID).Delete(&models.TempLayerAttribute{})
		DB.Where("tb_id  =  ?", record.TBID).Delete(&models.GeoPic{})
		DB.Where("tb_id  =  ?", record.TBID).Delete(&models.ZDTPic{})
		DB.Delete(&record)
		outDir := filepath.Join("PIC", record.TBID)
		outDir2 := filepath.Join("ZDT", record.TBID+".jpg")
		if _, err := os.Stat(outDir); err == nil {
			os.RemoveAll(outDir)
			os.Remove(outDir2)
		}
	}
	c.JSON(http.StatusOK, "OK")
}

func GetTempGeoList(jsonData map[string]interface{}) geojson.FeatureCollection {
	DB := models.DB
	//查询用户模板权限
	query := DB.Model(&models.TempGeo{})
	mytable := methods.TempDataSearch(query, jsonData)

	var data []*geojson.Feature
	for _, item := range mytable {
		var featureCollection struct {
			Features []*geojson.Feature `json:"features"`
		}
		json.Unmarshal(item.Geojson, &featureCollection)
		featureCollection.Features[0].Properties["zt"] = item.ZT
		featureCollection.Features[0].Properties["TBID"] = item.TBID
		data = append(data, featureCollection.Features...)
	}
	features := geojson.NewFeatureCollection()
	features.Features = data
	return *features
}

func (uc *UserController) ShowTempGeoList(c *gin.Context) {
	var jsonData map[string]interface{}
	c.BindJSON(&jsonData)
	data := GetTempGeoList(jsonData)
	c.JSON(http.StatusOK, data)
}

// 删除临时数据
func (uc *UserController) DelTempLayer(c *gin.Context) {
	bsm := c.Query("bsm") // 从查询中获取bsm
	DB := models.DB       // 获取数据库连接
	var maindata []models.TempLayer
	DB.Where("bsm = ?", bsm).Find(&maindata)
	var tempLayerAttributeIDs []string
	for _, item := range maindata {
		tempLayerAttributeIDs = append(tempLayerAttributeIDs, item.TBID)
		outDir := filepath.Join("PIC", item.TBID)
		outDir2 := filepath.Join("ZDT", item.TBID+".jpg")
		if _, err := os.Stat(outDir); err == nil {
			os.RemoveAll(outDir)
			os.Remove(outDir2)
		}
	}
	// 检查TempLayer里是否有记录存在
	var count int64
	DB.Model(&models.TempLayer{}).Where("bsm = ?", bsm).Count(&count)
	// 直接删除匹配的TempLayHeader记录
	DB.Where("bsm = ?", bsm).Delete(&models.TempLayHeader{})
	// 直接删除匹配的TempLayer记录
	DB.Where("bsm = ?", bsm).Delete(&models.TempLayer{})

	DB.Where("tb_id IN (?)", tempLayerAttributeIDs).Delete(&models.TempLayerAttribute{})
	DB.Where("tb_id IN (?)", tempLayerAttributeIDs).Delete(&models.GeoPic{})
	DB.Where("tb_id IN (?)", tempLayerAttributeIDs).Delete(&models.ZDTPic{})
	c.JSON(http.StatusOK, "OK") // 所有删除操作成功后，返回200
}

// 绘制图形下载
func copyFile(sourceFile string, destinationFolder string) error {
	// 检查并创建目标文件夹
	err := os.MkdirAll(destinationFolder, os.ModePerm)
	if err != nil {
		return fmt.Errorf("创建文件夹时出错: %w", err)
	}

	// 打开源文件
	src, err := os.Open(sourceFile)
	if err != nil {
		return err
	}
	defer src.Close()

	// 创建目标文件
	destinationFile := filepath.Join(destinationFolder, filepath.Base(sourceFile))
	dest, err := os.Create(destinationFile)
	if err != nil {
		return err
	}
	defer dest.Close()

	// 从源文件复制内容到目标文件
	_, err = io.Copy(dest, src)
	if err != nil {
		return err
	}

	return nil
}

// 数据导出
func QSBG(bsm string, outDir string, mygeojson *geojson.FeatureCollection) {
	//制作报告
	err := os.MkdirAll(outDir, os.ModePerm)
	if err != nil {
		fmt.Errorf("创建文件夹时出错: %w", err)
	}
	DB := models.DB
	doc, _ := document.Open("./word/权属说明.docx")
	defer doc.Close()
	//完善表1
	//查询截图
	var ZD *models.ZDTPic
	DB.Where("tb_id = ? ", bsm).Find(&ZD)
	var ZDPIC *string
	if ZD != nil {
		bb := "./ZDT/" + bsm + ".jpg"
		ZDPIC = &bb
	}
	//查询其他信息
	var MSG *models.TempLayerAttribute
	DB.Where("tb_id = ? ", bsm).Find(&MSG)

	if MSG != nil {
		WordGenerator.ZDTable(doc, MSG, ZDPIC)
	}
	//制作界址点成果表
	WordGenerator.BoundaryPointsTable(doc, mygeojson)

	//查询照片
	var pics []models.GeoPic
	DB.Where("tb_id = ? ", bsm).Find(&pics)
	if len(pics) >= 1 {
		WordGenerator.PICTable(doc, pics)
		for _, item := range pics {
			copyFile("./PIC/"+item.TBID+"/"+item.Pic_bsm+".jpg", outDir+"/现状照片")
		}
	}

	//输出word
	doc.SaveToFile(outDir + "/权属调查报告.docx")
}
func (uc *UserController) DownloadTempGeo(c *gin.Context) {

	var jsonData map[string]interface{}
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		// 处理JSON数据绑定错误
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	bsm := jsonData["TBID"].(string)
	base64String_shp := jsonData["shp"].(string)

	var mytable []models.TempGeo
	var mytable2 []models.TempLayer
	DB := models.DB
	DB.Where("tb_id = ?", bsm).Find(&mytable)
	var data []*geojson.Feature
	if len(mytable) >= 1 {
		for _, item := range mytable {
			var featureCollection struct {
				Features []*geojson.Feature `json:"features"`
			}
			json.Unmarshal(item.Geojson, &featureCollection)
			featureCollection.Features[0].Properties["zt"] = item.ZT
			featureCollection.Features[0].Properties["TBID"] = item.TBID
			data = append(data, featureCollection.Features...)
		}
	} else {
		DB.Where("tb_id = ? ", bsm).Find(&mytable2)
		for _, item := range mytable2 {
			var featureCollection struct {
				Features []*geojson.Feature `json:"features"`
			}

			error := json.Unmarshal(item.Geojson, &featureCollection)
			if error == nil {
				featureCollection.Features[0].ID = item.ID
				featureCollection.Features[0].Properties["zt"] = item.ZT
				featureCollection.Features[0].Properties["TBID"] = item.TBID
				data = append(data, featureCollection.Features...)
			} else {
				str, _ := hex.DecodeString(string(item.Geojson))
				finalDecodedString := string(str)
				json.Unmarshal([]byte(finalDecodedString), &featureCollection)
				featureCollection.Features[0].ID = item.ID
				featureCollection.Features[0].Properties["zt"] = item.ZT
				featureCollection.Features[0].Properties["TBID"] = item.TBID
				data = append(data, featureCollection.Features...)
			}
		}
		//图层查询

	}
	mygeojson := geojson.NewFeatureCollection()
	mygeojson.Features = data
	featureCollection := geojson.NewFeatureCollection()
	jsonBytes, _ := json.Marshal(mygeojson)
	json.Unmarshal(jsonBytes, &featureCollection)

	//  将Base64字符串解码成原始二进制数据
	decodedBytes, _ := base64.StdEncoding.DecodeString(base64String_shp)

	//  定义你想要保存的文件的路径和名字
	os.Mkdir(filepath.Join("OutFile", bsm), os.ModePerm)
	outDir := filepath.Join("OutFile", bsm)
	filePath_shp := filepath.Join(outDir, mytable[0].Name+"shp矢量.zip") //  例如  "output.zip"
	filePath_dxf := filepath.Join(outDir, mytable[0].Name+".dxf")      //  例如  "output.zip"
	//  将解码后的数据写入到文件
	absolutePath_shp, _ := filepath.Abs(filePath_shp)
	err := os.WriteFile(absolutePath_shp, decodedBytes, 0666)
	if err != nil {
		c.String(http.StatusOK, "err")
		return
	}
	methods.ConvertGeoJSONToDXF(*featureCollection, filePath_dxf)
	//制作报告
	QSBG(bsm, outDir, mygeojson)
	//压缩全部文件
	absolutePath2, _ := filepath.Abs(outDir)
	methods.ZipFolder(absolutePath2, mytable[0].Name)
	copyFile("./OutFile/"+bsm+"/"+mytable[0].Name+".zip", config.Download)

	c.String(http.StatusOK, "/OutFile/"+bsm+"/"+mytable[0].Name+".zip")

}

// 数据批量导出
func (uc *UserController) DownloadTempGeoALL(c *gin.Context) {
	var jsonData map[string]interface{}
	c.BindJSON(&jsonData)
	data := GetTempGeoList(jsonData)
	//  定义你想要保存的文件的路径和名字
	bsm := uuid.New().String()
	os.Mkdir(filepath.Join("OutFile", bsm), os.ModePerm)
	ctime := time.Now().Format("2006-01-02")
	outDir := filepath.Join("OutFile", bsm)
	Transformer.ConvertGeoJSONToSHP(&data, filepath.Join(outDir, "矢量.shp"))
	//照片打包
	DB := models.DB
	query := DB.Model(&models.TempGeo{})
	mytable := methods.TempDataSearch(query, jsonData)
	for _, items := range mytable {
		var pics []models.GeoPic
		DB.Where("tb_id = ? ", items.TBID).Find(&pics)
		if len(pics) >= 1 {
			Transformer.ConvertPointToArrow(pics, filepath.Join(outDir, "方位角.shp"))
			for _, item := range pics {
				copyFile("./PIC/"+item.TBID+"/"+item.Pic_bsm+".jpg", outDir+"/现状照片/"+items.Name)
			}
		}

	}

	absolutePath2, _ := filepath.Abs(outDir)
	methods.ZipFolder(absolutePath2, ctime+"调查成果")
	copyFile("./OutFile/"+bsm+"/"+ctime+"调查成果"+".zip", config.Download)

	c.String(http.StatusOK, "/OutFile/"+bsm+"/"+ctime+"调查成果"+".zip")
}

// 图层数据导出
func (uc *UserController) DownloadTempLayer(c *gin.Context) {
	tbbsm := c.Query("bsm")
	DB := models.DB
	var mytable []models.TempLayer
	DB.Where("bsm = ?", tbbsm).Find(&mytable)
	var data []*geojson.Feature
	for index, item := range mytable {
		var featureCollection struct {
			Features []*geojson.Feature `json:"features"`
		}

		error := json.Unmarshal(item.Geojson, &featureCollection)
		if error == nil {
			if _, exists := featureCollection.Features[0].Properties["name"]; exists {
			} else {
				featureCollection.Features[0].Properties["name"] = strconv.Itoa(index)
			}
			featureCollection.Features[0].ID = item.TBID
			featureCollection.Features[0].Properties["zt"] = item.ZT
			featureCollection.Features[0].Properties["TBID"] = item.TBID
			data = append(data, featureCollection.Features...)

		}

	}
	features := geojson.NewFeatureCollection()
	features.Features = data
	//  定义你想要保存的文件的路径和名字
	bsm := uuid.New().String()
	os.Mkdir(filepath.Join("OutFile", bsm), os.ModePerm)
	ctime := time.Now().Format("2006-01-02")
	outDir := filepath.Join("OutFile", bsm)
	Transformer.ConvertGeoJSONToSHP(features, filepath.Join(outDir, "矢量.shp"))

	for _, items := range mytable {
		feature := geojson.NewFeatureCollection()
		var data2 []*geojson.Feature
		var featureCollection struct {
			Features []*geojson.Feature `json:"features"`
		}
		json.Unmarshal(items.Geojson, &featureCollection)
		featureCollection.Features[0].Properties["TBID"] = items.TBID
		data2 = append(data2, featureCollection.Features...)
		feature.Features = data2
		QSBG(items.TBID, outDir+"/调查报告/"+items.Name, feature)

		var pics []models.GeoPic
		DB.Where("tb_id = ? ", items.TBID).Find(&pics)
		if len(pics) >= 1 {
			Transformer.ConvertPointToArrow(pics, filepath.Join(outDir, "方位角.shp"))
			for _, item := range pics {
				copyFile("./PIC/"+item.TBID+"/"+item.Pic_bsm+".jpg", outDir+"/现状照片/"+items.Name)
			}
		}

	}

	absolutePath2, _ := filepath.Abs(outDir)
	methods.ZipFolder(absolutePath2, ctime+"调查成果")
	copyFile("./OutFile/"+bsm+"/"+ctime+"调查成果"+".zip", config.Download)

	c.String(http.StatusOK, "/OutFile/"+bsm+"/"+ctime+"调查成果"+".zip")
}

type ZDList struct {
	Line geojson.FeatureCollection `json:"Line"`
	TBID string                    `json:"TBID"`
}
type TempWay struct {
	TempGeo   []models.TempGeo
	TempLayer []models.TempLayer
}

// 图斑分割
func (uc *UserController) SplitGeo(c *gin.Context) {
	var jsonData ZDList
	c.BindJSON(&jsonData)
	var pic []models.GeoPic
	DB := models.DB
	var Way TempWay
	DB.Where("tb_id = ?", jsonData.TBID).Find(&Way.TempGeo)
	if len(Way.TempGeo) == 0 {
		DB.Where("tb_id = ?", jsonData.TBID).Find(&Way.TempLayer)
	}
	DB.Where("tb_id = ?", jsonData.TBID).Find(&pic)
	var att models.TempLayerAttribute
	DB.Where("tb_id = ?", jsonData.TBID).Find(&att)
	//线切割面
	line := Transformer.GetGeometryString(jsonData.Line.Features[0])
	p := GetSingleGeo(jsonData.TBID)
	polygon := Transformer.GetGeometryString(p.Features[0])
	sql := fmt.Sprintf(`
WITH geom AS ( SELECT ST_GeomFromGeoJSON('%s') AS line, 
ST_GeomFromGeoJSON('%s' ) AS polygon) 
SELECT ST_AsGeoJSON(ST_Split(geom.polygon,geom.line)) AS geojson FROM geom`, line, polygon)
	var geomData Transformer.GeometryData
	err := DB.Raw(sql).Scan(&geomData)
	if err.Error != nil {
		c.String(http.StatusBadRequest, "err")
		return
	}
	t := p.Features[0].Type
	Properties := p.Features[0].Properties
	geo := PGBytesToGeojson(geomData)
	var NewGeo geojson.FeatureCollection
	for index, item := range geo.Geometry {
		var feature struct {
			Geometry   map[string]interface{} `json:"geometry"`
			Properties map[string]interface{} `json:"properties"`
			Type       string                 `json:"type"`
		}
		feature.Properties = Properties
		feature.Type = t
		feature.Geometry = item
		data2, _ := json.Marshal(feature)
		var myfeature *geojson.Feature
		json.Unmarshal(data2, &myfeature)
		//判断是图层还是调查

		TBID := uuid.New().String()
		myfeature.ID = TBID
		myfeature.Properties["TBID"] = TBID

		//照片ID转移
		for pi, _ := range pic {
			pic[pi].TBID = TBID
		}
		DB.Save(pic)
		if len(Way.TempGeo) >= 1 {
			myfeature.Properties["bsm"] = TBID
			myfeature.Properties["name"] = Way.TempGeo[0].Name + strconv.Itoa(index)
			newgeo := geojson.NewFeatureCollection()

			newgeo.Features = append(newgeo.Features, myfeature)
			geoJSONData, err := json.MarshalIndent(newgeo, "", "  ")
			if err == nil {
				result := models.TempGeo{TBID: TBID, Geojson: geoJSONData, MAC: Way.TempGeo[0].MAC, BSM: TBID, Date: time.Now().Format("2006-01-02 15:04:05"), Name: Way.TempGeo[0].Name + strconv.Itoa(index), ZT: Way.TempGeo[0].ZT}
				result_att := models.TempLayerAttribute{TBID: TBID, QKSM: att.QKSM, B: att.B, D: att.D, N: att.N, X: att.X, BZ: att.BZ, ZJR: att.ZJR, DCR: att.ZJR}
				DB.Create(&result)
				DB.Create(&result_att)
			}
		} else {
			Layername := Way.TempLayer[0].Layername
			bsm := Way.TempLayer[0].BSM
			MAC := Way.TempLayer[0].MAC
			myfeature.Properties["name"] = Way.TempLayer[0].Name + strconv.Itoa(index)
			newgeo := geojson.NewFeatureCollection()
			newgeo.Features = append(newgeo.Features, myfeature)
			geoJSONData, err := json.MarshalIndent(newgeo, "", "  ")
			if err == nil {
				result := models.TempLayer{Layername: Layername, MAC: MAC, BSM: bsm, TBID: TBID, Geojson: geoJSONData, ZT: Way.TempLayer[0].ZT, Name: Way.TempLayer[0].Name}
				result_att := models.TempLayerAttribute{TBID: TBID, Layername: Layername, QKSM: att.QKSM, B: att.B, D: att.D, N: att.N, X: att.X, BZ: att.BZ, ZJR: att.ZJR, DCR: att.ZJR}
				DB.Create(&result_att)
				DB.Create(&result)
			}
		}
		NewGeo.Features = append(NewGeo.Features, myfeature)
	}
	DB.Delete(&att)
	//照片处理

	//删除原图形
	if len(Way.TempGeo) >= 1 {
		DB.Delete(&Way.TempGeo)
	} else {
		DB.Delete(&Way.TempLayer)
	}
	c.JSON(http.StatusOK, NewGeo)
}

type Geometries struct {
	Geometry []map[string]interface{} `json:"geometries"`
}

// 查询的geojson转换
func PGBytesToGeojson(geomData Transformer.GeometryData) Geometries {
	var Geometry Geometries
	json.Unmarshal(geomData.GeoJSON, &Geometry)
	return Geometry
}

// 图斑合并
type DissolverType struct {
	ZD       []string `json:"ZD"`
	MainTBID string   `json:"MainTBID"`
}

func (uc *UserController) DissolverGeo(c *gin.Context) {
	var jsonData DissolverType
	c.BindJSON(&jsonData)
	db := models.DB
	var Properties map[string]interface{}
	var NewGeo geojson.FeatureCollection
	var t string
	var geolist []string
	var zds []*geojson.FeatureCollection
	for _, tt := range jsonData.ZD {
		zds = append(zds, GetSingleGeo(tt))
	}
	var DelID []string
	for _, features := range zds {
		geo := Transformer.GetGeometryString(features.Features[0])
		t = features.Features[0].Type
		if features.Features[0].Properties["TBID"] == jsonData.MainTBID {
			Properties = features.Features[0].Properties
		} else {
			DelID = append(DelID, features.Features[0].Properties["TBID"].(string))

		}
		geolist = append(geolist, geo)
	}
	var sqlgeo string
	var geom []string
	var geom1 []string
	for index, item := range geolist {
		sql1 := fmt.Sprintf("%s AS ( SELECT ST_GeomFromGeoJSON('%s') AS geom),", "geo"+strconv.Itoa(index), item)
		sqlgeo = sqlgeo + sql1
		geom = append(geom, "geo"+strconv.Itoa(index)+".geom")
		geom1 = append(geom1, "geo"+strconv.Itoa(index))
	}
	result1 := strings.Join(geom, ",")
	result2 := strings.Join(geom1, ",")
	sql := fmt.Sprintf("WITH %s is_adjacent AS ( SELECT ST_Intersects(%s) AS intersects , ST_Touches(%s) AS touches FROM %s) SELECT  CASE  WHEN intersects OR touches THEN ST_AsGeoJSON(ST_Union(%s)) ELSE ST_AsGeoJSON(ST_GeomFromGeoJSON('{\"type\": \"GeometryCollection\", \"geometries\": []}')) END AS geojson FROM %s,is_adjacent;", sqlgeo, result1, result1, result2, result1, result2)
	var geomData Transformer.GeometryData
	err := db.Raw(sql).Scan(&geomData)
	if err.Error != nil {
		c.String(http.StatusBadRequest, "err")
		return
	}

	var feature struct {
		Geometry   map[string]interface{} `json:"geometry"`
		Properties map[string]interface{} `json:"properties"`
		Type       string                 `json:"type"`
	}
	feature.Properties = Properties
	feature.Type = t
	json.Unmarshal(geomData.GeoJSON, &feature.Geometry)

	if feature.Geometry["geometries"] != nil {
		if len(feature.Geometry["geometries"].([]interface{})) == 0 {
			c.String(http.StatusBadRequest, "err")
			return
		}
	}

	data2, _ := json.Marshal(feature)

	var myfeature *geojson.Feature
	json.Unmarshal(data2, &myfeature)
	myfeature.ID = jsonData.MainTBID
	NewGeo.Features = append(NewGeo.Features, myfeature)

	geoJSONData, _ := json.MarshalIndent(NewGeo, "", "  ")
	var templayer []models.TempLayer
	db.Where("tb_id = ?", jsonData.MainTBID).Find(&templayer)
	if len(templayer) >= 1 {
		templayer[0].Geojson = geoJSONData
		db.Save(&templayer)
	} else {
		var templayer2 models.TempGeo
		db.Where("tb_id = ?", jsonData.MainTBID).Find(&templayer2)
		templayer2.Geojson = geoJSONData
		db.Save(&templayer2)
	}
	db.Where("tb_id IN (?)", DelID).Delete(&models.TempLayer{})
	db.Where("tb_id IN (?)", DelID).Delete(&models.TempGeo{})
	db.Where("tb_id IN (?)", DelID).Delete(&models.TempLayerAttribute{})
	c.JSON(http.StatusOK, NewGeo)
}
