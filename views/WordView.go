package views

import (
	"encoding/json"
	"gitee.com/gooffice/gooffice/document"
	"github.com/GrainArc/SouceMap/WordGenerator"
	"github.com/GrainArc/SouceMap/config"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/paulmach/orb/geojson"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
)

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
	path := filepath.Join("OutFile/" + taskid)
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
