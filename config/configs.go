package config

import (
	"encoding/xml"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// 10.0.4.10:8426 124.220.233.230:8426
var MainOutRouter string
var MainRouter string
var DSN string
var Raster string
var Dem string
var Tiles3d string

var Download string
var Loader string

var DeviceName string
var MainConfig Config

type Config struct {
	XMLName       xml.Name `xml:"config"`
	MainRouter    string   `xml:"MainRouter"`
	MainOutRouter string   `xml:"MainOutRouter"`
	Dbname        string   `xml:"dbname"`
	Host          string   `xml:"host"`
	Port          string   `xml:"port"`
	Username      string   `xml:"user"`
	Password      string   `xml:"password"`
	Texture       string   `xml:"texture"`
	Raster        string   `xml:"raster"`
	Dem           string   `xml:"dem"`
	RootPath      string   `xml:"RootPath"`
	Tiles3d       string   `xml:"tiles3d"`
	DeviceName    string   `xml:"DeviceName"`
	Download      string   `xml:"download"`
}

func InitConfig() {
	configDir, err := os.UserConfigDir()
	if err != nil {
		log.Fatal("无法获取用户配置目录:", err)
	}

	appDir := filepath.Join(configDir, "BoundlessMap")
	// 创建应用配置目录（如果不存在）
	os.MkdirAll(appDir, 0755)
	appConfig := filepath.Join(appDir, "config.xml")

	xmlFile, err := os.Open(appConfig)
	if err != nil {
		fmt.Println("Error  opening  file:", err)
		return
	}
	defer xmlFile.Close()

	xmlDecoder := xml.NewDecoder(xmlFile)
	err = xmlDecoder.Decode(&MainConfig)
	if err != nil {
		fmt.Println("Error  decoding  XML:", err)
		return
	}
	MainOutRouter = MainConfig.MainOutRouter
	MainRouter = MainConfig.MainRouter
	Raster = MainConfig.Raster
	Dem = MainConfig.Dem
	Tiles3d = MainConfig.Tiles3d
	Download = MainConfig.Download
	DeviceName = MainConfig.DeviceName
	MainConfig.Host = MainConfig.Host
	MainConfig.Username = MainConfig.Username
	MainConfig.Password = MainConfig.Password
	MainConfig.Dbname = MainConfig.Dbname
	MainConfig.Port = MainConfig.Port
	DSN = fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable TimeZone=UTC", MainConfig.Host, MainConfig.Username, MainConfig.Password, MainConfig.Dbname, MainConfig.Port)

}

func InitConfigLocal() {

	xmlFile, err := os.Open("config.xml")
	if err != nil {
		fmt.Println("Error  opening  file:", err)
		return
	}
	defer xmlFile.Close()

	xmlDecoder := xml.NewDecoder(xmlFile)
	err = xmlDecoder.Decode(&MainConfig)
	if err != nil {
		fmt.Println("Error  decoding  XML:", err)
		return
	}
	MainOutRouter = MainConfig.MainOutRouter
	MainRouter = MainConfig.MainRouter
	Raster = MainConfig.Raster
	Dem = MainConfig.Dem
	Tiles3d = MainConfig.Tiles3d
	Download = MainConfig.Download
	DeviceName = MainConfig.DeviceName
	MainConfig.Host = MainConfig.Host
	MainConfig.Username = MainConfig.Username
	MainConfig.Password = MainConfig.Password
	MainConfig.Dbname = MainConfig.Dbname
	MainConfig.Port = MainConfig.Port
	DSN = fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable TimeZone=UTC", MainConfig.Host, MainConfig.Username, MainConfig.Password, MainConfig.Dbname, MainConfig.Port)

}
