package config

import (
	"encoding/xml"
	"fmt"
	"os"
)

// 10.0.4.10:8426 124.220.233.230:8426
var MainOutRouter string
var MainRouter string
var DSN string
var Raster string
var Dem string
var Tiles3d string
var Dbname string
var Download string
var Loader string
var UpdateIP string
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
	Loader        string   `xml:"loader"`
	Raster        string   `xml:"raster"`
	Dem           string   `xml:"dem"`
	RootPath      string   `xml:"RootPath"`
	Tiles3d       string   `xml:"tiles3d"`
	DeviceName    string   `xml:"DeviceName"`
	Download      string   `xml:"download"`
	UpdateIP      string   `xml:"updateip"`
}

func init() {

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
	UpdateIP = MainConfig.UpdateIP
	Raster = MainConfig.Raster
	Dem = MainConfig.Dem
	Dbname = MainConfig.Dbname
	Tiles3d = MainConfig.Tiles3d
	Loader = MainConfig.Loader
	Download = MainConfig.Download
	DeviceName = MainConfig.DeviceName

	DSN = fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable TimeZone=UTC", MainConfig.Host, MainConfig.Username, MainConfig.Password, MainConfig.Dbname, MainConfig.Port)

}
