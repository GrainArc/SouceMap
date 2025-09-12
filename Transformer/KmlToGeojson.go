package Transformer

import (
	"encoding/xml"
	"github.com/fmecool/SouceMap/Transformer/KmlGeo"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"io"
	"os"
	"strconv"
	"strings"
)

type Document struct {
	Name       string      `xml:"name"`
	Visibility int         `xml:"visibility"`
	Schema     Schema      `xml:"Schema"`
	Folder     Folder      `xml:"Folder"`
	Placemark  []Placemark `xml:"Placemark"`
}
type Schema struct {
	Name        string        `xml:"name,attr"`
	ID          string        `xml:"id,attr"`
	SimpleField []SimpleField `xml:"SimpleField"`
}
type SimpleField struct {
	Type        string `xml:"type,attr"`
	Name        string `xml:"name,attr"`
	DisplayName string `xml:"displayName"`
}
type Folder struct {
	ID        string      `xml:"id,attr"`
	Name      string      `xml:"name"`
	Placemark []Placemark `xml:"Placemark"`
}
type Placemark struct {
	ID            string                `xml:"id,attr"`
	Name          string                `xml:"name"`
	Description   string                `xml:"description"`
	Style         Style                 `xml:"Style"`
	ExtendedData  ExtendedData          `xml:"ExtendedData"`
	LineString    *KmlGeo.LineString    `xml:"LineString"`
	Point         *KmlGeo.Point         `xml:"Point"`
	Polygon       *KmlGeo.Polygon       `xml:"Polygon"`
	MultiGeometry *KmlGeo.MultiGeometry `xml:"MultiGeometry"`
}
type Style struct {
	IconStyle  IconStyle  `xml:"IconStyle"`
	LabelStyle LabelStyle `xml:"LabelStyle"`
	LineStyle  LineStyle  `xml:"LineStyle"`
}
type IconStyle struct {
	Scale float32 `xml:"scale"`
}
type LabelStyle struct {
}
type LineStyle struct {
	Color           string `xml:"color"`
	Width           int    `xml:"width"`
	LabelVisibility int    `xml:"gx labelVisibility"`
}
type ExtendedData struct {
	SchemaData SchemaData `xml:"SchemaData"`
}
type SchemaData struct {
	SimpleData []SimpleData `xml:"SimpleData"`
}
type SimpleData struct {
	Name  string `xml:"name,attr"`
	Value string `xml:",chardata"`
}

type Kml struct {
	XMLName  xml.Name `xml:"kml"`
	Document Document `xml:"Document"`
}

func StringToCoords(Coords string) []orb.Point {
	Coordinates := strings.Split(Coords, " ")
	var coords []orb.Point
	for _, coord := range Coordinates {
		mycoord := strings.Split(coord, ",")
		if len(mycoord) >= 2 {
			x, _ := strconv.ParseFloat(mycoord[0], 64)
			y, _ := strconv.ParseFloat(mycoord[1], 64)
			if x > 0 && y > 0 {
				coords = append(coords, orb.Point{x, y})
			}
		}

	}
	return coords
}

func KmlToGeojson(path string) (*geojson.FeatureCollection, string) {
	// 打开KML文件
	var isTransform string
	xmlFile, _ := os.Open(path)
	defer xmlFile.Close()
	// 读取文件内容
	byteValue, _ := io.ReadAll(xmlFile)
	// 解析KML
	var kml Kml
	xml.Unmarshal(byteValue, &kml)

	// 打印结果来验证
	featureCollection := geojson.NewFeatureCollection()
	for _, item := range kml.Document.Folder.Placemark {
		//属性提取
		attrs := make(map[string]interface{})
		myatts := item.ExtendedData.SchemaData.SimpleData
		for _, f := range myatts {
			attrs[f.Name] = f.Value
		}
		attrs["kml_name"] = item.Name
		//线数据
		if item.LineString != nil {
			coords := StringToCoords(item.LineString.Coordinates)
			for _, coord := range coords {
				x := coord[0]
				if x >= 100000 && x <= 10000000 {
					isTransform = "4544"
				} else if x <= 1000 {
					isTransform = "4326"
				} else if x >= 33000000 && x <= 34000000 {
					isTransform = "4521"
				} else if x >= 34000000 && x <= 35000000 {
					isTransform = "4522"
				} else if x >= 35000000 && x <= 36000000 {
					isTransform = "4523"
				} else if x >= 36000000 && x <= 37000000 {
					isTransform = "4524"
				}
			}
			line := orb.LineString(coords)
			feature := geojson.NewFeature(line)
			feature.Properties = attrs
			featureCollection.Append(feature)
		}
		//点数据解析
		if item.Point != nil {
			Coord := strings.Split(item.Point.Coordinates, ",")
			x, _ := strconv.ParseFloat(Coord[0], 64)
			y, _ := strconv.ParseFloat(Coord[1], 64)

			if x >= 100000 && x <= 10000000 {
				isTransform = "4544"
			} else if x <= 1000 {
				isTransform = "4326"
			} else if x >= 33000000 && x <= 34000000 {
				isTransform = "4521"
			} else if x >= 34000000 && x <= 35000000 {
				isTransform = "4522"
			} else if x >= 35000000 && x <= 36000000 {
				isTransform = "4523"
			} else if x >= 36000000 && x <= 37000000 {
				isTransform = "4524"
			}
			geometry := orb.Point{x, y}
			feature := geojson.NewFeature(geometry)
			feature.Properties = attrs
			featureCollection.Append(feature)
		}
		//面数据解析
		if item.Polygon != nil {
			var rings []orb.Ring
			//获取外环

			OuterBoundaryIs := StringToCoords(item.Polygon.OuterBoundaryIs.LinearRing.Coordinates)
			for _, coord := range OuterBoundaryIs {

				x := coord[0]
				if x >= 100000 && x <= 10000000 {
					isTransform = "4544"
				} else if x <= 1000 {
					isTransform = "4326"
				} else if x >= 33000000 && x <= 34000000 {
					isTransform = "4521"
				} else if x >= 34000000 && x <= 35000000 {
					isTransform = "4522"
				} else if x >= 35000000 && x <= 36000000 {
					isTransform = "4523"
				} else if x >= 36000000 && x <= 37000000 {
					isTransform = "4524"
				}
			}
			rings = append(rings, OuterBoundaryIs)
			//获取内环
			for _, inner := range item.Polygon.InnerBoundaryIs {
				InnerBoundaryIs := StringToCoords(inner.LinearRing.Coordinates)
				rings = append(rings, InnerBoundaryIs)
			}
			//构造几何
			geometry := orb.Polygon(rings)
			feature := geojson.NewFeature(geometry)
			feature.Properties = attrs
			featureCollection.Append(feature)

		}
		//聚合数据解析
		if item.MultiGeometry != nil {
			Points := item.MultiGeometry.Point
			LineStrings := item.MultiGeometry.LineString
			Polygons := item.MultiGeometry.Polygons
			if len(Points) != 0 {
				for _, point := range Points {
					Coord := strings.Split(point.Coordinates, ",")
					x, _ := strconv.ParseFloat(Coord[0], 64)
					y, _ := strconv.ParseFloat(Coord[1], 64)
					if x >= 100000 && x <= 10000000 {
						isTransform = "4544"
					} else if x <= 1000 {
						isTransform = "4326"
					} else if x >= 33000000 && x <= 34000000 {
						isTransform = "4521"
					} else if x >= 34000000 && x <= 35000000 {
						isTransform = "4522"
					} else if x >= 35000000 && x <= 36000000 {
						isTransform = "4523"
					} else if x >= 36000000 && x <= 37000000 {
						isTransform = "4524"
					}
					geometry := orb.Point{x, y}
					feature := geojson.NewFeature(geometry)
					feature.Properties = attrs
					featureCollection.Append(feature)
				}

			}
			if len(LineStrings) != 0 {
				for _, LineString := range LineStrings {
					coords := StringToCoords(LineString.Coordinates)
					for _, coord := range coords {
						x := coord[0]
						if x >= 100000 && x <= 10000000 {
							isTransform = "4544"
						} else if x <= 1000 {
							isTransform = "4326"
						} else if x >= 33000000 && x <= 34000000 {
							isTransform = "4521"
						} else if x >= 34000000 && x <= 35000000 {
							isTransform = "4522"
						} else if x >= 35000000 && x <= 36000000 {
							isTransform = "4523"
						} else if x >= 36000000 && x <= 37000000 {
							isTransform = "4524"
						}
					}
					line := orb.LineString(coords)
					feature := geojson.NewFeature(line)
					feature.Properties = attrs
					featureCollection.Append(feature)
				}

			}
			if len(Polygons) != 0 {
				for _, Polygon := range Polygons {
					var rings []orb.Ring
					//获取外环
					OuterBoundaryIs := StringToCoords(Polygon.OuterBoundaryIs.LinearRing.Coordinates)
					for _, coord := range OuterBoundaryIs {
						x := coord[0]
						if x >= 100000 && x <= 10000000 {
							isTransform = "4544"
						} else if x <= 1000 {
							isTransform = "4326"
						} else if x >= 33000000 && x <= 34000000 {
							isTransform = "4521"
						} else if x >= 34000000 && x <= 35000000 {
							isTransform = "4522"
						} else if x >= 35000000 && x <= 36000000 {
							isTransform = "4523"
						} else if x >= 36000000 && x <= 37000000 {
							isTransform = "4524"
						}
					}
					rings = append(rings, OuterBoundaryIs)
					//获取内环
					for _, inner := range Polygon.InnerBoundaryIs {
						InnerBoundaryIs := StringToCoords(inner.LinearRing.Coordinates)
						rings = append(rings, InnerBoundaryIs)
					}
					//构造几何
					geometry := orb.Polygon(rings)
					feature := geojson.NewFeature(geometry)
					feature.Properties = attrs
					featureCollection.Append(feature)
				}
			}
		}
	}
	for _, item := range kml.Document.Placemark {
		//属性提取
		attrs := make(map[string]interface{})
		myatts := item.ExtendedData.SchemaData.SimpleData
		for _, f := range myatts {
			attrs[f.Name] = f.Value
		}
		attrs["kml_name"] = item.Name
		//线数据
		if item.LineString != nil {
			coords := StringToCoords(item.LineString.Coordinates)
			for _, coord := range coords {
				x := coord[0]
				if x >= 100000 && x <= 10000000 {
					isTransform = "4544"
				} else if x <= 1000 {
					isTransform = "4326"
				} else if x >= 33000000 && x <= 34000000 {
					isTransform = "4521"
				} else if x >= 34000000 && x <= 35000000 {
					isTransform = "4522"
				} else if x >= 35000000 && x <= 36000000 {
					isTransform = "4523"
				} else if x >= 36000000 && x <= 37000000 {
					isTransform = "4524"
				}
			}
			line := orb.LineString(coords)
			feature := geojson.NewFeature(line)
			feature.Properties = attrs
			featureCollection.Append(feature)
		}
		//点数据解析
		if item.Point != nil {
			Coord := strings.Split(item.Point.Coordinates, ",")
			x, _ := strconv.ParseFloat(Coord[0], 64)
			y, _ := strconv.ParseFloat(Coord[1], 64)
			if x >= 100000 && x <= 10000000 {
				isTransform = "4544"
			} else if x <= 1000 {
				isTransform = "4326"
			} else if x >= 33000000 && x <= 34000000 {
				isTransform = "4521"
			} else if x >= 34000000 && x <= 35000000 {
				isTransform = "4522"
			} else if x >= 35000000 && x <= 36000000 {
				isTransform = "4523"
			} else if x >= 36000000 && x <= 37000000 {
				isTransform = "4524"
			}
			geometry := orb.Point{x, y}
			feature := geojson.NewFeature(geometry)
			feature.Properties = attrs
			featureCollection.Append(feature)
		}
		//面数据解析
		if item.Polygon != nil {
			var rings []orb.Ring
			//获取外环

			OuterBoundaryIs := StringToCoords(item.Polygon.OuterBoundaryIs.LinearRing.Coordinates)
			for _, coord := range OuterBoundaryIs {
				x := coord[0]
				if x >= 100000 && x <= 10000000 {
					isTransform = "4544"
				} else if x <= 1000 {
					isTransform = "4326"
				} else if x >= 33000000 && x <= 34000000 {
					isTransform = "4521"
				} else if x >= 34000000 && x <= 35000000 {
					isTransform = "4522"
				} else if x >= 35000000 && x <= 36000000 {
					isTransform = "4523"
				} else if x >= 36000000 && x <= 37000000 {
					isTransform = "4524"
				}
			}
			rings = append(rings, OuterBoundaryIs)
			//获取内环
			for _, inner := range item.Polygon.InnerBoundaryIs {
				InnerBoundaryIs := StringToCoords(inner.LinearRing.Coordinates)
				rings = append(rings, InnerBoundaryIs)
			}
			//构造几何
			geometry := orb.Polygon(rings)
			feature := geojson.NewFeature(geometry)
			feature.Properties = attrs
			featureCollection.Append(feature)

		}
		//聚合数据解析
		if item.MultiGeometry != nil {
			Points := item.MultiGeometry.Point
			LineStrings := item.MultiGeometry.LineString
			Polygons := item.MultiGeometry.Polygons
			if len(Points) != 0 {
				for _, point := range Points {
					Coord := strings.Split(point.Coordinates, ",")
					x, _ := strconv.ParseFloat(Coord[0], 64)
					y, _ := strconv.ParseFloat(Coord[1], 64)
					if x >= 100000 && x <= 10000000 {
						isTransform = "4544"
					} else if x <= 1000 {
						isTransform = "4326"
					} else if x >= 33000000 && x <= 34000000 {
						isTransform = "4521"
					} else if x >= 34000000 && x <= 35000000 {
						isTransform = "4522"
					} else if x >= 35000000 && x <= 36000000 {
						isTransform = "4523"
					} else if x >= 36000000 && x <= 37000000 {
						isTransform = "4524"
					}
					geometry := orb.Point{x, y}
					feature := geojson.NewFeature(geometry)
					feature.Properties = attrs
					featureCollection.Append(feature)
				}

			}
			if len(LineStrings) != 0 {
				for _, LineString := range LineStrings {
					coords := StringToCoords(LineString.Coordinates)
					for _, coord := range coords {
						x := coord[0]
						if x >= 100000 && x <= 10000000 {
							isTransform = "4544"
						} else if x <= 1000 {
							isTransform = "4326"
						} else if x >= 33000000 && x <= 34000000 {
							isTransform = "4521"
						} else if x >= 34000000 && x <= 35000000 {
							isTransform = "4522"
						} else if x >= 35000000 && x <= 36000000 {
							isTransform = "4523"
						} else if x >= 36000000 && x <= 37000000 {
							isTransform = "4524"
						}
					}
					line := orb.LineString(coords)
					feature := geojson.NewFeature(line)
					feature.Properties = attrs
					featureCollection.Append(feature)
				}

			}
			if len(Polygons) != 0 {
				for _, Polygon := range Polygons {
					var rings []orb.Ring
					//获取外环
					OuterBoundaryIs := StringToCoords(Polygon.OuterBoundaryIs.LinearRing.Coordinates)
					for _, coord := range OuterBoundaryIs {
						x := coord[0]
						if x >= 100000 && x <= 10000000 {
							isTransform = "4544"
						} else if x <= 1000 {
							isTransform = "4326"
						} else if x >= 33000000 && x <= 34000000 {
							isTransform = "4521"
						} else if x >= 34000000 && x <= 35000000 {
							isTransform = "4522"
						} else if x >= 35000000 && x <= 36000000 {
							isTransform = "4523"
						} else if x >= 36000000 && x <= 37000000 {
							isTransform = "4524"
						}
					}
					rings = append(rings, OuterBoundaryIs)
					//获取内环
					for _, inner := range Polygon.InnerBoundaryIs {
						InnerBoundaryIs := StringToCoords(inner.LinearRing.Coordinates)
						rings = append(rings, InnerBoundaryIs)
					}
					//构造几何
					geometry := orb.Polygon(rings)
					feature := geojson.NewFeature(geometry)
					feature.Properties = attrs
					featureCollection.Append(feature)
				}
			}
		}
	}
	return featureCollection, isTransform
}
