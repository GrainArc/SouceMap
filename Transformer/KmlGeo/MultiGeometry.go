package KmlGeo

import "encoding/xml"

type MultiGeometry struct {
	XMLName    xml.Name     `xml:"MultiGeometry"`
	Polygons   []Polygon    `xml:"Polygon"`
	LineString []LineString `xml:"LineString"`
	Point      []Point      `xml:"Point"`
}
