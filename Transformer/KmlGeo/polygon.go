package KmlGeo

import "encoding/xml"

type Polygon struct {
	XMLName         xml.Name          `xml:"Polygon"`
	OuterBoundaryIs outerBoundaryIs   `xml:"outerBoundaryIs"`
	InnerBoundaryIs []innerBoundaryIs `xml:"innerBoundaryIs"`
}
type outerBoundaryIs struct {
	LinearRing LinearRing `xml:"LinearRing"`
}
type innerBoundaryIs struct {
	LinearRing LinearRing `xml:"LinearRing"`
}
type LinearRing struct {
	Coordinates string `xml:"coordinates"`
}
