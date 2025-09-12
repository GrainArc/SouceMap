package pgmvt

import "math"

func lonlat2mercator(lon float64, lat float64) (float64, float64) {
	semimajor_axis := 6378137.0
	x := semimajor_axis * (math.Pi / 180) * lon
	y := semimajor_axis * math.Log(math.Tan((math.Pi/4)+((math.Pi/180)*lat/2)))
	return x, y
}

func mercator2lonlat(x float64, y float64) (float64, float64) {
	semimajor_axis := 6378137.0
	lon := (x / semimajor_axis) * (180 / math.Pi)
	lat := (2*math.Atan(math.Exp(y/semimajor_axis)) - math.Pi/2) * (180 / math.Pi)
	return lon, lat
}

func epsg4326_to_epsg3857(lon float64, lat float64) (float64, float64) {
	x, y := lonlat2mercator(lon, lat)
	r_major := 6378137.0
	x = r_major * (math.Pi / 180) * lon
	scale := x / lon
	y = 180.0 / math.Pi * math.Log(math.Tan(math.Pi/4.0+lat*(math.Pi/180.0)/2.0)) * scale
	return x, y
}

func Epsg4326_to_epsg3857(lon float64, lat float64) (float64, float64) {
	x, y := lonlat2mercator(lon, lat)
	r_major := 6378137.0
	x = r_major * (math.Pi / 180) * lon
	scale := x / lon
	y = 180.0 / math.Pi * math.Log(math.Tan(math.Pi/4.0+lat*(math.Pi/180.0)/2.0)) * scale
	return x, y
}

func Epsg3857_to_epsg4326(x float64, y float64) (float64, float64) {
	r_major := 6378137.0
	lon := x / r_major * 180.0 / math.Pi
	lat := math.Atan(math.Exp(y/r_major))*360.0/math.Pi - 90.0
	return lon, lat
}
