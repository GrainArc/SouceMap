package pgmvt

import (
	"math"
)

func XyzLonLat(x float64, y float64, z float64) []float64 {
	n := math.Pow(2, z)
	LonDeg := (x/n)*360.0 - 180.0
	LatRad := math.Atan(math.Sinh(math.Pi * (1 - (2*y)/n)))
	LatDeg := (180 * LatRad) / math.Pi
	return []float64{LonDeg, LatDeg}
}
