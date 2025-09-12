package pgmvt

import (
	"github.com/paulmach/orb"
)

func Bounds(geo orb.Geometry) []tile {
	aa := geo.Bound()
	xmin := aa.Min[0]
	ymin := aa.Min[1]
	xmax := aa.Max[0]
	ymax := aa.Max[1]
	xmin, ymin = epsg4326_to_epsg3857(xmin, ymin)
	xmax, ymax = epsg4326_to_epsg3857(xmax, ymax)
	tiles := TileGenerate(xmin, ymin, xmax, ymax)
	return tiles
}

func KeepTile(zoom_level int64, tiles []tile) []tile {
	var newtiles []tile
	for _, item := range tiles {
		if item.Z == zoom_level {
			newtiles = append(newtiles, item)
		}
	}
	return newtiles
}
