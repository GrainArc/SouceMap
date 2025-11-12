// models/track.go
package models

import (
	"github.com/paulmach/orb/geojson"
)

type TrackData struct {
	LayerNames []string                  `json:"layer_names"` // 需要追踪的图层
	Box        geojson.FeatureCollection `json:"box"`         // 当前视角范围
	StartPoint []float64                 `json:"start_point"` // 起始点 [x, y]
}

type TrackResponse struct {
	Type    string                     `json:"type"`              // "init", "path" 或 "complete"
	Lines   *geojson.FeatureCollection `json:"lines,omitempty"`   // 打断后的线段（init时返回）
	Path    *geojson.FeatureCollection `json:"path,omitempty"`    // 追踪路径
	Message string                     `json:"message,omitempty"` // 消息
}
