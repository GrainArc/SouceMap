// models/track.go
package models

import (
	"github.com/paulmach/orb/geojson"
)

// 在 TrackResponse 后添加
type SnapPointRequest struct {
	Point []float64 `json:"point"` // 要捕捉的点 [x, y]
}

type SnapPointResponse struct {
	Type         string    `json:"type"`                    // "snap_point"
	SnappedPoint []float64 `json:"snapped_point,omitempty"` // 捕捉到的最近点 [x, y]
	Distance     float64   `json:"distance,omitempty"`      // 距离(米)
	LineID       int       `json:"line_id,omitempty"`       // 所在线段ID
	Message      string    `json:"message,omitempty"`       // 错误信息
}
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
