// services/track_service.go
package services

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"strings"
)

type TrackService struct{}

func NewTrackService() *TrackService {
	return &TrackService{}
}

// GetAndBreakGeometries 获取相交的几何并打断为线段，返回 GeoJSON
func (s *TrackService) GetAndBreakGeometries(
	ctx context.Context,
	layerNames []string,
	bbox geojson.FeatureCollection,
) (*geojson.FeatureCollection, error) {
	// 构建 bbox 的 WKT
	bboxWKT := s.featureCollectionToWKT(bbox)
	if bboxWKT == "" {
		return nil, fmt.Errorf("invalid bbox")
	}

	fc := geojson.NewFeatureCollection()

	for _, layerName := range layerNames {
		// 查询相交的几何并打断为线段，直接返回 GeoJSON
		query := fmt.Sprintf(`
            WITH intersecting_geoms AS (
                -- 获取与 bbox 相交的几何
                SELECT geom 
                FROM %s 
                WHERE ST_Intersects(geom, ST_GeomFromText($1, 4326))
            ),
            dumped AS (
                -- 展开多几何为单几何
                SELECT (ST_Dump(geom)).geom as geom
                FROM intersecting_geoms
            ),
            as_lines AS (
                -- 将所有几何转换为线
                SELECT 
                    CASE 
                        WHEN ST_GeometryType(geom) = 'ST_LineString' THEN geom
                        WHEN ST_GeometryType(geom) = 'ST_Polygon' THEN ST_Boundary(geom)
                        WHEN ST_GeometryType(geom) = 'ST_MultiLineString' THEN geom
                        WHEN ST_GeometryType(geom) = 'ST_MultiPolygon' THEN ST_Boundary(geom)
                        WHEN ST_GeometryType(geom) = 'ST_Point' THEN NULL
                        ELSE geom
                    END as geom
                FROM dumped
            ),
            exploded_lines AS (
                -- 再次展开，确保都是单线
                SELECT (ST_Dump(geom)).geom as geom
                FROM as_lines
                WHERE geom IS NOT NULL
                  AND ST_GeometryType(geom) IN ('ST_LineString', 'ST_MultiLineString')
            ),
            segments AS (
                -- 将每条线打断为单个线段
                SELECT 
                    row_number() OVER () as id,
                    ST_MakeLine(
                        ST_PointN(geom, n),
                        ST_PointN(geom, n + 1)
                    ) as geom
                FROM exploded_lines,
                     generate_series(1, ST_NPoints(geom) - 1) as n
                WHERE ST_GeometryType(geom) = 'ST_LineString'
            )
            SELECT 
                id,
                ST_AsGeoJSON(geom)::json as geom_json,
                ST_Length(geom::geography) as length
            FROM segments
            WHERE geom IS NOT NULL
              AND ST_Length(geom) > 0
        `, layerName)

		var segments []struct {
			ID       int             `gorm:"column:id"`
			GeomJSON json.RawMessage `gorm:"column:geom_json"`
			Length   float64         `gorm:"column:length"`
		}

		err := models.DB.WithContext(ctx).Raw(query, bboxWKT).Scan(&segments).Error
		if err != nil {
			return nil, fmt.Errorf("failed to get geometries from %s: %w", layerName, err)
		}

		// 将每个线段添加到 FeatureCollection
		for _, seg := range segments {
			var geom orb.Geometry
			if err := json.Unmarshal(seg.GeomJSON, &geom); err != nil {
				continue
			}

			feature := geojson.NewFeature(geom)
			feature.Properties["id"] = seg.ID
			feature.Properties["layer"] = layerName
			feature.Properties["length"] = seg.Length
			fc.Append(feature)
		}
	}

	return fc, nil
}

// CalculateShortestPath 计算最短路径，返回 GeoJSON
func (s *TrackService) CalculateShortestPath(
	ctx context.Context,
	linesGeoJSON *geojson.FeatureCollection,
	startPoint []float64,
	endPoint []float64,
) (*geojson.FeatureCollection, error) {
	if linesGeoJSON == nil || len(linesGeoJSON.Features) == 0 {
		return nil, fmt.Errorf("no line segments available")
	}

	// 构建线段数据的 VALUES 子句
	var segmentValues []string
	for i, feature := range linesGeoJSON.Features {
		if feature.Geometry == nil {
			continue
		}

		geomJSON, err := json.Marshal(feature.Geometry)
		if err != nil {
			continue
		}

		segmentValues = append(segmentValues,
			fmt.Sprintf("(%d, ST_GeomFromGeoJSON('%s'))", i+1, string(geomJSON)))
	}

	if len(segmentValues) == 0 {
		return nil, fmt.Errorf("no valid segments")
	}

	segmentsSQL := strings.Join(segmentValues, ",")
	startWKT := fmt.Sprintf("POINT(%f %f)", startPoint[0], startPoint[1])
	endWKT := fmt.Sprintf("POINT(%f %f)", endPoint[0], endPoint[1])

	// 使用递归 CTE 计算最短路径
	query := fmt.Sprintf(`
        WITH 
        -- 线段网络
        network AS (
            SELECT * FROM (VALUES %s) AS t(id, geom)
        ),
        -- 找到起点最近的线段和投影点
        start_info AS (
            SELECT 
                id,
                geom,
                ST_ClosestPoint(geom, ST_GeomFromText($1, 4326)) as snap_point,
                ST_Distance(geom, ST_GeomFromText($1, 4326)) as dist
            FROM network
            ORDER BY dist
            LIMIT 1
        ),
        -- 找到终点最近的线段和投影点
        end_info AS (
            SELECT 
                id,
                geom,
                ST_ClosestPoint(geom, ST_GeomFromText($2, 4326)) as snap_point,
                ST_Distance(geom, ST_GeomFromText($2, 4326)) as dist
            FROM network
            ORDER BY dist
            LIMIT 1
        ),
        -- 递归查找路径
        RECURSIVE path_search AS (
            -- 初始：从起点线段开始
            SELECT 
                s.id,
                s.geom,
                0.0 as total_dist,
                ST_Distance(s.snap_point, e.snap_point) as heuristic,
                ARRAY[s.id] as path_ids,
                s.snap_point as current_point,
                ARRAY[s.geom] as path_segments
            FROM start_info s, end_info e
            
            UNION ALL
            
            -- 递归：查找相邻线段
            SELECT 
                n.id,
                n.geom,
                p.total_dist + ST_Length(n.geom::geography),
                ST_Distance(
                    CASE 
                        WHEN ST_DWithin(ST_StartPoint(n.geom), p.current_point, 0.0001) 
                        THEN ST_EndPoint(n.geom)
                        ELSE ST_StartPoint(n.geom)
                    END,
                    e.snap_point
                ) as heuristic,
                p.path_ids || n.id,
                CASE 
                    WHEN ST_DWithin(ST_StartPoint(n.geom), p.current_point, 0.0001) 
                    THEN ST_EndPoint(n.geom)
                    ELSE ST_StartPoint(n.geom)
                END as current_point,
                p.path_segments || n.geom
            FROM path_search p
            CROSS JOIN end_info e
            JOIN network n ON (
                (ST_DWithin(ST_StartPoint(n.geom), p.current_point, 0.0001) OR
                 ST_DWithin(ST_EndPoint(n.geom), p.current_point, 0.0001))
                AND NOT n.id = ANY(p.path_ids)
            )
            WHERE p.total_dist < 100000  -- 防止无限递归
              AND array_length(p.path_ids, 1) < 1000
        ),
        -- 找到到达终点的最短路径
        best_path AS (
            SELECT 
                p.path_segments,
                p.path_ids,
                p.total_dist
            FROM path_search p
            CROSS JOIN end_info e
            WHERE ST_DWithin(p.current_point, e.snap_point, 0.001)
            ORDER BY p.total_dist + p.heuristic
            LIMIT 1
        )
        -- 返回路径的 GeoJSON FeatureCollection
        SELECT 
            json_build_object(
                'type', 'FeatureCollection',
                'features', COALESCE(
                    (
                        SELECT json_agg(
                            json_build_object(
                                'type', 'Feature',
                                'geometry', ST_AsGeoJSON(geom)::json,
                                'properties', json_build_object(
                                    'segment_id', segment_id,
                                    'sequence', sequence
                                )
                            )
                        )
                        FROM (
                            SELECT 
                                unnest(path_segments) as geom,
                                unnest(path_ids) as segment_id,
                                generate_series(1, array_length(path_ids, 1)) as sequence
                            FROM best_path
                        ) segments
                    ),
                    -- 如果没找到路径，返回直线
                    json_build_array(
                        json_build_object(
                            'type', 'Feature',
                            'geometry', ST_AsGeoJSON(
                                ST_MakeLine(
                                    ST_GeomFromText($1, 4326),
                                    ST_GeomFromText($2, 4326)
                                )
                            )::json,
                            'properties', json_build_object(
                                'type', 'direct_line'
                            )
                        )
                    )
                )
            ) as feature_collection
        FROM (SELECT 1) dummy
        LEFT JOIN best_path ON true
    `, segmentsSQL)

	var fcJSON string
	err := models.DB.WithContext(ctx).Raw(query, startWKT, endWKT).Scan(&fcJSON).Error
	if err != nil {
		return nil, fmt.Errorf("failed to calculate path: %w", err)
	}

	// 解析 FeatureCollection
	fc, err := geojson.UnmarshalFeatureCollection([]byte(fcJSON))
	if err != nil {
		return nil, fmt.Errorf("failed to parse feature collection: %w", err)
	}

	return fc, nil
}

// 辅助函数：将 FeatureCollection 转换为 WKT
func (s *TrackService) featureCollectionToWKT(fc geojson.FeatureCollection) string {
	if len(fc.Features) == 0 {
		return ""
	}

	// 假设第一个 feature 是 bbox 多边形
	feature := fc.Features[0]
	if feature.Geometry == nil {
		return ""
	}

	switch geom := feature.Geometry.(type) {
	case orb.Polygon:
		if len(geom) == 0 || len(geom[0]) == 0 {
			return ""
		}
		coords := geom[0]
		wkt := "POLYGON(("
		for i, coord := range coords {
			if i > 0 {
				wkt += ","
			}
			wkt += fmt.Sprintf("%f %f", coord[0], coord[1])
		}
		wkt += "))"
		return wkt

	case orb.MultiPolygon:
		if len(geom) == 0 ||
			len(geom[0]) == 0 ||
			len(geom[0][0]) == 0 {
			return ""
		}
		coords := geom[0][0]
		wkt := "POLYGON(("
		for i, coord := range coords {
			if i > 0 {
				wkt += ","
			}
			wkt += fmt.Sprintf("%f %f", coord[0], coord[1])
		}
		wkt += "))"
		return wkt
	}

	return ""
}
