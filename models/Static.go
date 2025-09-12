package models

type Tile struct {
	ZoomLevel  int64
	TileColumn int64
	TileRow    int64
	TileData   []byte
}

type TilesByte struct {
	Folder   string
	TileName string
	TileData []byte
}
type TilesHeader struct {
	Folder   string
	JsonName string
	TileJson []byte
}

type TileSetJson struct {
	Asset struct {
		Version      string `json:"version"`
		Generatetool string `json:"generatetool"`
		GltfUpAxis   string `json:"gltfUpAxis"`
	} `json:"asset"`
	ExtensionsUsed []string `json:"extensionsUsed"`
	GeometricError float64  `json:"geometricError"`
	Refine         string   `json:"refine"`
	Root           struct {
		BoundingVolume struct {
			Sphere []float64 `json:"sphere"`
		} `json:"boundingVolume"`
		GeometricError float64 `json:"geometricError"`
		Children       []struct {
			BoundingVolume struct {
				Sphere []float64 `json:"sphere"`
			} `json:"boundingVolume"`
			GeometricError float64 `json:"geometricError"`
			Children       []struct {
				BoundingVolume struct {
					Sphere []float64 `json:"sphere"`
				} `json:"boundingVolume"`
				GeometricError int `json:"geometricError"`
				Content        struct {
					Uri string `json:"uri"`
				} `json:"content"`
			} `json:"children"`
		} `json:"children"`
	} `json:"root"`
}

// BoundingVolume 包围体
type BoundingVolume struct {
	Sphere []float64 `json:"sphere,omitempty"`
	Box    []float64 `json:"box,omitempty"`
	Region []float64 `json:"region,omitempty"`
}

// Content 内容
type Content struct {
	Uri string `json:"uri,omitempty"`
}

// Tile 瓦片节点 - 递归结构
type Tile2 struct {
	BoundingVolume BoundingVolume `json:"boundingVolume"`
	Refine         string         `json:"refine,omitempty"`
	GeometricError interface{}    `json:"geometricError"` // 使用interface{}因为可能是int或float64
	Content        Content        `json:"content,omitempty"`
	Children       []Tile2        `json:"children,omitempty"` // 递归引用自身
}

// Asset 资产信息
type Asset struct {
	Version      string `json:"version"`
	Generatetool string `json:"generatetool,omitempty"`
	GltfUpAxis   string `json:"gltfUpAxis,omitempty"`
}

// TilesJson 3D Tiles 主结构
type TilesJson struct {
	Asset          Asset   `json:"asset"`
	GeometricError float64 `json:"geometricError"`
	Root           Tile2   `json:"root"`
}
type TilesSet struct {
	Name   string `gorm:"type:varchar(255);primary_key"`
	UpDown string `gorm:"type:varchar(255)"`
}
