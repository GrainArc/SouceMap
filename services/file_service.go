// service/file_service.go
package services

import (
	"github.com/gin-gonic/gin"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type FileNode struct {
	Name    string    `json:"name"`
	Path    string    `json:"path"` // 绝对路径
	IsDir   bool      `json:"isDir"`
	Size    int64     `json:"size"`
	Ext     string    `json:"ext"` // 文件扩展名
	ModTime time.Time `json:"modTime"`
}

type FileService struct {
	RootPath string // 限制访问的根目录
}

func NewFileService(rootPath string) *FileService {
	// 确保根路径是绝对路径
	absRoot, _ := filepath.Abs(rootPath)
	return &FileService{
		RootPath: absRoot,
	}
}

// GetDirectoryContent 获取指定目录下的直接子项（非递归）
func (s *FileService) GetDirectoryContent(requestPath string) ([]FileNode, error) {
	var targetPath string

	// 如果请求路径为空，使用根目录
	if requestPath == "" {
		targetPath = s.RootPath
	} else {
		targetPath = requestPath
	}

	// 安全检查：确保请求的路径在根目录下
	if !s.isPathSafe(targetPath) {
		return nil, os.ErrPermission
	}

	// 检查路径是否存在且是目录
	info, err := os.Stat(targetPath)
	if err != nil {
		return nil, err
	}

	if !info.IsDir() {
		return nil, os.ErrInvalid
	}

	// 读取目录内容
	files, err := ioutil.ReadDir(targetPath)
	if err != nil {
		return nil, err
	}

	nodes := make([]FileNode, 0, len(files))
	for _, file := range files {
		absolutePath := filepath.Join(targetPath, file.Name())

		// 获取文件扩展名
		ext := ""
		if !file.IsDir() {
			ext = strings.ToLower(filepath.Ext(file.Name()))
			// 移除扩展名前的点
			if len(ext) > 0 {
				ext = ext[1:]
			}
		}

		node := FileNode{
			Name:    file.Name(),
			Path:    absolutePath, // 绝对路径
			IsDir:   file.IsDir(),
			Size:    file.Size(),
			Ext:     ext,
			ModTime: file.ModTime(),
		}
		nodes = append(nodes, node)
	}

	return nodes, nil
}

// GetRootPath 获取根目录信息
func (s *FileService) GetRootPath() string {
	return s.RootPath
}

// isPathSafe 检查路径是否安全（防止目录遍历攻击）
func (s *FileService) isPathSafe(path string) bool {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false
	}

	absRoot, err := filepath.Abs(s.RootPath)
	if err != nil {
		return false
	}

	// 检查请求的路径是否在根目录下
	rel, err := filepath.Rel(absRoot, absPath)
	if err != nil {
		return false
	}

	// 不允许访问根目录之外的路径
	return !strings.HasPrefix(rel, "..") && !filepath.IsAbs(rel)
}

type FileController struct {
	fileService *FileService
}

func NewFileController(fileService *FileService) *FileController {
	return &FileController{
		fileService: fileService,
	}
}

// GetDirectoryContent 获取目录内容（懒加载）
func (c *FileController) GetDirectoryContent(ctx *gin.Context) {
	// 从查询参数获取路径，如果为空则返回根目录
	path := ctx.Query("path")

	content, err := c.fileService.GetDirectoryContent(path)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"code": -1,
			"data": nil,
			"msg":  err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": content,
		"msg":  "success",
	})
}

// GetRootPath 获取根目录路径
func (c *FileController) GetRootPath(ctx *gin.Context) {
	rootPath := c.fileService.GetRootPath()

	ctx.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": gin.H{
			"rootPath": rootPath,
		},
		"msg": "success",
	})
}
