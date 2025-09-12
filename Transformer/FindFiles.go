package Transformer

import (
	"os"
	"path/filepath"
	"strings"
)

func FindFiles(root string, Exc string) []string {
	var files []string
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err // 如果遇到错误，直接返回
		}
		// 检查文件是否以.dxf结尾
		if strings.HasSuffix(strings.ToLower(info.Name()), "."+Exc) {
			files = append(files, path)
		}
		return nil
	})
	return files
}
