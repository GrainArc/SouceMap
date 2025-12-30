package Transformer

import (
	"os"
	"path/filepath"
	"strings"
)

// 方案2：排除重复的父子路径关系（推荐）
func FindFiles(root string, Exc string) []string {
	var files []string
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if strings.HasSuffix(strings.ToLower(info.Name()), "."+Exc) {
			files = append(files, path)
		}
		return nil
	})

	// 过滤掉被其他路径包含的项
	var result []string
	for i, f1 := range files {
		isChild := false
		for j, f2 := range files {
			if i != j && strings.HasPrefix(f1, f2+string(filepath.Separator)) {
				isChild = true
				break
			}
		}
		if !isChild {
			result = append(result, f1)
		}
	}

	return result
}
