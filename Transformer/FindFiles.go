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
			return err
		}
		// 匹配指定扩展名的文件或文件夹
		if strings.HasSuffix(strings.ToLower(info.Name()), "."+strings.ToLower(Exc)) {
			files = append(files, path)
		}
		return nil
	})
	// 保留最深层的路径
	var result []string
	for i, f1 := range files {
		isParent := false
		for j, f2 := range files {
			// 如果 f2 是 f1 的子路径，说明 f1 是父路径，应该被过滤
			if i != j && strings.HasPrefix(f2, f1+string(filepath.Separator)) {
				isParent = true
				break
			}
		}
		if !isParent {
			result = append(result, f1)
		}
	}
	return result
}
