package methods

import (
	"fmt"
	"os"
	"path/filepath"
)

// 删除文件夹内的所有文件
func DeleteFiles(dirPath string) error {
	// 读取目录中的所有文件和子目录
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("读取目录失败: %w", err)
	}

	// 遍历删除目录中的所有内容
	for _, entry := range entries {
		path := filepath.Join(dirPath, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("删除 %s 失败: %w", path, err)
		}
	}

	return nil
}

func GetAllFiles(path string) ([]string, error) {
	var files []string
	err := filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
		// 排除文件夹本身
		if filePath != path {
			if !info.IsDir() {
				files = append(files, filePath)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func FindShpFile(dir string, ex string) *string {
	files, err := os.ReadDir(dir)
	if err != nil {
		fmt.Println("读取文件夹失败:", err)
		return nil
	}
	for _, file := range files {
		if !file.IsDir() {
			ext := filepath.Ext(file.Name())
			if ext == ex {
				path := filepath.Join(dir, file.Name())
				return &path
			}
		}
	}
	return nil
}
func DirectoryExists(dirPath string, folderName string) bool {
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && info.Name() == folderName {
			return fmt.Errorf("folder exists")
		}
		return nil
	})
	if err != nil && err.Error() == "folder exists" {
		return true
	}
	return false
}
