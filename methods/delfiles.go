package methods

import (
	"fmt"
	"os"
	"path/filepath"
)

// 删除文件夹内的所有文件
func DeleteFiles(dirPath string) {
	// 判断文件夹是否存在
	os.RemoveAll(dirPath)
	os.Mkdir("TempFile", os.ModePerm)
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
