package methods

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	Runtime "runtime"
	"strings"
)

func OpenURL(url string) error {
	var cmd *exec.Cmd

	switch Runtime.GOOS {
	case "linux":
		cmd = exec.Command("xdg-open", url) // Linux
	case "darwin":
		cmd = exec.Command("open", url) // macOS
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url) // Windows
	default:
		return fmt.Errorf("unsupported platform")
	}

	return cmd.Start() // 启动命令
}

func ReplaceInJSFiles(dir string, oldValue string, newValue string) error {
	// 检查路径是否为文件
	fileInfo, err := os.Stat(dir)
	if err != nil {
		return err
	}

	// 如果是文件，直接处理该文件
	if !fileInfo.IsDir() {
		// 检查是否为 .js 文件
		if filepath.Ext(dir) == ".js" {
			return replaceInSingleFile(dir, oldValue, newValue)
		}
		// 如果不是 .js 文件，返回错误或忽略
		return fmt.Errorf("file %s is not a JavaScript file", dir)
	}

	// 如果是目录，按原来的逻辑处理
	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, file := range files {
		// 只处理 .js 文件
		if filepath.Ext(file.Name()) == ".js" {
			filePath := filepath.Join(dir, file.Name())
			err := replaceInSingleFile(filePath, oldValue, newValue)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// 提取出来的单文件处理函数
func replaceInSingleFile(filePath string, oldValue string, newValue string) error {
	// 读取文件内容
	content, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	// 替换内容
	updatedContent := strings.ReplaceAll(string(content), oldValue, newValue)

	// 如果内容有变化，写回文件
	if updatedContent != string(content) {
		err := os.WriteFile(filePath, []byte(updatedContent), 0644)
		if err != nil {
			return err
		}
	}
	return nil
}
