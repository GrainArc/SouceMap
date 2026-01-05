package methods

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/axgle/mahonia"
	"github.com/mholt/archiver/v3"
)

func Unzip(src string) error {
	ext := filepath.Ext(src)
	switch strings.ToLower(ext) {
	case ".zip":
		return UnzipZip(src)
	case ".rar":
		return UnzipRar(src)
	default:
		return errors.New("Unsupported file format")
	}
}

func UnzipZip(src string) error {
	dirpath, _ := filepath.Split(src)
	fileName := filepath.Base(src)
	fileExt := filepath.Ext(src)
	unpath := filepath.Join(dirpath, fileName[0:len(fileName)-len(fileExt)])

	if _, err := os.Stat(unpath); os.IsNotExist(err) {
		if err := os.Mkdir(unpath, os.ModePerm); err != nil {
			return err
		}
	}

	reader, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer reader.Close()

	for _, file := range reader.File {
		if err := extractFileWithEncoding(file, unpath); err != nil {
			return err
		}
	}
	return nil
}

// detectAndConvertEncoding 检测并转换文件名编码
func detectAndConvertEncoding(fileName string) string {
	// 如果已经是有效的 UTF-8，直接返回
	if utf8.ValidString(fileName) {
		return fileName
	}

	// 尝试常见的中文编码
	encodings := []string{"gbk", "gb18030", "gb2312", "big5"}

	for _, encoding := range encodings {
		decoder := mahonia.NewDecoder(encoding)
		if decoder == nil {
			continue
		}

		// 尝试解码
		decoded := decoder.ConvertString(fileName)
		// 检查解码后是否为有效的 UTF-8
		if utf8.ValidString(decoded) {
			// 进一步验证：检查是否包含可打印字符
			if isPrintableString(decoded) {
				return decoded
			}
		}
	}

	// 如果所有编码都失败，返回原始文件名
	return fileName
}

// isPrintableString 检查字符串是否包含可打印字符
func isPrintableString(s string) bool {
	if len(s) == 0 {
		return false
	}

	printableCount := 0
	for _, r := range s {
		// 检查是否为可打印字符（包括中文字符范围）
		if r >= 0x20 && r <= 0x7E || // ASCII 可打印字符
			r >= 0x4E00 && r <= 0x9FA5 || // 中文常用字
			r >= 0x3400 && r <= 0x4DB5 || // 中文扩展A
			r >= 0x20000 && r <= 0x2A6D6 { // 中文扩展B
			printableCount++
		}
	}

	// 至少50%的字符是可打印的
	return float64(printableCount)/float64(len([]rune(s))) > 0.5
}

// extractFileWithEncoding 处理文件名编码问题
func extractFileWithEncoding(zf *zip.File, dest string) error {
	// 处理文件名编码
	fileName := detectAndConvertEncoding(zf.Name)

	// 清理文件名中的非法字符
	fileName = filepath.Clean(fileName)
	fpath := filepath.Join(dest, fileName)

	// 防止解压到目标目录之外（路径遍历攻击）
	cleanDest := filepath.Clean(dest) + string(os.PathSeparator)
	cleanPath := filepath.Clean(fpath)

	if !strings.HasPrefix(cleanPath, cleanDest) && cleanPath != filepath.Clean(dest) {
		return fmt.Errorf("%s: illegal file path", fpath)
	}

	if zf.FileInfo().IsDir() {
		return os.MkdirAll(fpath, os.ModePerm)
	}

	// 确保父目录存在
	if err := os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// 创建文件
	outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, zf.Mode())
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer outFile.Close()

	// 打开压缩文件中的文件
	rc, err := zf.Open()
	if err != nil {
		return fmt.Errorf("failed to open compressed file: %w", err)
	}
	defer rc.Close()

	// 复制内容
	if _, err = io.Copy(outFile, rc); err != nil {
		return fmt.Errorf("failed to copy file content: %w", err)
	}

	return nil
}

// extractFile 保留原有函数以兼容
func extractFile(zf *zip.File, dest string) error {
	return extractFileWithEncoding(zf, dest)
}

func UnzipRar(src string) error {
	rarFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer rarFile.Close()

	dirpath, _ := filepath.Split(src)
	fileName := filepath.Base(src)
	fileExt := filepath.Ext(src)
	unpath := filepath.Join(dirpath, fileName[0:len(fileName)-len(fileExt)])

	if err := os.MkdirAll(unpath, os.ModePerm); err != nil {
		return err
	}

	return archiver.Unarchive(src, unpath)
}

// ConvertToUTF8 通用编码转换函数
func ConvertToUTF8(src string, srcEncoding string) (string, error) {
	decoder := mahonia.NewDecoder(srcEncoding)
	if decoder == nil {
		return "", fmt.Errorf("unsupported encoding: %s", srcEncoding)
	}
	return decoder.ConvertString(src), nil
}

// ConvertFromUTF8 从 UTF-8 转换到指定编码
func ConvertFromUTF8(src string, destEncoding string) (string, error) {
	encoder := mahonia.NewEncoder(destEncoding)
	if encoder == nil {
		return "", fmt.Errorf("unsupported encoding: %s", destEncoding)
	}
	return encoder.ConvertString(src), nil
}

func ZipFolder(folderPath string, name string) error {
	zipPath := filepath.Join(folderPath, name+".zip")
	return createZipFile(zipPath, folderPath, func(path string) bool {
		return zipPath == path
	})
}

func ZipFolderTo(folderPath string, outpath string) error {
	return createZipFile(outpath, folderPath, func(path string) bool {
		return strings.HasSuffix(path, ".zip")
	})
}

// createZipFile 通用的创建 zip 文件函数
func createZipFile(zipPath string, folderPath string, skipFunc func(string) bool) error {
	zipFile, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	return filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 使用自定义跳过函数
		if skipFunc(path) {
			return nil
		}

		// 跳过目录
		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(folderPath, path)
		if err != nil {
			return err
		}

		// 创建 zip 文件头
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		header.Name = relPath
		header.Method = zip.Deflate // 使用压缩

		zipFileHeader, err := zipWriter.CreateHeader(header)
		if err != nil {
			return err
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		_, err = io.Copy(zipFileHeader, file)
		return err
	})
}

func ZipFileOut(folderPath string) ([]byte, error) {
	var buf bytes.Buffer
	zipWriter := zip.NewWriter(&buf)

	err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 跳过目录和 zip 文件
		if info.IsDir() || strings.HasSuffix(path, ".zip") {
			return nil
		}

		relPath, err := filepath.Rel(folderPath, path)
		if err != nil {
			return err
		}

		// 创建 zip 文件头
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		header.Name = relPath
		header.Method = zip.Deflate

		zipFileHeader, err := zipWriter.CreateHeader(header)
		if err != nil {
			return err
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		_, err = io.Copy(zipFileHeader, file)
		return err
	})

	if err != nil {
		return nil, err
	}

	if err := zipWriter.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func UnzipToDir(src string, destDir string) error {
	// 检查源文件是否存在
	if _, err := os.Stat(src); os.IsNotExist(err) {
		return fmt.Errorf("source file does not exist: %s", src)
	}

	// 确保目标目录存在
	if err := os.MkdirAll(destDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	// 打开 zip 文件
	reader, err := zip.OpenReader(src)
	if err != nil {
		return fmt.Errorf("failed to open zip file: %w", err)
	}
	defer reader.Close()

	// 解压每个文件（使用支持编码转换的函数）
	for _, file := range reader.File {
		if err := extractFileWithEncoding(file, destDir); err != nil {
			return fmt.Errorf("failed to extract file %s: %w", file.Name, err)
		}
	}

	return nil
}

// UnzipWithEncoding 指定编码解压
func UnzipWithEncoding(src string, destDir string, encoding string) error {
	if _, err := os.Stat(src); os.IsNotExist(err) {
		return fmt.Errorf("source file does not exist: %s", src)
	}

	if err := os.MkdirAll(destDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	reader, err := zip.OpenReader(src)
	if err != nil {
		return fmt.Errorf("failed to open zip file: %w", err)
	}
	defer reader.Close()

	decoder := mahonia.NewDecoder(encoding)
	if decoder == nil {
		return fmt.Errorf("unsupported encoding: %s", encoding)
	}

	for _, file := range reader.File {
		fileName := decoder.ConvertString(file.Name)
		if err := extractFileWithCustomName(file, destDir, fileName); err != nil {
			return fmt.Errorf("failed to extract file %s: %w", fileName, err)
		}
	}

	return nil
}

// extractFileWithCustomName 使用自定义文件名解压
func extractFileWithCustomName(zf *zip.File, dest string, customName string) error {
	fileName := filepath.Clean(customName)
	fpath := filepath.Join(dest, fileName)

	cleanDest := filepath.Clean(dest) + string(os.PathSeparator)
	cleanPath := filepath.Clean(fpath)

	if !strings.HasPrefix(cleanPath, cleanDest) && cleanPath != filepath.Clean(dest) {
		return fmt.Errorf("%s: illegal file path", fpath)
	}

	if zf.FileInfo().IsDir() {
		return os.MkdirAll(fpath, os.ModePerm)
	}

	if err := os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
		return err
	}

	outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, zf.Mode())
	if err != nil {
		return err
	}
	defer outFile.Close()

	rc, err := zf.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	_, err = io.Copy(outFile, rc)
	return err
}
