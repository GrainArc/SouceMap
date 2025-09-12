package methods

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"github.com/mholt/archiver/v3"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
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
		if err := extractFile(file, unpath); err != nil {
			return err
		}
	}
	return nil
}

func extractFile(zf *zip.File, dest string) error {
	fpath := filepath.Join(dest, zf.Name)

	// 防止解压到目标目录之外
	if !strings.HasPrefix(fpath, filepath.Clean(dest)+string(os.PathSeparator)) {
		return fmt.Errorf("%s: illegal file path", fpath)
	}

	if zf.FileInfo().IsDir() {
		os.MkdirAll(fpath, os.ModePerm)
		return nil
	} else {
		if err := os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return err
		}
		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, zf.Mode())
		if err != nil {
			return err
		}
		rc, err := zf.Open()
		if err != nil {
			return err
		}
		_, err = io.Copy(outFile, rc)
		rc.Close()
		outFile.Close()
		return err
	}
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
	unpath := dirpath + fileName[0:len(fileName)-len(fileExt)]
	os.Mkdir(unpath, os.ModePerm)
	err1 := archiver.Unarchive(src, unpath)
	return err1

}
func isGBK(str string) bool {
	return utf8.ValidString(str) && !utf8.Valid([]byte(str))
}

func gbkToUtf8(s string) (string, error) {
	reader := transform.NewReader(bytes.NewReader([]byte(s)), simplifiedchinese.GB18030.NewDecoder())
	d, e := io.ReadAll(reader)
	if e != nil {
		return "", e
	}
	return string(d), nil
}

func ZipFolder(folderPath string, name string) error {
	zipPath := filepath.Join(folderPath, name+".zip")
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
		// Skip the zip file itself
		if zipPath == path {
			return nil
		}
		// Skip directories since they are implicitly added by including their files
		if info.IsDir() {
			return nil
		}
		relPath, err := filepath.Rel(folderPath, path)
		if err != nil {
			return err
		}
		zipFileHeader, err := zipWriter.Create(relPath)
		if err != nil {
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = io.Copy(zipFileHeader, file)
		if err != nil {
			return err
		}
		return nil
	})
}

func ZipFolderTo(folderPath string, outpath string) error {
	zipFile, err := os.Create(outpath)
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
		// Skip the zip file itself
		if strings.HasSuffix(path, ".zip") {
			return nil
		}
		// Skip directories since they are implicitly added by including their files
		if info.IsDir() {
			return nil
		}
		relPath, err := filepath.Rel(folderPath, path)
		if err != nil {
			return err
		}
		zipFileHeader, err := zipWriter.Create(relPath)
		if err != nil {
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = io.Copy(zipFileHeader, file)
		if err != nil {
			return err
		}
		return nil
	})
}

func ZipFileOut(folderPath string) ([]byte, error) {
	var buf bytes.Buffer
	zipWriter := zip.NewWriter(&buf)
	defer zipWriter.Close()
	err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Skip directories since they are implicitly added by including their files
		if info.IsDir() || strings.HasSuffix(path, ".zip") {
			return nil
		}
		relPath, err := filepath.Rel(folderPath, path)
		if err != nil {
			return err
		}
		zipFileHeader, err := zipWriter.Create(relPath)
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
	err = zipWriter.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
