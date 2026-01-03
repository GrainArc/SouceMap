package methods

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
)

func Md5Str(data string) string {

	// 创建一个 MD5 哈希对象
	hash := md5.New()

	// 将数据写入哈希对象
	hash.Write([]byte(data))

	// 获取加密结果（字节数组）
	md5Bytes := hash.Sum(nil)

	// 将加密结果转换为十六进制字符串
	md5String := hex.EncodeToString(md5Bytes)

	// 输出加密结果
	return md5String
}

func EncryptStr(text string, key string) (string, error) {
	// 1. 处理密钥（确保16/24/32字节）
	keyBytes := []byte(key)
	keyBytes = padKey(keyBytes)

	// 2. 创建 AES cipher
	block, err := aes.NewCipher(keyBytes)
	if err != nil {
		return "", err
	}

	// 3. 创建 GCM 模式
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	// 4. 生成随机 nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	// 5. 加密
	encrypted := gcm.Seal(nonce, nonce, []byte(text), nil)

	// 6. Base64 编码
	return base64.StdEncoding.EncodeToString(encrypted), nil
}

func DecryptStr(encryptedB64Str string, key string) (string, error) {
	// 1. 处理密钥
	keyBytes := []byte(key)
	keyBytes = padKey(keyBytes)

	// 2. Base64 解码
	encrypted, err := base64.StdEncoding.DecodeString(encryptedB64Str)
	if err != nil {
		return "", err
	}

	// 3. 创建 AES cipher
	block, err := aes.NewCipher(keyBytes)
	if err != nil {
		return "", err
	}

	// 4. 创建 GCM 模式
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	// 5. 提取 nonce 和密文
	nonceSize := gcm.NonceSize()
	if len(encrypted) < nonceSize {
		return "", fmt.Errorf("密文过短")
	}

	nonce, ciphertext := encrypted[:nonceSize], encrypted[nonceSize:]

	// 6. 解密
	decrypted, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}

	return string(decrypted), nil
}

// padKey 将密钥填充到标准长度（16、24 或 32 字节）
func padKey(key []byte) []byte {
	switch len(key) {
	case 16, 24, 32:
		return key
	case 0:
		return make([]byte, 16)
	default:
		if len(key) < 16 {
			return padTo16(key)
		} else if len(key) < 24 {
			return padTo24(key)
		} else {
			return padTo32(key)
		}
	}
}

func padTo16(key []byte) []byte {
	padded := make([]byte, 16)
	copy(padded, key)
	return padded
}

func padTo24(key []byte) []byte {
	padded := make([]byte, 24)
	copy(padded, key)
	return padded
}

func padTo32(key []byte) []byte {
	padded := make([]byte, 32)
	copy(padded, key)
	return padded
}
