package methods

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
)

func EncryptStr(text string, key string) string {
	encrypted := make([]byte, len(text))
	for i := 0; i < len(text); i++ {
		encrypted[i] = byte((int(text[i]) + int(key[i%len(key)])) % 256)
	}

	hexEncoded := hex.EncodeToString(encrypted)
	base64Encoded := base64.StdEncoding.EncodeToString([]byte(hexEncoded))
	return base64Encoded
}

func DecryptStr(encryptedB64Str string, key string) string {
	base64Decoded, err := base64.StdEncoding.DecodeString(encryptedB64Str)

	if err != nil {
		return ""
	}

	hexDecoded, err := hex.DecodeString(string(base64Decoded))

	if err != nil {
		return ""
	}

	decrypted := make([]byte, len(hexDecoded))
	for i := 0; i < len(hexDecoded); i++ {
		decrypted[i] = byte((int(hexDecoded[i]) - int(key[i%len(key)])) % 256)
	}

	return string(decrypted)
}

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
