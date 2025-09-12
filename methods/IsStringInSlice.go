package methods

import (
	"github.com/mozillazg/go-pinyin"
	"regexp"
	"strings"
	"unicode"
)

func IsStringInSlice(s string, slice []string) bool {
	set := make(map[string]bool)
	for _, v := range slice {
		set[v] = true
	}
	return set[s]
}
func RemoveKeyFromMapArray(input []map[string]interface{}, key string) []map[string]interface{} {
	for _, m := range input {
		if _, ok := m[key]; ok {
			delete(m, key)
		}
	}
	return input
}
func moveLeadingNumbersToEnd(s string) string {
	// 定义正则表达式，匹配字符串开头的数字
	re := regexp.MustCompile(`^(\d+)(.*)$`)
	// 使用正则表达式提取匹配部分
	match := re.FindStringSubmatch(s)
	// match[0] 是整个匹配字符串，match[1] 是前导数字，match[2] 是剩余部分
	if len(match) == 3 {
		return match[2] + match[1]
	}
	// 如果没有找到匹配的前导数字，就返回原字符串
	return s
}
func filterString(str string) string {
	// 定义正则表达式，匹配中文、英文和数字
	reg := regexp.MustCompile("[^\\p{Han}\\p{Latin}\\p{N}_]")

	// 使用正则表达式替换掉非中文、英文和数字的字符
	result := reg.ReplaceAllString(str, "")

	// 去除字符串中的空格
	result = strings.ReplaceAll(result, " ", "")

	return result
}

// ConvertToInitials  将中文字符串转换为拼音首字母拼接字符串
func ConvertToInitials(hanzi string) string {
	// 配置选项，选择带声调和不带声调的组合，并提取首字母
	hanzi = filterString(hanzi)
	a := pinyin.NewArgs()
	a.Style = pinyin.FirstLetter // 设置拼音风格为首字母
	var result string
	for _, runeValue := range hanzi {
		if unicode.Is(unicode.Han, runeValue) {
			// 如果是汉字，则获取拼音首字母
			pinyinSlice := pinyin.SinglePinyin(runeValue, a)
			if len(pinyinSlice) > 0 {
				result += pinyinSlice[0]
			}
		} else {
			// 如果不是汉字，则直接保留字符
			result += string(runeValue)
		}
	}
	processed := moveLeadingNumbersToEnd(result)
	str := strings.ToLower(processed)
	return str
}
