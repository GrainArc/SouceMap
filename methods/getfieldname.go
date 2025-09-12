package methods

import (
	"reflect"
	"strings"
	"unicode"
)

func GetFieldName(i interface{}) []string {
	v := reflect.ValueOf(i)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()
	numField := t.NumField()
	var fieldNames []string
	for i := 0; i < numField; i++ {
		fieldName := strings.ToLower(t.Field(i).Name)
		if fieldName != "geom" {
			fieldNames = append(fieldNames, fieldName)
		}
	}
	return fieldNames
}

func CamelCaseToUnderscore(str string) string {
	var result strings.Builder
	result.Grow(len(str) + 5) // 预估一下不合理就会导致多次扩容
	var last rune = -1        // 上一个字符
	for _, r := range str {
		if unicode.IsUpper(r) {
			if last != -1 && !unicode.IsUpper(last) {
				result.WriteRune('_')
			}
			result.WriteRune(unicode.ToLower(r))
		} else {
			result.WriteRune(r)
		}
		last = r
	}
	return result.String()
}
func CamelCaseToUnderscore2(str string) string {
	var result strings.Builder
	result.Grow(len(str) + 5) // 预估一下不合理就会导致多次扩容
	var last rune = -1        // 上一个字符
	for _, r := range str {
		if unicode.IsUpper(r) {
			if last != -1 && !unicode.IsUpper(last) {
				result.WriteRune('_')
			}
			result.WriteRune(unicode.ToLower(r))
		} else {
			result.WriteRune(r)
		}
		last = r
	}
	return result.String()
}
