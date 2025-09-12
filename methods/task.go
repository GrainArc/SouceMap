package methods

func ToStringSlice(args []interface{}) []string {
	strSlice := make([]string, len(args))
	for i, v := range args {
		strSlice[i] = v.(string) // 类型断言转换为 string 类型
	}
	return strSlice
}
