package methods

// mergeMaps函数负责合并两个map成一个[]map[string]interface{}
func MergeMaps(map1, map2 map[string]interface{}) map[string]interface{} {
	// 创建一个切片用于存放结果

	// 创建新map来合并两个输入map的内容
	mergedMap := make(map[string]interface{})
	// 遍历第一个map并将其内容添加到新map中
	for k, v := range map1 {
		mergedMap[k] = v
	}
	// 遍历第二个map并将其内容添加到新map中，如果有重复的键，则覆盖
	for k, v := range map2 {
		mergedMap[k] = v
	}
	// 将合并后的新map添加到结果切片中

	return mergedMap
}
