package methods

import (
	"reflect"
)

func transformStruct(itemValue reflect.Value) map[string]interface{} {
	item := make(map[string]interface{})
	itemType := itemValue.Type()

	for j := 0; j < itemValue.NumField(); j++ {
		field := itemType.Field(j)
		fieldValue := itemValue.Field(j)

		//  Check  if  the  field  is  an  embedded  struct  or  a  slice  of  structs  and  transform  recursively
		if field.Type.Kind() == reflect.Struct {
			//  Recurse  for  nested  structs
			item[CamelCaseToUnderscore(field.Name)] = transformStruct(fieldValue)
		} else if field.Type.Kind() == reflect.Slice && fieldValue.Len() > 0 && fieldValue.Index(0).Kind() == reflect.Struct {
			//  Recurse  for  each  item  in  the  slice  of  structs
			slice := make([]interface{}, fieldValue.Len())
			for i := 0; i < fieldValue.Len(); i++ {
				slice[i] = transformStruct(fieldValue.Index(i))
			}
			item[CamelCaseToUnderscore2(field.Name)] = slice
		} else {
			fieldName := CamelCaseToUnderscore2(field.Name)
			item[fieldName] = fieldValue.Interface()
		}
	}
	return item
}

func LowerJSONTransform(xmList interface{}) interface{} {
	var result []map[string]interface{}
	slice := reflect.ValueOf(xmList)
	if slice.Kind() != reflect.Slice {
		return transformStruct(slice)
	}
	for i := 0; i < slice.Len(); i++ {
		itemValue := slice.Index(i)
		if itemValue.Kind() == reflect.Struct {
			//  Process  a  single  struct
			transformedItem := transformStruct(itemValue)
			result = append(result, transformedItem)
		} else {
			//  If  the  items  are  not  struct  types,  add  them  as-is
			result = append(result, itemValue.Interface().(map[string]interface{}))
		}
	}
	return result
}
