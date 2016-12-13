package csvrecord

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
)

var Comma = '\t'
var Comment = '#'

type Index map[interface{}]interface{}
type CsvRecord struct {
	Comma      rune
	Comment    rune
	typeRecord reflect.Type
	records    []interface{}
	indexesMap map[string]Index
}

type fieldInfo struct {
	fieldName  string
	fieldType  string
	fieldIndex int
}

//原始变量赋值
func setValue(value reflect.Value, str string) error {
	switch value.Kind() {
	case reflect.Int:
		var v int64
		v, err := strconv.ParseInt(str, 0, value.Type().Bits())
		if err == nil {
			value.SetInt(v)
		} else {
			return err
		}
	case reflect.Float32:
		var v float64
		v, err := strconv.ParseFloat(str, value.Type().Bits())
		if err == nil {
			value.SetFloat(v)
		} else {
			return err
		}
	case reflect.String:
		value.SetString(str)
		return nil
	}
	return nil
}

func New(st interface{}) (*CsvRecord, error) {
	typeRecord := reflect.TypeOf(st)

	if typeRecord == nil || typeRecord.Kind() != reflect.Struct {
		return nil, errors.New("st must be a struct")
	}

	for i := 0; i < typeRecord.NumField(); i++ {
		f := typeRecord.Field(i)

		kind := f.Type.Kind()
		switch kind {
		case reflect.Int:
		case reflect.String:
		case reflect.Float32:
		case reflect.Slice:
		default:
			return nil, fmt.Errorf("invalid type: %v %s", f.Name, kind)
		}

		tag := f.Tag.Get("index")
		if tag == "true" {
			switch kind {
			case reflect.Slice:
				return nil, fmt.Errorf("count not index %s field %v %v",
					kind, i, f.Name)
			}
		}
	}

	rf := new(CsvRecord)
	rf.typeRecord = typeRecord
	return rf, nil
}

func (cr *CsvRecord) Read(fileName string) error {
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	if cr.Comma == 0 {
		cr.Comma = Comma
	}
	if cr.Comment == 0 {
		cr.Comment = Comment
	}

	reader := csv.NewReader(file)
	reader.Comma = cr.Comma
	reader.Comment = cr.Comment
	fieldTypeLine, err := reader.Read()
	if err != nil {
		return err
	}

	fieldNameLine, err := reader.Read()
	if err != nil {
		return err
	}

	dataLines, err := reader.ReadAll()
	if err != nil {
		return err
	}

	typeRecord := cr.typeRecord
	fieldInfoMap := make(map[string]*fieldInfo)
	for i := 0; i < len(fieldNameLine); i++ {
		fieldInfo := &fieldInfo{
			fieldName:  fieldNameLine[i],
			fieldType:  fieldTypeLine[i],
			fieldIndex: i,
		}
		fieldInfoMap[fieldInfo.fieldName] = fieldInfo
	}

	indexesMap := make(map[string]Index)

	for i := 0; i < typeRecord.NumField(); i++ {

		tag := typeRecord.Field(i).Tag.Get("index")
		if tag == "true" {
			indexesMap[typeRecord.Field(i).Tag.Get("csv")] = make(Index)
		}
	}

	records := make([]interface{}, len(dataLines))
	for n := 0; n < len(dataLines); n++ {
		value := reflect.New(typeRecord)
		records[n] = value.Interface()
		record := value.Elem()

		dataLine := dataLines[n]

		for i := 0; i < typeRecord.NumField(); i++ {
			f := typeRecord.Field(i)
			fieldName := f.Tag.Get("csv")
			fInfo := fieldInfoMap[fieldName]
			fieldStr := dataLine[fInfo.fieldIndex]

			field := record.Field(i)

			if !field.CanSet() {
				continue
			}

			var err error
			kind := field.Kind()
			if fInfo.fieldType == "int" && kind == reflect.Int {
				err = setValue(field, fieldStr)
			} else if fInfo.fieldType == "float" && kind == reflect.Float32 {
				err = setValue(field, fieldStr)
			} else if fInfo.fieldType == "string" && kind == reflect.String {
				err = setValue(field, fieldStr)
			} else if fInfo.fieldType == "array1" && kind == reflect.Slice {
				arrayElemStrs := strings.Split(fieldStr, ",")
				field.Set(reflect.MakeSlice(field.Type(), len(arrayElemStrs), len(arrayElemStrs)))
				for i := 0; i < len(arrayElemStrs); i++ {
					err = setValue(field.Index(i), arrayElemStrs[i])
					if err != nil {
						break
					}
				}
			} else if fInfo.fieldType == "array2" && kind == reflect.Slice {
				array2ElemStrs := strings.Split(fieldStr, "|")
				field.Set(reflect.MakeSlice(field.Type(), len(array2ElemStrs), len(array2ElemStrs)))
				for i := 0; i < len(array2ElemStrs); i++ {
					array1ElemStrs := strings.Split(array2ElemStrs[i], ",")
					field.Index(i).Set(reflect.MakeSlice(field.Index(i).Type(), len(array1ElemStrs), len(array1ElemStrs)))
					for j := 0; j < len(array1ElemStrs); j++ {
						err = setValue(field.Index(i).Index(j), array1ElemStrs[j])
						if err != nil {
							break
						}
					}
					if err != nil {
						break
					}
				}
			}

			if err != nil {
				return fmt.Errorf("parse field (row=%v, col=%s) error: %v", n, fInfo.fieldName, err)
			}
			if f.Tag.Get("index") == "true" {
				index := indexesMap[f.Tag.Get("csv")]
				if _, ok := index[field.Interface()]; ok {
					return fmt.Errorf("index error: duplicate at (row=%v, col=%v", n, i)
				}
				index[field.Interface()] = records[n]
			}
		}
	}

	cr.records = records
	cr.indexesMap = indexesMap

	return nil
}

//返回第i+1条数据 i < NumRecord()
func (cr *CsvRecord) Record(i int) interface{} {
	return cr.records[i]
}

//返回有多少条数据
func (cr *CsvRecord) NumRecrod() int {
	return len(cr.records)
}

//不存在返回 nil
// fieldName 为csv表中 与 struct tag 中标注为 “index:true" 对应的名字
func (cr *CsvRecord) Index(fieldName string, key interface{}) interface{} {
	index, ok := cr.indexesMap[fieldName]
	if !ok {
		return nil
	}
	return index[key]
}
