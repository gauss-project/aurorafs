package filestore

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

const (
	EQ = "eq"
	NE = "ne"
)

const (
	GT = "gt"
	GE = "ge"
	IT = "it"
	LE = "le"
	IN = "in"
)

const (
	CN = "cn"
	NC = "nc"
	HP = "hp"
	HS = "hs"
)

func filterFile(list map[string]FileView, filter []Filter) (filterList []FileView) {
	filterFunc := func(file FileView, filter Filter) bool {
		defer func() {
			recover()
		}()
		value := reflect.ValueOf(file).FieldByName(filter.Key).Interface()
		v, _ := filterCalculate(value, filter)
		return v
	}

	filterList = make([]FileView, 0, len(list))
	for _, v := range list {
		isFilter := false
		for _, f := range filter {
			ok := filterFunc(v, f)
			if !ok {
				isFilter = true
			}
		}
		if !isFilter {
			filterList = append(filterList, v)
		}
	}

	return filterList
}

func filterCalculate(data interface{}, filter Filter) (bool, error) {

	switch filter.Term {
	case EQ:
		if reflect.TypeOf(data).Kind() == reflect.TypeOf(filter.Value).Kind() {
			return data == filter.Value, nil
		} else {
			dataStr := fmt.Sprintf("%v", data)
			return dataStr == filter.Value, nil
		}
	case NE:
		if reflect.TypeOf(data).Kind() == reflect.TypeOf(filter.Value).Kind() {
			return data != filter.Value, nil
		} else {
			dataStr := fmt.Sprintf("%v", data)
			return dataStr != filter.Value, nil
		}
	case GT:
		value, err := strconv.ParseFloat(filter.Value, 64)
		if err != nil {
			return false, fmt.Errorf("The value [%s] cannot see > operation ", filter.Value)
		}
		if reflect.TypeOf(data).Kind() == reflect.TypeOf(value).Kind() {
			return data.(float64) > value, nil
		} else {
			f, err := toFloat(data)
			if err != nil {
				return false, nil
			}
			return f > value, nil
		}
	case GE:
		value, err := strconv.ParseFloat(filter.Value, 64)
		if err != nil {
			return false, fmt.Errorf("The value [%s] cannot see >= operation ", filter.Value)
		}
		if reflect.TypeOf(data).Kind() == reflect.TypeOf(value).Kind() {
			return data.(float64) >= value, nil
		} else {
			f, err := toFloat(data)
			if err != nil {
				return false, nil
			}
			return f >= value, nil
		}
	case IT:
		value, err := strconv.ParseFloat(filter.Value, 64)
		if err != nil {
			return false, fmt.Errorf("The value [%s] cannot see < operation ", filter.Value)
		}
		if reflect.TypeOf(data).Kind() == reflect.TypeOf(value).Kind() {
			return data.(float64) < value, nil
		} else {
			f, err := toFloat(data)
			if err != nil {
				return false, nil
			}
			return f < value, nil
		}
	case LE:
		value, err := strconv.ParseFloat(filter.Value, 64)
		if err != nil {
			return false, fmt.Errorf("The value [%s] cannot see <= operation ", filter.Value)
		}
		if reflect.TypeOf(data).Kind() == reflect.TypeOf(value).Kind() {
			return data.(float64) <= value, nil
		} else {
			f, err := toFloat(data)
			if err != nil {
				return false, nil
			}
			return f <= value, nil
		}
	case IN:
		valueArry := strings.Split(filter.Value, ",")
		if len(valueArry) != 2 {
			return false, fmt.Errorf("Please pass in the query range correctly ")
		}
		star, err := strconv.ParseFloat(valueArry[0], 64)
		if err != nil {
			return false, fmt.Errorf("The value [%s] cannot see in operation ", valueArry[0])
		}
		end, err := strconv.ParseFloat(valueArry[1], 64)
		if err != nil {
			return false, fmt.Errorf("The value [%s] cannot see in operation ", valueArry[1])
		}
		f, err := toFloat(data)
		if err != nil {
			return false, nil
		}
		if f >= star && f <= end {
			return true, nil
		} else {
			return false, nil
		}
	case CN:
		sValue := fmt.Sprintf("%v", data)
		return strings.Contains(sValue, filter.Value), nil
	case NC:
		sValue := fmt.Sprintf("%v", data)
		return !strings.Contains(sValue, filter.Value), nil
	case HP:
		sValue := fmt.Sprintf("%v", data)
		return strings.HasPrefix(sValue, filter.Value), nil
	case HS:
		sValue := fmt.Sprintf("%v", data)
		return strings.HasSuffix(sValue, filter.Value), nil
	default:
		return false, nil
	}
}

func toFloat(filedValue interface{}) (float64, error) {
	switch val := filedValue.(type) {

	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		v := fmt.Sprintf("%d", val)
		floatV, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("The value [%s] Unable to convert to float ", filedValue)
		}
		return floatV, nil
	case string:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return 0, fmt.Errorf("The value [%s] Can't turn into float ", filedValue)
		}
		return f, nil
	case bool:
		if val {
			return 1, nil
		} else {
			return 0, nil
		}
	default:
		return 0, fmt.Errorf("The value [%s] Can't turn into float ", filedValue)
	}
}
