package filestore

import (
	"fmt"
	"github.com/gauss-project/aurorafs/pkg/boson"
	"reflect"
	"sort"
	"strconv"
)

const (
	ASC  = "asc"
	DESC = "desc"
)

func sortFile(list []FileView, sortName, sortType string) (sortList []FileView) {
	if len(list) == 0 {
		return
	}

	sortFunc := func(frontValue, backValue interface{}, sortType string) bool {
		switch frontValue.(type) {
		case string:
			return frontValue.(string) > backValue.(string)
		case boson.Address:
			if sortType == ASC {
				return frontValue.(boson.Address).String() < backValue.(boson.Address).String()
			} else {
				return frontValue.(boson.Address).String() > backValue.(boson.Address).String()
			}
		case int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64:
			frontStr := fmt.Sprintf("%d", frontValue)
			backStr := fmt.Sprintf("%d", backValue)

			frontInt, err := strconv.ParseInt(frontStr, 10, 64)
			if err != nil {
				return false
			}
			backInt, err := strconv.ParseInt(backStr, 10, 64)
			if err != nil {
				return false
			}
			if sortType == ASC {
				return frontInt < backInt
			} else {
				return frontInt > backInt
			}

		case float32:
			if sortType == ASC {
				return frontValue.(float32) < backValue.(float32)
			} else {
				return frontValue.(float32) > backValue.(float32)
			}
		case float64:
			if sortType == ASC {
				return frontValue.(float64) < backValue.(float64)
			} else {
				return frontValue.(float64) > backValue.(float64)
			}
		case bool:
			i := 0
			j := 0
			if frontValue.(bool) {
				i = 1
			}
			if backValue.(bool) {
				j = 1
			}
			if sortType == ASC {
				return i < j
			} else {
				return i > j
			}
		default:
			return true
		}
	}

	if sortName != "" && sortType != "" {
		sort.Slice(list, func(i, j int) bool {
			defer func() {
				recover()
			}()
			iv := reflect.ValueOf(list[i]).FieldByName(sortName).Interface()
			jv := reflect.ValueOf(list[j]).FieldByName(sortName).Interface()
			return sortFunc(iv, jv, sortType)
		})
	}
	return list
}
