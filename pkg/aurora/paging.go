package aurora

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/gauss-project/aurorafs/pkg/logging"
)

const (
	EQ = "eq"
	NE = "ne"
)

const (
	GT = "gt"
	Ge = "ge"
	IT = "it"
	LE = "le"
	IN = "in"
)

const (
	CN = "contains"
	NC = "NoContains"
	HP = "HasPrefix"
	HS = "HasSuffix"
)

const (
	ASC  = "asc"
	DESC = "desc"
)

type ApiFilter struct {
	Key   string `json:"key"`
	Term  string `json:"term"`
	Value string `json:"value"`
}

type ApiSort struct {
	Key   string `json:"key"`
	Order string `json:"order"`
}

type ApiPage struct {
	PageSize int `json:"pageSize"`
	PageNum  int `json:"pageNum"`
}

type ApiBody struct {
	Page   ApiPage     `json:"page"`
	Sort   ApiSort     `json:"sort"`
	Filter []ApiFilter `json:"filter"`
}

type Paging struct {
	page   int
	limit  int
	sort   string
	order  string
	logger logging.Logger
}

func NewPaging(logger logging.Logger, page, limit int, sort string, order string) *Paging {
	return &Paging{
		page:   page,
		limit:  limit,
		sort:   sort,
		order:  order,
		logger: logger,
	}
}

func (pg *Paging) ResponseFilter(list []map[string]interface{}, filterTerm []ApiFilter) (newList []map[string]interface{}) {
	filterFunc := func(mp map[string]interface{}, filter ApiFilter) (bool, error) {
		if fieldVal, ok := mp[filter.Key]; ok {
			return pg.filterCalculate(fieldVal, filter)
		} else {
			pg.logger.Errorf("API-paging-ResponseFilter: The field [%s] does not exist", filter.Key)
			return false, fmt.Errorf("The field [%s] does not exist ", filter.Key)
		}
	}

	isDel := false
	for i := 0; i < len(list); {
		isDel = false
		for _, filter := range filterTerm {
			b, _ := filterFunc(list[i], filter)
			if !b {
				isDel = true
				break
			}
		}
		if isDel {
			list = append(list[:i], list[i+1:]...)
		} else {
			i++
		}
	}

	return list
}

func (pg *Paging) PageSort(list []map[string]interface{}, sortName, sortType string) (newList []map[string]interface{}) {
	if len(list) == 0 {
		return
	}

	sortFunc := func(frontValue, backValue interface{}, sortType string) bool {
		switch frontValue.(type) {
		case string:
			return frontValue.(string) > backValue.(string)
		case int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64:
			frontStr := fmt.Sprintf("%d", frontValue)
			backStr := fmt.Sprintf("%d", backValue)

			frontInt, err := strconv.ParseInt(frontStr, 10, 64)
			if err != nil {
				pg.logger.Errorf("API-paging-PageSort:%v Error in int conversion %v", frontValue, err.Error())
				return false
			}
			backInt, err := strconv.ParseInt(backStr, 10, 64)
			if err != nil {
				pg.logger.Errorf("API-paging-PageSort:%v Error in int conversion %v", backValue, err.Error())
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

	sort.Slice(list, func(i, j int) bool {
		return sortFunc(list[i][sortName], list[j][sortName], sortType)
	})
	return list
}

func (pg *Paging) Page(list []map[string]interface{}) []map[string]interface{} {
	if len(list) == 0 {
		return list
	}
	start := pg.limit * (pg.page - 1)
	if len(list)-start >= pg.limit {
		list = list[start : start+pg.limit]
		return list
	}
	if len(list) < pg.page {
		return nil
	} else {
		list = list[start:]
	}
	return list
}

func (pg *Paging) filterCalculate(data interface{}, filter ApiFilter) (bool, error) {

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
			pg.logger.Error("API-paging: The value [%s] cannot see > operation \n", filter.Value)
			return false, fmt.Errorf("The value [%s] cannot see > operation ", filter.Value)
		}
		if reflect.TypeOf(data).Kind() == reflect.TypeOf(value).Kind() {
			return data.(float64) > value, nil
		} else {
			f, err := pg.toFloat(data)
			if err != nil {
				pg.logger.Error(err)
				return false, nil
			}
			return f > value, nil
		}
	case Ge:
		value, err := strconv.ParseFloat(filter.Value, 64)
		if err != nil {
			pg.logger.Error("API-paging: The value [%s] cannot see >= operation \n", filter.Value)
			return false, fmt.Errorf("The value [%s] cannot see >= operation ", filter.Value)
		}
		if reflect.TypeOf(data).Kind() == reflect.TypeOf(value).Kind() {
			return data.(float64) >= value, nil
		} else {
			f, err := pg.toFloat(data)
			if err != nil {
				pg.logger.Error(err)
				return false, nil
			}
			return f >= value, nil
		}
	case IT:
		value, err := strconv.ParseFloat(filter.Value, 64)
		if err != nil {
			pg.logger.Error("API-paging: The value [%s] cannot see < operation \n", filter.Value)
			return false, fmt.Errorf("The value [%s] cannot see < operation ", filter.Value)
		}
		if reflect.TypeOf(data).Kind() == reflect.TypeOf(value).Kind() {
			return data.(float64) < value, nil
		} else {
			f, err := pg.toFloat(data)
			if err != nil {
				pg.logger.Error(err)
				return false, nil
			}
			return f < value, nil
		}
	case LE:
		value, err := strconv.ParseFloat(filter.Value, 64)
		if err != nil {
			pg.logger.Error("API-paging: The value [%s] cannot see <= operation \n", filter.Value)
			return false, fmt.Errorf("The value [%s] cannot see <= operation ", filter.Value)
		}
		if reflect.TypeOf(data).Kind() == reflect.TypeOf(value).Kind() {
			return data.(float64) <= value, nil
		} else {
			f, err := pg.toFloat(data)
			if err != nil {
				pg.logger.Error(err)
				return false, nil
			}
			return f <= value, nil
		}
	case IN:
		valueArry := strings.Split(filter.Value, ",")
		if len(valueArry) != 2 {
			pg.logger.Error("API-paging: Please pass in the query range correctly.")
			return false, fmt.Errorf("Please pass in the query range correctly ")
		}
		star, err := strconv.ParseFloat(valueArry[0], 64)
		if err != nil {
			pg.logger.Error("API-paging: The value [%s] cannot see in operation ", valueArry[0])
			return false, fmt.Errorf("The value [%s] cannot see in operation ", valueArry[0])
		}
		end, err := strconv.ParseFloat(valueArry[1], 64)
		if err != nil {
			pg.logger.Error("API-paging: The value [%s] cannot see in operation,", valueArry[1])
			return false, fmt.Errorf("The value [%s] cannot see in operation ", valueArry[1])
		}
		f, err := pg.toFloat(data)
		if err != nil {
			pg.logger.Error(err)
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

func (pg *Paging) toFloat(filedValue interface{}) (float64, error) {
	switch val := filedValue.(type) {

	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		v := fmt.Sprintf("%d", val)
		floatV, err := strconv.ParseFloat(v, 64)
		if err != nil {
			pg.logger.Error("API-paging-toFloat: The value [%s] Unable to convert to float. ", filedValue)
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
