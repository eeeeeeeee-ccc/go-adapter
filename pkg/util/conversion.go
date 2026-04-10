package util

import (
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
)

func AnyToString(v any) (string, error) {
	if v == nil {
		return "", errors.New("nil value")
	}

	switch val := v.(type) {

	// 字符串类型
	case string:
		return val, nil
	// 布尔类型
	case bool:
		return strconv.FormatBool(val), nil
	case json.Number:
		return val.String(), nil
	// 整数类型
	case int:
		return strconv.FormatInt(int64(val), 10), nil
	case int8:
		return strconv.FormatInt(int64(val), 10), nil
	case int16:
		return strconv.FormatInt(int64(val), 10), nil
	case int64:
		return strconv.FormatInt(val, 10), nil
	// 无符号整数类型
	case uint:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint64:
		return strconv.FormatUint(val, 10), nil
	case uintptr:
		return strconv.FormatUint(uint64(val), 10), nil
	// 浮点数类型
	case float32:
		return strconv.FormatFloat(float64(val), 'g', -1, 32), nil
	case float64:
		return strconv.FormatFloat(val, 'g', -1, 64), nil
	// 复数类型
	case complex64:
		return strconv.FormatComplex(complex128(val), 'g', -1, 64), nil
	case complex128:
		return strconv.FormatComplex(val, 'g', -1, 128), nil
	// 字符类型
	case rune:
		return string(val), nil
	case byte:
		return string(val), nil
	default:
		return "", errors.New("unsupported type: " + reflect.TypeOf(v).String())
	}
}
