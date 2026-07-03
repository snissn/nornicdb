package storage

import (
	"fmt"
	"time"
)

// validatePropertiesForStorage ensures property values are gob-encodable.
// This prevents runtime serialization failures during async flush.
func validatePropertiesForStorage(properties map[string]interface{}) error {
	if len(properties) == 0 {
		return nil
	}
	for key, value := range properties {
		if err := validatePropertyValueForStorage(value); err != nil {
			return fmt.Errorf("invalid property value for key %q: %w", key, err)
		}
	}
	return nil
}

func validatePropertyValueForStorage(value interface{}) error {
	switch value.(type) {
	case nil,
		string,
		bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32,
		float64,
		time.Time:
		return nil
	case []interface{}:
		for i, item := range value.([]interface{}) {
			if err := validatePropertyValueForStorage(item); err != nil {
				return fmt.Errorf("index %d: %w", i, err)
			}
		}
		return nil
	case []string, []int, []int32, []int64, []float32, []float64, []bool:
		return nil
	case map[string]interface{}:
		for key, item := range value.(map[string]interface{}) {
			if err := validatePropertyValueForStorage(item); err != nil {
				return fmt.Errorf("key %q: %w", key, err)
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported property value type %T", value)
	}
}

func normalizePropertyMapShapes(properties map[string]interface{}) {
	for key, value := range properties {
		properties[key] = normalizePropertyValueShape(value)
	}
}

func normalizePropertyValueShape(value interface{}) interface{} {
	switch v := value.(type) {
	case []interface{}:
		return normalizeInterfaceSliceShape(v)
	case map[string]interface{}:
		if v == nil {
			return v
		}
		out := make(map[string]interface{}, len(v))
		for key, item := range v {
			out[key] = normalizePropertyValueShape(item)
		}
		return out
	default:
		return value
	}
}

func normalizeInterfaceSliceShape(values []interface{}) interface{} {
	if len(values) == 0 {
		return values
	}

	if out, ok := interfaceSliceToFloat64(values); ok {
		return out
	}
	if out, ok := interfaceSliceToInt64(values); ok {
		return out
	}
	if out, ok := interfaceSliceToUint64(values); ok {
		return out
	}

	out := make([]interface{}, len(values))
	for i, item := range values {
		out[i] = normalizePropertyValueShape(item)
	}
	return out
}

func interfaceSliceToFloat64(values []interface{}) ([]float64, bool) {
	out := make([]float64, len(values))
	for i, item := range values {
		switch v := item.(type) {
		case float64:
			out[i] = v
		default:
			return nil, false
		}
	}
	return out, true
}

func interfaceSliceToInt64(values []interface{}) ([]int64, bool) {
	out := make([]int64, len(values))
	for i, item := range values {
		switch v := item.(type) {
		case int:
			out[i] = int64(v)
		case int8:
			out[i] = int64(v)
		case int16:
			out[i] = int64(v)
		case int32:
			out[i] = int64(v)
		case int64:
			out[i] = v
		default:
			return nil, false
		}
	}
	return out, true
}

func interfaceSliceToUint64(values []interface{}) ([]uint64, bool) {
	out := make([]uint64, len(values))
	for i, item := range values {
		switch v := item.(type) {
		case uint:
			out[i] = uint64(v)
		case uint8:
			out[i] = uint64(v)
		case uint16:
			out[i] = uint64(v)
		case uint32:
			out[i] = uint64(v)
		case uint64:
			out[i] = v
		default:
			return nil, false
		}
	}
	return out, true
}
