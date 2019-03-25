package asc

import (
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/data"
	"strings"
)

func jsonNormalize(source interface{}) interface{} {
	if source == nil {
		return source
	}
	if toolbox.IsMap(source) {
		var aMap = toolbox.AsMap(source)
		for k, v := range aMap {
			aMap[k] = jsonNormalize(v)
		}
		return aMap
	} else if toolbox.IsSlice(source) {
		var aSlice = toolbox.AsSlice(source)
		for i, v := range aSlice {
			aSlice[i] = jsonNormalize(v)
		}
		return aSlice
	}
	return source
}

func AsJSON(source interface{}, state data.Map) (interface{}, error) {
	keyPath := strings.TrimSpace(toolbox.AsString(source))
	val, ok := state.GetValue(keyPath)
	if ok {
		val = jsonNormalize(val)
		return toolbox.AsIndentJSONText(val)
	}
	return nil, nil
}

func AsArray(source interface{}, state data.Map) (interface{}, error) {
	keyPath := strings.TrimSpace(toolbox.AsString(source))
	if val, ok := state.GetValue(keyPath); ok && val != nil {
		if toolbox.IsMap(val) {
			aMap := toolbox.AsMap(val)
			result := []interface{}{}
			for k, v := range aMap {
				result = append(result, map[string]interface{}{
					"key":   k,
					"value": v,
				})
			}
			return result, nil
		}
	}
	return nil, nil
}

func AsTimestamp(config *dsc.Config) func(source interface{}, state data.Map) (interface{}, error) {
	return func(source interface{}, state data.Map) (interface{}, error) {
		keyPath := strings.TrimSpace(toolbox.AsString(source))
		if val, ok := state.GetValue(keyPath); ok {
			return toolbox.ToTime(val, config.Get(DateLayoutKey))
		}
		return nil, nil
	}
}
