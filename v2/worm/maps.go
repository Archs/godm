// Tideland Go Data Management - Write once read multiple - Maps
//
// Copyright (C) 2012-2014 Frank Mueller / Tideland / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package worm

//--------------------
// IMPORTS
//--------------------

import (
	"fmt"
	"sort"
	"strconv"
)

//--------------------
// BOOL MAP
//--------------------

// BoolMapValues is a string/bool map for data exchange with an according map.
type BoolMapValues map[string]bool

// BoolMap stores string keys and bool values.
type BoolMap struct {
	values BoolMapValues
}

// NewBoolMap creates a new bool map with the given values.
func NewBoolMap(values BoolMapValues) BoolMap {
	bm := BoolMap{BoolMapValues{}}
	if values != nil {
		for key, value := range values {
			bm.values[key] = value
		}
	}
	return bm
}

// Len returns the length of the bool map.
func (bm BoolMap) Len() int {
	return len(bm.values)
}

// Get returns the value of key in the bool map. If the key doesn't
// exist the default bool (false) is returned.
func (bm BoolMap) Get(key string) bool {
	return bm.values[key]
}

// Keys returns the sorted keys of the bool map.
func (bm BoolMap) Keys() []string {
	keys := make([]string, len(bm.values))
	idx := 0
	for key := range bm.values {
		keys[idx] = key
		idx++
	}
	sort.Strings(keys)
	return keys
}

// ContainsKeys tests if all the passed keys are in the bool map.
func (bm BoolMap) ContainsKeys(keys ...string) bool {
	for _, key := range keys {
		if _, ok := bm.values[key]; !ok {
			return false
		}
	}
	return true
}

// Copy creates a new bool map and copies the values of the keys to it.
func (bm BoolMap) Copy(keys ...string) BoolMap {
	copied := BoolMapValues{}
	for _, key := range keys {
		if value, ok := bm.values[key]; ok {
			copied[key] = value
		}
	}
	return NewBoolMap(copied)
}

// CopyAll creates a new bool map and copies all values to it.
func (bm BoolMap) CopyAll() BoolMap {
	return NewBoolMap(bm.values)
}

// CopyAllValues returns a copy of the values of the bool map.
func (bm BoolMap) CopyAllValues() BoolMapValues {
	values := BoolMapValues{}
	for key, value := range bm.values {
		values[key] = value
	}
	return values
}

// Apply creates a new bool map with all passed values and those
// of this map which are not in the values.
func (bm BoolMap) Apply(values BoolMapValues) BoolMap {
	applied := NewBoolMap(values)
	for key, value := range bm.values {
		if _, ok := applied.values[key]; !ok {
			applied.values[key] = value
		}
	}
	return applied
}

//--------------------
// INT MAP
//--------------------

// IntMapValues is a string/int map for data exchange with an according map.
type IntMapValues map[string]int

// IntMap stores string keys and int values.
type IntMap struct {
	values IntMapValues
}

// NewIntMap creates a new int map with the given values.
func NewIntMap(values IntMapValues) IntMap {
	im := IntMap{IntMapValues{}}
	if values != nil {
		for key, value := range values {
			im.values[key] = value
		}
	}
	return im
}

// Len returns the length of the int map.
func (im IntMap) Len() int {
	return len(im.values)
}

// Get returns the value of key in the int map. If the key doesn't
// exist the default int (0) is returned.
func (im IntMap) Get(key string) int {
	return im.values[key]
}

// Keys returns the sorted keys of the int map.
func (im IntMap) Keys() []string {
	keys := make([]string, len(im.values))
	idx := 0
	for key := range im.values {
		keys[idx] = key
		idx++
	}
	sort.Strings(keys)
	return keys
}

// ContainsKeys tests if all the passed keys are in the int map.
func (im IntMap) ContainsKeys(keys ...string) bool {
	for _, key := range keys {
		if _, ok := im.values[key]; !ok {
			return false
		}
	}
	return true
}

// Copy creates a new int map and copies the values of the keys to it.
func (im IntMap) Copy(keys ...string) IntMap {
	copied := IntMapValues{}
	for _, key := range keys {
		if value, ok := im.values[key]; ok {
			copied[key] = value
		}
	}
	return NewIntMap(copied)
}

// CopyAll creates a new bool map and copies all values to it.
func (im IntMap) CopyAll() IntMap {
	return NewIntMap(im.values)
}

// CopyAllValues returns a copy of the values of the int map.
func (im IntMap) CopyAllValues() IntMapValues {
	values := IntMapValues{}
	for key, value := range im.values {
		values[key] = value
	}
	return values
}

// Apply creates a new int map with all passed values and those
// of this map which are not in the values.
func (im IntMap) Apply(values IntMapValues) IntMap {
	applied := NewIntMap(values)
	for key, value := range im.values {
		if _, ok := applied.values[key]; !ok {
			applied.values[key] = value
		}
	}
	return applied
}

//--------------------
// STRING MAP
//--------------------

// StringMapValues is a string/string map for data exchange with an according map.
type StringMapValues map[string]string

// StringMap stores string keys and string values.
type StringMap struct {
	values StringMapValues
}

// NewStringMap creates a new string map with the given values.
func NewStringMap(values StringMapValues) StringMap {
	sm := StringMap{StringMapValues{}}
	if values != nil {
		for key, value := range values {
			sm.values[key] = value
		}
	}
	return sm
}

// Len returns the length of the string map.
func (sm StringMap) Len() int {
	return len(sm.values)
}

// Get returns the value of key in the string map. If the key doesn't
// exist the default string ("") is returned.
func (sm StringMap) Get(key string) string {
	return sm.values[key]
}

// Keys returns the sorted keys of the string map.
func (sm StringMap) Keys() []string {
	keys := make([]string, len(sm.values))
	idx := 0
	for key := range sm.values {
		keys[idx] = key
		idx++
	}
	sort.Strings(keys)
	return keys
}

// ContainsKeys tests if all the passed keys are in the string map.
func (sm StringMap) ContainsKeys(keys ...string) bool {
	for _, key := range keys {
		if _, ok := sm.values[key]; !ok {
			return false
		}
	}
	return true
}

// Copy creates a new string map and copies the values of the keys to it.
func (sm StringMap) Copy(keys ...string) StringMap {
	copied := StringMapValues{}
	for _, key := range keys {
		if value, ok := sm.values[key]; ok {
			copied[key] = value
		}
	}
	return NewStringMap(copied)
}

// CopyAll creates a new string map and copies all values to it.
func (sm StringMap) CopyAll() StringMap {
	return NewStringMap(sm.values)
}

// CopyAllValues returns a copy of the values of the string map.
func (sm StringMap) CopyAllValues() StringMapValues {
	values := StringMapValues{}
	for key, value := range sm.values {
		values[key] = value
	}
	return values
}

// Apply creates a new string map with all passed values and those
// of this map which are not in the values.
func (sm StringMap) Apply(values StringMapValues) StringMap {
	applied := NewStringMap(values)
	for key, value := range sm.values {
		if _, ok := applied.values[key]; !ok {
			applied.values[key] = value
		}
	}
	return applied
}

//--------------------
// MULTI MAP
//--------------------

// MultiMapValues is a string/interface{} map for data exchange with an according map.
type MultiMapValues map[string]interface{}

// MultiMap stores string keys and string values.
type MultiMap struct {
	values MultiMapValues
}

// NewMultiMap creates a new string map with the given values. Allowed are
// only bool, int64, float64 and string. As a convenience ints are added as
// int64, all other types are added as string.
func NewMultiMap(values MultiMapValues) MultiMap {
	mm := MultiMap{MultiMapValues{}}
	if values != nil {
		for key, value := range values {
			switch inputValue := value.(type) {
			case bool, int64, float64, string:
				mm.values[key] = inputValue
			case int:
				mm.values[key] = int64(inputValue)
			default:
				mm.values[key] = fmt.Sprintf("%v", inputValue)
			}
		}
	}
	return mm
}

// Len returns the length of the multi map.
func (mm MultiMap) Len() int {
	return len(mm.values)
}

// GetBool returns the bool value of key in the multi map. If the key doesn't
// exist the default bool (false) is returned. An integer or float value
// lower or equal zero is interpreted as false, else true and a string is
// parsed with strconv.ParseBool.
func (mm MultiMap) GetBool(key string) bool {
	switch value := mm.values[key].(type) {
	case bool:
		return value
	case int64:
		return value > 0
	case float64:
		return value > 0.0
	case string:
		boolValue, _ := strconv.ParseBool(value)
		return boolValue
	default:
		return false
	}
}

// GetInt64 returns the int64 value of key in the multi map. If the key doesn't
// exist the default int64 (0) is returned. An float value is casted, a bool
// false is returned as 0, as true as 1 and a string is interpreted with
// strconv.ParseInt.
func (mm MultiMap) GetInt64(key string) int64 {
	switch value := mm.values[key].(type) {
	case bool:
		if value {
			return 1
		}
		return 0
	case int64:
		return value
	case float64:
		return int64(value)
	case string:
		intValue, _ := strconv.ParseInt(value, 10, 0)
		return intValue
	default:
		return 0
	}
}

// GetInt is a convenience accessor acting like GetInt64 but casting
// the result into int.
func (mm MultiMap) GetInt(key string) int {
	return int(mm.GetInt64(key))
}

// GetFloat64 returns the float64 value of key in the multi map. If the key doesn't
// exist the default float64 (0.0) is returned. An float value is casted, a bool
// false is returned as 0.0, as true as 1.0 and a string is interpreted with
// strconv.ParseFloat.
func (mm MultiMap) GetFloat64(key string) float64 {
	switch value := mm.values[key].(type) {
	case bool:
		if value {
			return 1.0
		}
		return 0.0
	case int64:
		return float64(value)
	case float64:
		return value
	case string:
		floatValue, _ := strconv.ParseFloat(value, 64)
		return floatValue
	default:
		return 0.0
	}
}

// GetString returns the string value of key in the multi map. If the key doesn't
// exist the default string ("") is returned. Non-strings are returned using Sprintf.
func (mm MultiMap) GetString(key string) string {
	switch value := mm.values[key].(type) {
	case string:
		return value
	case bool, int64, float64:
		return fmt.Sprintf("%v", value)
	default:
		return ""
	}
}

// Keys returns the sorted keys of the multi map.
func (mm MultiMap) Keys() []string {
	keys := make([]string, len(mm.values))
	idx := 0
	for key := range mm.values {
		keys[idx] = key
		idx++
	}
	sort.Strings(keys)
	return keys
}

// ContainsKeys tests if all the passed keys are in the string map.
func (mm MultiMap) ContainsKeys(keys ...string) bool {
	for _, key := range keys {
		if _, ok := mm.values[key]; !ok {
			return false
		}
	}
	return true
}

// Copy creates a new multi map and copies the values of the keys to it.
func (mm MultiMap) Copy(keys ...string) MultiMap {
	copied := MultiMapValues{}
	for _, key := range keys {
		if value, ok := mm.values[key]; ok {
			copied[key] = value
		}
	}
	return NewMultiMap(copied)
}

// CopyAll creates a new multi map and copies all values to it.
func (mm MultiMap) CopyAll() MultiMap {
	return NewMultiMap(mm.values)
}

// CopyAllValues returns a copy of the values of the multi map.
func (mm MultiMap) CopyAllValues() MultiMapValues {
	values := MultiMapValues{}
	for key, value := range mm.values {
		values[key] = value
	}
	return values
}

// Apply creates a new multi map with all passed values and those
// of this map which are not in the values.
func (mm MultiMap) Apply(values MultiMapValues) MultiMap {
	applied := NewMultiMap(values)
	for key, value := range mm.values {
		if _, ok := applied.values[key]; !ok {
			applied.values[key] = value
		}
	}
	return applied
}

// EOF
