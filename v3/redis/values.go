// Tideland Go Data Management - Redis Client - Values
//
// Copyright (C) 2009-2014 Frank Mueller / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package redis

//--------------------
// IMPORTS
//--------------------

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/tideland/goas/v3/errors"
)

//--------------------
// VALUE
//--------------------

// Value is simply a byte slice.
type Value []byte

// NewValue creates a value out of the passed data.
func NewValue(value interface{}) Value {
	return Value(valueToBytes(value))
}

// String returns the value as string (alternative to type conversion).
func (v Value) String() string {
	if v == nil {
		return "(nil)"
	}
	return string([]byte(v))
}

// IsOK returns true if the value is the Redis OK value.
func (v Value) IsOK() bool {
	return v.String() == "OK"
}

// Bool return the value as bool.
func (v Value) Bool() (bool, error) {
	b, err := strconv.ParseBool(v.String())
	if err != nil {
		return false, v.invalidTypeError(err, "bool")
	}
	return b, nil
}

// Int returns the value as int.
func (v Value) Int() (int, error) {
	i, err := strconv.Atoi(v.String())
	if err != nil {
		return 0, v.invalidTypeError(err, "int")
	}
	return i, nil
}

// Int64 returns the value as int64.
func (v Value) Int64() (int64, error) {
	i, err := strconv.ParseInt(v.String(), 10, 64)
	if err != nil {
		return 0, v.invalidTypeError(err, "int64")
	}
	return i, nil
}

// Uint64 returns the value as uint64.
func (v Value) Uint64() (uint64, error) {
	i, err := strconv.ParseUint(v.String(), 10, 64)
	if err != nil {
		return 0, v.invalidTypeError(err, "uint64")
	}
	return i, nil
}

// Float64 returns the value as float64.
func (v Value) Float64() (float64, error) {
	f, err := strconv.ParseFloat(v.String(), 64)
	if err != nil {
		return 0.0, v.invalidTypeError(err, "float64")
	}
	return f, nil
}

// Bytes returns the value as byte slice.
func (v Value) Bytes() []byte {
	return []byte(v)
}

// StringSlice returns the value as slice of strings when seperated by CRLF.
func (v Value) StringSlice() []string {
	return strings.Split(v.String(), "\r\n")
}

// StringMap returns the value as a map of strings when seperated by CRLF
// and colons between key and value.
func (v Value) StringMap() map[string]string {
	tmp := v.StringSlice()
	m := make(map[string]string, len(tmp))
	for _, s := range tmp {
		kv := strings.Split(s, ":")
		if len(kv) > 1 {
			m[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}
	return m
}

// Unpack removes the braces of a list value.
func (v Value) Unpack() Value {
	if len(v) > 2 && v[0] == '[' && v[len(v)-1] == ']' {
		return Value(v[1 : len(v)-1])
	}
	return v
}

// invalidTypeError returns an annotated error if a value access has
// been unsuccessful.
func (v Value) invalidTypeError(err error, descr string) error {
	return errors.Annotate(err, ErrInvalidType, errorMessages, v.String(), descr)
}

//--------------------
// ARRAY
//--------------------

// ResultSet contains a number of values or nested result sets.
type ResultSet struct {
	parent *ResultSet
	items  []interface{}
	length int
}

// newResultSet creates a new result set.
func newResultSet() *ResultSet {
	return &ResultSet{nil, []interface{}{}, 1}
}

// append adds a value/result set to the result set. It panics if it's
// neither a value, even as a byte slice, nor an array.
func (rs *ResultSet) append(item interface{}) {
	switch i := item.(type) {
	case Value, *ResultSet:
		rs.items = append(rs.items, i)
	case []byte:
		rs.items = append(rs.items, Value(i))
	case ResultSet:
		rs.items = append(rs.items, &i)
	default:
		panic("illegal result set item type")
	}
}

// allReceived answers with true if all expected items are received.
func (rs *ResultSet) allReceived() bool {
	return len(rs.items) >= rs.length
}

// nextResultSet returns the parent stack upwards as long as all expected
// items are received.
func (rs *ResultSet) nextResultSet() *ResultSet {
	if !rs.allReceived() {
		return rs
	}
	if rs.parent == nil {
		return nil
	}
	return rs.parent.nextResultSet()
}

// Len returns the number of items in the result set.
func (rs *ResultSet) Len() int {
	return len(rs.items)
}

// ValueAt returns the value at index.
func (rs *ResultSet) ValueAt(index int) (Value, error) {
	if len(rs.items) < index-1 {
		return nil, errors.New(ErrIllegalItemIndex, errorMessages, index, len(rs.items))
	}
	value, ok := rs.items[index].(Value)
	if !ok {
		return nil, errors.New(ErrIllegalItemType, errorMessages, index, "value")
	}
	return value, nil
}

// BoolAt returns the value at index as bool. This is a convenience
// method as the bool is needed very often.
func (rs *ResultSet) BoolAt(index int) (bool, error) {
	value, err := rs.ValueAt(index)
	if err != nil {
		return false, err
	}
	return value.Bool()
}

// IntAt returns the value at index as int. This is a convenience
// method as the integer is needed very often.
func (rs *ResultSet) IntAt(index int) (int, error) {
	value, err := rs.ValueAt(index)
	if err != nil {
		return 0, err
	}
	return value.Int()
}

// StringAt returns the value at index as string. This is a convenience
// method as the string is needed very often.
func (rs *ResultSet) StringAt(index int) (string, error) {
	value, err := rs.ValueAt(index)
	if err != nil {
		return "", err
	}
	return value.String(), nil
}

// ResultSetAt returns the nested result set at index.
func (rs *ResultSet) ResultSetAt(index int) (*ResultSet, error) {
	if len(rs.items) < index-1 {
		return nil, errors.New(ErrIllegalItemIndex, errorMessages, index, len(rs.items))
	}
	resultSet, ok := rs.items[index].(*ResultSet)
	if !ok {
		return nil, errors.New(ErrIllegalItemType, errorMessages, index, "result set")
	}
	return resultSet, nil
}

// Values returnes a flattened list of all values.
func (rs *ResultSet) Values() []Value {
	values := []Value{}
	for _, item := range rs.items {
		switch i := item.(type) {
		case Value:
			values = append(values, i)
		case *ResultSet:
			values = append(values, i.Values()...)
		}
	}
	return values
}

// KeyValues returns the alternating values as key/value slice.
func (rs *ResultSet) KeyValues() (KeyValues, error) {
	kvs := KeyValues{}
	key := ""
	for index, item := range rs.items {
		value, ok := item.(Value)
		if !ok {
			return nil, errors.New(ErrIllegalItemType, errorMessages, index, "value")
		}
		if index%2 == 0 {
			key = value.String()
		} else {
			kvs = append(kvs, KeyValue{key, value})
		}
	}
	return kvs, nil
}

// Hash returns the values of the result set as hash.
func (rs *ResultSet) Hash() (Hash, error) {
	hash := make(Hash)
	key := ""
	for index, item := range rs.items {
		value, ok := item.(Value)
		if !ok {
			return nil, errors.New(ErrIllegalItemType, errorMessages, index, "value")
		}
		if index%2 == 0 {
			key = value.String()
		} else {
			hash.Set(key, value.Bytes())
		}
	}
	return hash, nil
}

// Strings returns all values/arrays of the array as a slice of strings.
func (rs *ResultSet) Strings() []string {
	ss := make([]string, len(rs.items))
	for index, item := range rs.items {
		s, ok := item.(fmt.Stringer)
		if !ok {
			// Must not happen!
			panic("illegal type in array")
		}
		ss[index] = s.String()
	}
	return ss
}

// String returns the result set in a human readable form.
func (rs *ResultSet) String() string {
	out := "RESULT SET ("
	ss := rs.Strings()
	return out + strings.Join(ss, " / ") + ")"
}

//--------------------
// KEY/VALUE
//--------------------

// KeyValue combines a key and a value
type KeyValue struct {
	Key   string
	Value Value
}

// String returs the key/value pair as string.
func (kv KeyValue) String() string {
	return fmt.Sprintf("%s = %v", kv.Key, kv.Value)
}

// KeyValues is a set of KeyValues.
type KeyValues []KeyValue

// Len returns the number of keys and values in the set.
func (kvs KeyValues) Len() int {
	return len(kvs)
}

// String returs the key/value pairs as string.
func (kvs KeyValues) String() string {
	kvss := []string{}
	for _, kv := range kvs {
		kvss = append(kvss, kv.String())
	}
	return fmt.Sprintf("[%s]", strings.Join(kvss, " / "))
}

//--------------------
// HASH
//--------------------

// Hash maps multiple fields of a hash to the
// according result values.
type Hash map[string]Value

// NewHash creates a new empty hash.
func NewHash() Hash {
	return make(Hash)
}

// Len returns the number of elements in the hash.
func (h Hash) Len() int {
	return len(h)
}

// Set sets a key to the given value.
func (h Hash) Set(key string, value interface{}) Hash {
	h[key] = Value(valueToBytes(value))
	return h
}

// String returns the value of a key as string.
func (h Hash) String(key string) (string, error) {
	if value, ok := h[key]; ok {
		return value.String(), nil
	}
	return "", errors.New(ErrInvalidKey, errorMessages, key)
}

// Bool returns the value of a key as bool.
func (h Hash) Bool(key string) (bool, error) {
	if value, ok := h[key]; ok {
		return value.Bool()
	}
	return false, errors.New(ErrInvalidKey, errorMessages, key)
}

// Int returns the value of a key as int.
func (h Hash) Int(key string) (int, error) {
	if value, ok := h[key]; ok {
		return value.Int()
	}
	return 0, errors.New(ErrInvalidKey, errorMessages, key)
}

// Int64 returns the value of a key as int64.
func (h Hash) Int64(key string) (int64, error) {
	if value, ok := h[key]; ok {
		return value.Int64()
	}
	return 0, errors.New(ErrInvalidKey, errorMessages, key)
}

// Uint64 returns the value of a key as uint64.
func (h Hash) Uint64(key string) (uint64, error) {
	if value, ok := h[key]; ok {
		return value.Uint64()
	}
	return 0, errors.New(ErrInvalidKey, errorMessages, key)
}

// Float64 returns the value of a key as float64.
func (h Hash) Float64(key string) (float64, error) {
	if value, ok := h[key]; ok {
		return value.Float64()
	}
	return 0.0, errors.New(ErrInvalidKey, errorMessages, key)
}

// Bytes returns the value of a key as byte slice.
func (h Hash) Bytes(key string) []byte {
	if value, ok := h[key]; ok {
		return value.Bytes()
	}
	return []byte{}
}

// StringSlice returns the value of a key as string slice.
func (h Hash) StringSlice(key string) []string {
	if value, ok := h[key]; ok {
		return value.StringSlice()
	}
	return []string{}
}

// StringMap returns the value of a key as string map.
func (h Hash) StringMap(key string) map[string]string {
	if value, ok := h[key]; ok {
		return value.StringMap()
	}
	return map[string]string{}
}

// Hashable represents types for Redis hashes.
type Hashable interface {
	Len() int
	GetHash() Hash
	SetHash(h Hash)
}

//--------------------
// PUBLISHED VALUE
//--------------------

// PublishedValue contains a published value and its channel
// channel pattern.
type PublishedValue struct {
	value          Value
	channelPattern string
	channel        string
}

// newPublishedValue creates a published value out of the
// passed array.
func newPublishedValue(result *ResultSet) (*PublishedValue, error) {
	v1, err := result.ValueAt(1)
	if err != nil {
		return nil, err
	}
	v2, err := result.ValueAt(2)
	if err != nil {
		return nil, err
	}
	v3, err := result.ValueAt(3)
	if err != nil {
		return nil, err
	}
	switch result.Len() {
	case 3:
		return &PublishedValue{v2, "*", v1.String()}, nil
	case 4:
		return &PublishedValue{v3, v1.String(), v2.String()}, nil
	}
	return nil, errors.New(ErrInvalidResponse, errorMessages, result)
}

// Value returns the published value itself.
func (p *PublishedValue) Value() Value {
	return p.value
}

// ChannelPattern returns the channel pattern the value was
// published to, my be "*".
func (p *PublishedValue) ChannelPattern() string {
	return p.channelPattern
}

// Channel returns the concrete channel.
func (p *PublishedValue) Channel() string {
	return p.channel
}

// EOF
