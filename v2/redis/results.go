// Tideland Go Data Management - Redis Client - Results
//
// Copyright (C) 2009-2014 Frank Mueller / Tideland / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package redis

//--------------------
// IMPORTS
//--------------------

import (
	"strconv"
	"strings"

	"github.com/tideland/goas/v3/errors"
)

//--------------------
// VALUE
//--------------------

// Value is simply a byte slice.
type Value []byte

// String returns the value as string (alternative to type conversion).
func (v Value) String() string {
	if v == nil {
		return "nil"
	}
	return string([]byte(v))
}

// Bool return the value as bool.
func (v Value) Bool() (bool, error) {
	b, err := strconv.ParseBool(v.String())
	if err != nil {
		return false, errors.Annotate(err, ErrInvalidType, errorMessages, v.String(), "bool")
	}
	return b, nil
}

// Int returns the value as int.
func (v Value) Int() (int, error) {
	i, err := strconv.Atoi(v.String())
	if err != nil {
		return 0, errors.Annotate(err, ErrInvalidType, errorMessages, v.String(), "int")
	}
	return i, nil
}

// Int64 returns the value as int64.
func (v Value) Int64() (int64, error) {
	i, err := strconv.ParseInt(v.String(), 10, 64)
	if err != nil {
		return 0, errors.Annotate(err, ErrInvalidType, errorMessages, v.String(), "int64")
	}
	return i, nil
}

// Uint64 returns the value as uint64.
func (v Value) Uint64() (uint64, error) {
	i, err := strconv.ParseUint(v.String(), 10, 64)
	if err != nil {
		return 0, errors.Annotate(err, ErrInvalidType, errorMessages, v.String(), "uint64")
	}
	return i, nil
}

// Float64 returns the value as float64.
func (v Value) Float64() (float64, error) {
	f, err := strconv.ParseFloat(v.String(), 64)
	if err != nil {
		return 0.0, errors.Annotate(err, ErrInvalidType, errorMessages, v.String(), "float64")
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

//--------------------
// KEY/VALUE
//--------------------

// KeyValue combines a key and a value
type KeyValue struct {
	Key   string
	Value Value
}

// KeyValues is a set of KeyValues.
type KeyValues []KeyValue

//--------------------
// RESULT SET
//--------------------

// ResultSet is a set of values.
type ResultSet []Value

// ResultSets is a slice of result sets.
type ResultSets []ResultSet

// FirstValue returns the first value.
func (rs ResultSet) FirstValue() Value {
	if len(rs) == 0 {
		return nil
	}
	return rs[0]
}

// KeyValue return the first value as key and the second as value.
func (rs ResultSet) KeyValue() KeyValue {
	return KeyValue{
		Key:   rs[0].String(),
		Value: rs[1],
	}
}

// KeyValues returns the alternating values as key/value slice.
func (rs ResultSet) KeyValues() KeyValues {
	kvs := KeyValues{}
	key := ""
	for idx, value := range rs {
		if idx%2 == 0 {
			key = value.String()
		} else {
			kvs = append(kvs, KeyValue{key, value})
		}
	}
	return kvs
}

// Hash returns the values of the result set as hash.
func (rs ResultSet) Hash() Hash {
	key := ""
	result := make(Hash)
	set := false
	for _, value := range rs {
		if set {
			// Write every second value.
			result.Set(key, value.Bytes())
			set = false
		} else {
			// First value is always a key.
			key = value.String()
			set = true
		}
	}
	return result
}

//--------------------
// RESULT SET FUTURE
//--------------------

// Future just waits for a result set
// returned somewhere in the future.
type Future interface {
	// ResultSet returns the result as result set in the moment it is available.
	ResultSet() (ResultSet, error)

	// ResultSets returns the result as result sets in the moment it is available.
	ResultSets() (ResultSets, error)
}

// future implements Future.
type future struct {
	result chan interface{}
}

// newFuture creates the new future.
func newFuture() *future {
	return &future{make(chan interface{}, 1)}
}

// setResult sets the result.
func (f *future) setResult(result interface{}, err error) {
	if result != nil {
		f.result <- result
	} else {
		f.result <- err
	}
}

// ResultSet returns the result as result set in the moment it is available.
func (f *future) ResultSet() (ResultSet, error) {
	result := <-f.result
	switch typedResult := result.(type) {
	case ResultSet:
		return typedResult, nil
	case error:
		return nil, typedResult
	}
	return nil, errors.New(ErrFuture, errorMessages, result)
}

// ResultSets returns the result as result sets in the moment it is available.
func (f *future) ResultSets() (ResultSets, error) {
	result := <-f.result
	switch typedResult := result.(type) {
	case ResultSets:
		return typedResult, nil
	case error:
		return nil, typedResult
	}
	return nil, errors.New(ErrFuture, errorMessages, result)
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
func (h Hash) Set(k string, v interface{}) {
	h[k] = Value(valueToBytes(v))
}

// String returns the value of a key as string.
func (h Hash) String(k string) (string, error) {
	if v, ok := h[k]; ok {
		return v.String(), nil
	}
	return "", errors.New(ErrInvalidKey, errorMessages, k)
}

// Bool returns the value of a key as bool.
func (h Hash) Bool(k string) (bool, error) {
	if v, ok := h[k]; ok {
		return v.Bool()
	}
	return false, errors.New(ErrInvalidKey, errorMessages, k)
}

// Int returns the value of a key as int.
func (h Hash) Int(k string) (int, error) {
	if v, ok := h[k]; ok {
		return v.Int()
	}
	return 0, errors.New(ErrInvalidKey, errorMessages, k)
}

// Int64 returns the value of a key as int64.
func (h Hash) Int64(k string) (int64, error) {
	if v, ok := h[k]; ok {
		return v.Int64()
	}
	return 0, errors.New(ErrInvalidKey, errorMessages, k)
}

// Uint64 returns the value of a key as uint64.
func (h Hash) Uint64(k string) (uint64, error) {
	if v, ok := h[k]; ok {
		return v.Uint64()
	}
	return 0, errors.New(ErrInvalidKey, errorMessages, k)
}

// Float64 returns the value of a key as float64.
func (h Hash) Float64(k string) (float64, error) {
	if v, ok := h[k]; ok {
		return v.Float64()
	}
	return 0.0, errors.New(ErrInvalidKey, errorMessages, k)
}

// Bytes returns the value of a key as byte slice.
func (h Hash) Bytes(k string) []byte {
	if v, ok := h[k]; ok {
		return v.Bytes()
	}
	return []byte{}
}

// StringSlice returns the value of a key as string slice.
func (h Hash) StringSlice(k string) []string {
	if v, ok := h[k]; ok {
		return v.StringSlice()
	}
	return []string{}
}

// StringMap returns the value of a key as string map.
func (h Hash) StringMap(k string) map[string]string {
	if v, ok := h[k]; ok {
		return v.StringMap()
	}
	return map[string]string{}
}

// Hashable represents types for Redis hashes.
type Hashable interface {
	GetHash() Hash
	SetHash(h Hash)
}

//--------------------
// PUBLISHED VALUE
//--------------------

// PublishedValue is a published value plus
// channel pattern and channel.
type PublishedValue interface {
	// Value returns the published value itself.
	Value() Value

	// ChannelPattern returns the channel pattern the value was
	// published to, my be "*".
	ChannelPattern() string

	// Channel returns the concrete channel.
	Channel() string
}

// publishedValue implements the PublishedValue interface.
type publishedValue struct {
	value          Value
	channelPattern string
	channel        string
}

func (p *publishedValue) Value() Value {
	return p.value
}

func (p *publishedValue) ChannelPattern() string {
	return p.channelPattern
}

func (p *publishedValue) Channel() string {
	return p.channel
}

// EOF
