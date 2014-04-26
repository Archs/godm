// Tideland Go Data Management - Redis Client - Commands - Strings
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
	"time"
)

//--------------------
// STRING COMMANDS
//--------------------

// Append creates or appends a value to a key and returns the new
// length of the value.
func (conn *Connection) Append(key string, value interface{}) (int, error) {
	result, err := conn.Command("append", key, value)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// BitCount counts the number of set bits (population counting)
// in a value.
func (conn *Connection) BitCount(key string) (int, error) {
	result, err := conn.Command("bitcount", key)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// BitCountInterval counts the number of set bits (population counting)
// in a value in a defined interval. This can also be defined by negative
// values. In this case the interval is addressed from the end of the
// value.
func (conn *Connection) BitCountInterval(key string, start, end int) (int, error) {
	result, err := conn.Command("bitcount", key, start, end)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// BitOp defines the valid types for bit operations.
type BitOp string

// Definition of the different bit operations.
const (
	BitOpAnd BitOp = "and"
	BitOpOr  BitOp = "or"
	BitOpXOr BitOp = "xor"
	BitOpNot BitOp = "not"
)

// BitOp performs a bitwise operation between multiple keys (containing string
// values) and stores the result in the destination key. It returns the size of
// the string stored in the destination key, that is equal to the size of the
// longest input string.
func (conn *Connection) BitOp(op BitOp, destkey string, keys ...string) (int, error) {
	result, err := conn.Command("bitop", buildInterfaces(op, destkey, keys)...)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// Decr decrements the number stored at key by one.
func (conn *Connection) Decr(key string) (int, error) {
	result, err := conn.Command("decr", key)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// DecrBy decrements the number stored at key by the decrement.
func (conn *Connection) DecrBy(key string, decrement int) (int, error) {
	result, err := conn.Command("decrby", key, decrement)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// Get returns the value of key.
func (conn *Connection) Get(key string) (Value, error) {
	result, err := conn.Command("get", key)
	if err != nil {
		return nil, err
	}
	return result.ValueAt(0)
}

// GetBit returns the bit value at offset in the string value stored at a key.
func (conn *Connection) GetBit(key string, offset int) (bool, error) {
	result, err := conn.Command("getbit", key, offset)
	if err != nil {
		return false, err
	}
	return result.BoolAt(0)
}

// GetRange returns the substring of the string value stored at key, determined
// by the offsets start and end (both are inclusive).
func (conn *Connection) GetRange(key string, start, end int) (Value, error) {
	result, err := conn.Command("getrange", key, start, end)
	if err != nil {
		return nil, err
	}
	return result.ValueAt(0)
}

// GetSet atomically sets key to value and returns the old value stored at a key.
func (conn *Connection) GetSet(key string, value interface{}) (Value, error) {
	result, err := conn.Command("getset", key, value)
	if err != nil {
		return nil, err
	}
	return result.ValueAt(0)
}

// Incr increments the number stored at key by one.
func (conn *Connection) Incr(key string) (int, error) {
	result, err := conn.Command("incr", key)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// IncrBy increments the number stored at key by the increment.
func (conn *Connection) IncrBy(key string, increment int) (int, error) {
	result, err := conn.Command("incrby", key, increment)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// IncrByFloat increments the string representing a floating point number
// stored at a key by the specified increment.
func (conn *Connection) IncrByFloat(key string, increment float64) (float64, error) {
	result, err := conn.Command("incrbyfloat", key, increment)
	if err != nil {
		return 0.0, err
	}
	value, err := result.ValueAt(0)
	if err != nil {
		return 0.0, err
	}
	return value.Float64()
}

// MGet returns the values of all specified keys.
func (conn *Connection) MGet(keys ...string) (*ResultSet, error) {
	result, err := conn.Command("mget", buildInterfaces(keys)...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// MSet sets the given keys to their respective values.
func (conn *Connection) MSet(kvs Hash) error {
	result, err := conn.Command("mset", kvs)
	if err != nil {
		return err
	}
	return checkOK(result, ErrCannotSetKey, keyValueArgsToKeys(kvs))
}

// MSetNX sets the given keys to their respective values like MSet. But it will
// not perform any operation at all even if just a single key already exists.
func (conn *Connection) MSetNX(kvs Hash) error {
	result, err := conn.Command("msetnx", kvs)
	if err != nil {
		return err
	}
	return checkOK(result, ErrCannotSetKey, keyValueArgsToKeys(kvs))
}

// PSetEx sets a key to hold the value and set its TTL to a given number
// of milliseconds.
func (conn *Connection) PSetEx(key string, ttl time.Duration, value interface{}) error {
	milliseconds := ttl.Nanoseconds() / 1000000
	result, err := conn.Command("psetex", key, milliseconds, value)
	if err != nil {
		return err
	}
	return checkOK(result, ErrCannotSetKey, key)
}

// PSetExExists sets a key to hold the value and set its TTL to a given number
// of milliseconds depending on the exists key. It has to exist when set to
// true or must not exist when set to false.
func (conn *Connection) PSetExExists(key string, ttl time.Duration, exists bool, value interface{}) error {
	milliseconds := ttl.Nanoseconds() / 1000000
	flag := map[bool]string{true: "xx", false: "nx"}
	result, err := conn.Command("set", key, value, "px", milliseconds, flag[exists])
	if err != nil {
		return err
	}
	return checkOK(result, ErrCannotSetKey, key)
}

// Set sets key to hold the value.
func (conn *Connection) Set(key string, value interface{}) error {
	result, err := conn.Command("set", key, value)
	if err != nil {
		return err
	}
	return checkOK(result, ErrCannotSetKey, key)
}

// SetBit sets or clears the bit at offset in the string value stored at a key.
func (conn *Connection) SetBit(key string, offset int, value bool) (bool, error) {
	var zeroOne int
	if value {
		zeroOne = 1
	}
	result, err := conn.Command("setbit", key, offset, zeroOne)
	if err != nil {
		return false, err
	}
	return result.BoolAt(0)
}

// SetEx sets a key to hold the value together with a TTL.
func (conn *Connection) SetEx(key string, ttl time.Duration, value interface{}) error {
	result, err := conn.Command("setex", key, ttl.Seconds(), value)
	if err != nil {
		return err
	}
	return checkOK(result, ErrCannotSetKey, key)
}

// SetExExists sets a key to hold the value together with a TTL
// depending on the exists key. It has to exist when set to
// true or must not exist when set to false.
func (conn *Connection) SetExExists(key string, ttl time.Duration, exists bool, value interface{}) error {
	flag := map[bool]string{true: "xx", false: "nx"}
	result, err := conn.Command("set", key, value, "ex", ttl.Seconds(), flag[exists])
	if err != nil {
		return err
	}
	return checkOK(result, ErrCannotSetKey, key)
}

// SetNX sets key to hold the value if the key doesn't exist.
func (conn *Connection) SetNX(key string, value interface{}) error {
	result, err := conn.Command("setnx", key, value)
	if err != nil {
		return err
	}
	return checkOK(result, ErrCannotSetKey, key)
}

// SetRange overwrites a part of the string stored at a key, starting at the
// specified offset, for the entire length of value.
func (conn *Connection) SetRange(key string, offset int, value interface{}) (int, error) {
	result, err := conn.Command("setrange", key, offset, value)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// StrLen returns the length of the string value stored at a key.
func (conn *Connection) StrLen(key string) (int, error) {
	result, err := conn.Command("strlen", key)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// EOF
