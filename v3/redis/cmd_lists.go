// Tideland Go Data Management - Redis Client - Commands - Lists
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
// LIST COMMANDS
//--------------------

// BLPop is the blocking variant of LPop(). The timeout is taken
// as seconds.
func (conn *Connection) BLPop(timeout time.Duration, keys ...string) (string, Value, error) {
	result, err := conn.Command("blpop", buildInterfaces(keys, int(timeout.Seconds()))...)
	if err != nil {
		return "", nil, err
	}
	key, err := result.StringAt(0)
	if err != nil {
		return "", nil, err
	}
	value, err := result.ValueAt(1)
	if err != nil {
		return "", nil, err
	}
	return key, value, nil
}

// BRPop is the blocking variant of RPop(). The timeout is taken
// as seconds.
func (conn *Connection) BRPop(timeout time.Duration, keys ...string) (string, Value, error) {
	result, err := conn.Command("brpop", buildInterfaces(keys, int(timeout.Seconds()))...)
	if err != nil {
		return "", nil, err
	}
	key, err := result.StringAt(0)
	if err != nil {
		return "", nil, err
	}
	value, err := result.ValueAt(1)
	if err != nil {
		return "", nil, err
	}
	return key, value, nil
}

// BRPopLPush is the blocking variant of RPopLPush(). The timeout is taken
// as seconds.
func (conn *Connection) BRPopLPush(source, destination string, timeout time.Duration) (Value, error) {
	result, err := conn.Command("brpoplpush", source, destination, int(timeout.Seconds()))
	if err != nil {
		return nil, err
	}
	return result.ValueAt(0)
}

// LIndex returns the element at index index in the list stored at key.
func (conn *Connection) LIndex(key string, index int) (Value, error) {
	result, err := conn.Command("lindex", key, index)
	if err != nil {
		return nil, err
	}
	return result.ValueAt(0)
}

// LInsertBefore inserts value in the list stored at key before the
// reference value pivot.
func (conn *Connection) LInsertBefore(key string, pivot, value interface{}) (int, error) {
	result, err := conn.Command("linsert", key, "before", pivot, value)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// LInsertAfter inserts value in the list stored at key after the
// reference value pivot.
func (conn *Connection) LInsertAfter(key string, pivot, value interface{}) (int, error) {
	result, err := conn.Command("linsert", key, "after", pivot, value)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// LLen returns the length of the list stored at key.
func (conn *Connection) LLen(key string) (int, error) {
	result, err := conn.Command("llen", key)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// LPop removes and returns the first element of the list stored at key.
func (conn *Connection) LPop(key string) (Value, error) {
	result, err := conn.Command("lpop", key)
	if err != nil {
		return nil, err
	}
	return result.ValueAt(0)
}

// LPush inserts all the specified values at the head of the list stored at key.
func (conn *Connection) LPush(key string, values ...interface{}) (int, error) {
	result, err := conn.Command("lpush", buildInterfaces(key, values)...)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// LPushX inserts value at the head of the list stored at key, only if key
// already exists and holds a list.
func (conn *Connection) LPushX(key string, value interface{}) (int, error) {
	result, err := conn.Command("lpushx", key, value)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// LRange returns the specified elements of the list stored at key.
func (conn *Connection) LRange(key string, start, stop int) (*ResultSet, error) {
	result, err := conn.Command("lrange", key, start, stop)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// LRem removes the first count occurrences of elements equal to
// value from the list stored at key.
func (conn *Connection) LRem(key string, count int, value interface{}) (int, error) {
	result, err := conn.Command("lrem", key, count, value)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// LSet sets the list element at index to value.
func (conn *Connection) LSet(key string, index int, value interface{}) error {
	result, err := conn.Command("lset", key, index, value)
	if err != nil {
		return err
	}
	return checkOK(result, ErrCannotSetList, key, index)
}

// LTrim trims an existing list so that it will contain only the specified
// range of elements specified.
func (conn *Connection) LTrim(key string, start, stop int) error {
	result, err := conn.Command("ltrim", key, start, stop)
	if err != nil {
		return err
	}
	return checkOK(result, ErrCannotTrimList, key, start, stop)
}

// RPop removes and returns the last element of the list stored at key.
func (conn *Connection) RPop(key string) (Value, error) {
	result, err := conn.Command("rpop", key)
	if err != nil {
		return nil, err
	}
	return result.ValueAt(0)
}

// RPopLPush atomically returns and removes the last element (tail) of
// the list stored at source, and pushes the element at the first
// element (head) of the list stored at destination.
// as seconds.
func (conn *Connection) RPopLPush(source, destination string) (Value, error) {
	result, err := conn.Command("rpoplpush", source, destination)
	if err != nil {
		return nil, err
	}
	return result.ValueAt(0)
}

// RPush inserts all the specified values at the tail of the list stored at key.
func (conn *Connection) RPush(key string, values ...interface{}) (int, error) {
	result, err := conn.Command("rpush", buildInterfaces(key, values)...)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// RPushX inserts value at the tail of the list stored at key, only if key
// already exists and holds a list.
func (conn *Connection) RPushX(key string, value interface{}) (int, error) {
	result, err := conn.Command("rpushx", key, value)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// EOF
