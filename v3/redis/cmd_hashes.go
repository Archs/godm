// Tideland Go Data Management - Redis Client - Commands - Hashes
//
// Copyright (C) 2009-2014 Frank Mueller / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package redis

//--------------------
// IMPORTS
//--------------------

import ()

//--------------------
// HASH COMMANDS
//--------------------

// HDel removes the specified fields from the hash stored at the key.
func (conn *Connection) HDel(key string, fields ...string) (int, error) {
	result, err := conn.Command("hdel", buildInterfaces(key, fields)...)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// HExists returns if field is an existing field in the hash stored at key.
func (conn *Connection) HExists(key, field string) (bool, error) {
	result, err := conn.Command("hexists", key, field)
	if err != nil {
		return false, err
	}
	return result.BoolAt(0)
}

// HGet returns the value associated with field in the hash stored at key.
func (conn *Connection) HGet(key, field string) (Value, error) {
	result, err := conn.Command("hget", key, field)
	if err != nil {
		return nil, err
	}
	return result.ValueAt(0)
}

// HGetAll returns all fields and values of the hash stored at key.
func (conn *Connection) HGetAll(key string) (Hash, error) {
	result, err := conn.Command("hgetall", key)
	if err != nil {
		return nil, err
	}
	return result.Hash()
}

// HIncrBy increments the number stored at field in the hash stored at key by increment.
func (conn *Connection) HIncrBy(key, field string, increment int) (int, error) {
	result, err := conn.Command("hincrby", key, field, increment)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// HIncrByFloat increments the specified field of an hash stored at key, and representing
// a floating point number, by the specified increment.
func (conn *Connection) HIncrByFloat(key, field string, increment float64) (float64, error) {
	result, err := conn.Command("hincrbyfloat", key, field, increment)
	if err != nil {
		return 0.0, err
	}
	value, err := result.ValueAt(0)
	if err != nil {
		return 0.0, err
	}
	return value.Float64()
}

// HKeys returns all field names in the hash stored at key.
func (conn *Connection) HKeys(key string) ([]string, error) {
	result, err := conn.Command("hkeys", key)
	if err != nil {
		return nil, err
	}
	return result.Strings(), nil
}

// HLen returns all field names in the hash stored at key.
func (conn *Connection) HLen(key string) (int, error) {
	result, err := conn.Command("hlen", key)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// HMGet returns the values associated with the specified fields in the hash
// stored at key.
func (conn *Connection) HMGet(key string, fields ...string) (*ResultSet, error) {
	result, err := conn.Command("hmget", buildInterfaces(key, fields)...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// HMSet sets the specified fields to their respective values in the hash stored at key.
func (conn *Connection) HMSet(key string, kvs Hash) error {
	result, err := conn.Command("hmset", key, kvs)
	if err != nil {
		return err
	}
	return checkOK(result, ErrCannotSetKey, keyValueArgsToKeys(kvs))
}

// HScan iterates fields of Hash types and their associated values.
func (conn *Connection) HScan(key string, cursor int, pattern string, count int) (int, KeyValues, error) {
	args := []interface{}{key, cursor}
	if pattern != "" {
		args = append(args, "match", pattern)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	result, err := conn.Command("hscan", args...)
	if err != nil {
		return 0, nil, err
	}
	cursor, err = result.IntAt(0)
	if err != nil {
		return 0, nil, err
	}
	values, err := result.ResultSetAt(1)
	if err != nil {
		return 0, nil, err
	}
	keyValues, err := values.KeyValues()
	return cursor, keyValues, err
}

// HSet sets field in the hash stored at key to value. It retruns true
// if the field has been added and false if it has been updated.
func (conn *Connection) HSet(key, field string, value interface{}) (bool, error) {
	result, err := conn.Command("hset", key, field, value)
	if err != nil {
		return false, err
	}
	return result.BoolAt(0)
}

// HSetNX sets field in the hash stored at key to value, only if field does not yet exist.
func (conn *Connection) HSetNX(key, field string, value interface{}) error {
	result, err := conn.Command("hsetnx", key, field, value)
	if err != nil {
		return err
	}
	return checkOK(result, ErrCannotSetKey, key+" / "+field)
}

// HVals returns all values in the hash stored at key.
func (conn *Connection) HVals(key string) (*ResultSet, error) {
	result, err := conn.Command("hvals", key)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// EOF
