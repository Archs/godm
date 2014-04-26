// Tideland Go Data Management - Redis Client - Commands - Keys
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

	"github.com/tideland/goas/v3/errors"
)

//--------------------
// KEY COMMANDS
//--------------------

// Del deletes one or more keys and returns the number of
// deleted keys.
func (conn *Connection) Del(keys ...string) (int, error) {
	result, err := conn.Command("del", buildInterfaces(keys)...)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// Dump serializes the value stored at a key in a Redis-specific format
// and returns it to the user.
func (conn *Connection) Dump(key string) ([]byte, error) {
	result, err := conn.Command("dump", key)
	if err != nil {
		return nil, err
	}
	value, err := result.ValueAt(0)
	if err != nil {
		return nil, err
	}
	return value.Bytes(), nil
}

// Exists returns true if a key exists.
func (conn *Connection) Exists(key string) (bool, error) {
	result, err := conn.Command("exists", key)
	if err != nil {
		return false, err
	}
	return result.BoolAt(0)
}

// Expire sets a TTL on a key with seconds precision.
func (conn *Connection) Expire(key string, ttl time.Duration) error {
	seconds := int(ttl.Seconds())
	_, err := conn.Command("expire", key, seconds)
	return err
}

// ExpireAt sets a timeout on a key based on a time.
func (conn *Connection) ExpireAt(key string, timestamp time.Time) error {
	_, err := conn.Command("expireat", key, timestamp.Unix())
	return err
}

// Keys returns all keys matching pattern.
func (conn *Connection) Keys(pattern string) ([]string, error) {
	result, err := conn.Command("keys", pattern)
	if err != nil {
		return nil, err
	}
	return result.Strings(), nil
}

// Migrate transfers a key from a source Redis instance to a destination
// Redis instance.
func (conn *Connection) Migrate(host string, port int, key string, index int, timeout time.Duration, copy, replace bool) error {
	milliseconds := timeout.Nanoseconds() / 1000000
	args := []interface{}{host, port, key, index, milliseconds}
	if copy {
		args = append(args, "copy")
	}
	if replace {
		args = append(args, "replace")
	}
	_, err := conn.Command("migrate", args...)
	if err != nil {
		return err
	}
	return nil
}

// Move moves a key from the currently selected database to the specified
// destination database.
func (conn *Connection) Move(key string, index int) (bool, error) {
	result, err := conn.Command("move", key, index)
	if err != nil {
		return false, err
	}
	return result.BoolAt(0)
}

// Persist removes the existing timeout on a key.
func (conn *Connection) Persist(key string) error {
	_, err := conn.Command("persist", key)
	return err
}

// PExpire sets a TTL on a key with milliseconds precision.
func (conn *Connection) PExpire(key string, ttl time.Duration) error {
	milliseconds := ttl.Nanoseconds() / 1000000
	_, err := conn.Command("pexpire", key, milliseconds)
	return err
}

// PExpireAt sets a timeout on a key based on a time with milliseconds
// precision.
func (conn *Connection) PExpireAt(key string, timestamp time.Time) error {
	unixMillis := timestamp.UnixNano() / 1000000
	_, err := conn.Command("pexpireat", key, unixMillis)
	return err
}

// PTTL returns the remaining TTL of a key that has an expire
// set in milliseconds.
func (conn *Connection) PTTL(key string) (int, error) {
	result, err := conn.Command("pttl", key)
	if err != nil {
		return 0, err
	}
	ttl, err := result.IntAt(0)
	if err != nil {
		return 0, err
	}
	switch ttl {
	case -2:
		return 0, errors.New(ErrKeyNotFound, errorMessages, key)
	case -1:
		return 0, errors.New(ErrKeyNotFound, errorMessages, key)
	}
	return ttl, nil
}

// RandomKey returns a random key from the currently selected database.
func (conn *Connection) RandomKey() (string, error) {
	result, err := conn.Command("randomkey")
	if err != nil {
		return "", err
	}
	return result.StringAt(0)
}

// Rename renames oldkey to newkey.
func (conn *Connection) Rename(oldkey, newkey string) error {
	_, err := conn.Command("rename", oldkey, newkey)
	return err
}

// RenameNX renames oldkey to newkey if that doesn't exist.
func (conn *Connection) RenameNX(oldkey, newkey string) error {
	result, err := conn.Command("renamenx", oldkey, newkey)
	if err != nil {
		return err
	}
	value, err := result.ValueAt(0)
	if err != nil {
		return err
	}
	if !value.IsOK() {
		return errors.New(ErrCannotRenameKey, errorMessages, oldkey)
	}
	return nil
}

// Restore create a key associated with a value that is obtained by deserializing
// the provided serialized value with Dump().
func (conn *Connection) Restore(key string, ttl time.Duration, value []byte) error {
	milliseconds := ttl.Nanoseconds() / 1000000
	_, err := conn.Command("restore", key, milliseconds, value)
	return err
}

// Scan iterates keys.
func (conn *Connection) Scan(cursor int, match string, count int) (int, []string, error) {
	args := []interface{}{cursor}
	if match != "" {
		args = append(args, "match", match)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	result, err := conn.Command("scan", args...)
	if err != nil {
		return 0, nil, err
	}
	cursor, err = result.IntAt(0)
	if err != nil {
		return 0, nil, err
	}
	keys, err := result.ResultSetAt(1)
	return cursor, keys.Strings(), err
}

// TTL returns the remaining TTL of a key that has an expire
// set in seconds.
func (conn *Connection) TTL(key string) (int, error) {
	result, err := conn.Command("ttl", key)
	if err != nil {
		return 0, err
	}
	ttl, err := result.IntAt(0)
	if err != nil {
		return 0, err
	}
	switch ttl {
	case -2:
		return 0, errors.New(ErrKeyNotFound, errorMessages, key)
	case -1:
		return 0, errors.New(ErrKeyNotFound, errorMessages, key)
	}
	return ttl, nil
}

// Type returns the string representation of the type of the value
// stored at key.
func (conn *Connection) Type(key string) (string, error) {
	result, err := conn.Command("type", key)
	if err != nil {
		return "", err
	}
	return result.StringAt(0)
}

// EOF
