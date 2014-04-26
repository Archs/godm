// Tideland Go Data Management - Redis Client - Commands - Sets
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
// SET COMMANDS
//--------------------

// SAdd adds the specified members to the set stored at key.
func (conn *Connection) SAdd(key string, members ...interface{}) (int, error) {
	result, err := conn.Command("sadd", buildInterfaces(key, members)...)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// SCard returns the set cardinality (number of elements) of the set stored at key.
func (conn *Connection) SCard(key string) (int, error) {
	result, err := conn.Command("scard", key)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// SDiff returns the members of the set resulting from the difference between
// the first set and all the successive sets.
func (conn *Connection) SDiff(keys ...string) (*ResultSet, error) {
	result, err := conn.Command("sdiff", buildInterfaces(keys)...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// SDiffStore is equal to SDiff(), but instead of returning the resulting set,
// it is stored in destination.
func (conn *Connection) SDiffStore(destination string, keys ...string) (int, error) {
	result, err := conn.Command("sdiffstore", buildInterfaces(destination, keys)...)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// SInter returns the members of the set resulting from the intersection
// of all the given sets.
func (conn *Connection) SInter(keys ...string) (*ResultSet, error) {
	result, err := conn.Command("sinter", buildInterfaces(keys)...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// SInterStore is equal to SInter(), but instead of returning the resulting set,
// it is stored in destination.
func (conn *Connection) SInterStore(destination string, keys ...string) (int, error) {
	result, err := conn.Command("sinterstore", buildInterfaces(destination, keys)...)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// SIsMember returns true if member is a member of the set stored at key.
func (conn *Connection) SIsMember(key string, member interface{}) (bool, error) {
	result, err := conn.Command("sismember", key, member)
	if err != nil {
		return false, err
	}
	return result.BoolAt(0)
}

// SMembers returns all the members of the set value stored at key.
func (conn *Connection) SMembers(key string) (*ResultSet, error) {
	result, err := conn.Command("smembers", key)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// SMove moves member from the set at source to the set at destination.
func (conn *Connection) SMove(source, destination string, member interface{}) (bool, error) {
	result, err := conn.Command("smove", source, destination, member)
	if err != nil {
		return false, err
	}
	return result.BoolAt(0)
}

// SPop removes and returns a random element from the set value stored at key.
func (conn *Connection) SPop(key string) (Value, error) {
	result, err := conn.Command("spop", key)
	if err != nil {
		return nil, err
	}
	return result.ValueAt(0)
}

// SRandMember returns a random element from the set value stored at key.
// It returns an array of count distinct elements if count is positive. If
// called with a negative count the behavior changes and the command is allowed
// to return the same element multiple times.
func (conn *Connection) SRandMember(key string, count int) (*ResultSet, error) {
	var result *ResultSet
	var err error
	if count == 0 {
		result, err = conn.Command("srandmember", key)
	} else {
		result, err = conn.Command("srandmember", key, count)
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

// SRem removes the specified members from the set stored at key.
func (conn *Connection) SRem(key string, members ...interface{}) (int, error) {
	result, err := conn.Command("srem", buildInterfaces(key, members)...)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// SScan iterates elements of a set.
func (conn *Connection) SScan(key string, cursor int, pattern string, count int) (int, *ResultSet, error) {
	args := []interface{}{key, cursor}
	if pattern != "" {
		args = append(args, "match", pattern)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	result, err := conn.Command("sscan", args...)
	if err != nil {
		return 0, nil, err
	}
	cursor, err = result.IntAt(0)
	if err != nil {
		return 0, nil, err
	}
	values, err := result.ResultSetAt(1)
	return cursor, values, err
}

// SUnion returns the members of the set resulting from the union
// of all the given sets.
func (conn *Connection) SUnion(keys ...string) (*ResultSet, error) {
	result, err := conn.Command("sunion", buildInterfaces(keys)...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// SUnionStore is equal to SUnion(), but instead of returning the resulting set,
// it is stored in destination.
func (conn *Connection) SUnionStore(destination string, keys ...string) (int, error) {
	result, err := conn.Command("sunionstore", buildInterfaces(destination, keys)...)
	if err != nil {
		return 0, err
	}
	return result.IntAt(0)
}

// EOF
