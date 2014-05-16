// Tideland Go Data Management - Redis Client - Unit Tests - Set Commands
//
// Copyright (C) 2009-2014 Frank Mueller / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package redis_test

//--------------------
// IMPORTS
//--------------------

import (
	"fmt"
	"testing"

	"github.com/tideland/godm/v3/redis"
	"github.com/tideland/gots/v3/asserts"
)

//--------------------
// TESTS
//--------------------

func TestSAddRemCard(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()
	generateSetData(assert, conn, "s:add-rem-card", "abcdefghij")

	card, err := conn.SCard("s:add-rem-card")
	assert.Nil(err)
	assert.Equal(card, 100)
	card, err = conn.SCard("s:add-rem-card:not-existing")
	assert.Nil(err)
	assert.Equal(card, 0)

	added, err := conn.SAdd("s:add-rem-card", "a1", "a2")
	assert.Nil(err)
	assert.Equal(added, 0)
	added, err = conn.SAdd("s:add-rem-card", "aa", "bb")
	assert.Nil(err)
	assert.Equal(added, 2)
	card, err = conn.SCard("s:add-rem-card")
	assert.Nil(err)
	assert.Equal(card, 102)

	removed, err := conn.SRem("s:add-rem-card", "aa", "bb")
	assert.Nil(err)
	assert.Equal(removed, 2)
	removed, err = conn.SRem("s:add-rem-card", "aa", "bb")
	assert.Nil(err)
	assert.Equal(removed, 0)
	removed, err = conn.SRem("s:add-rem-card:not-existing", "aa", "bb")
	assert.Nil(err)
	assert.Equal(removed, 0)
	card, err = conn.SCard("s:add-rem-card")
	assert.Nil(err)
	assert.Equal(card, 100)
}

func TestSDiff(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	generateSetData(assert, conn, "s:diff:a", "abcde")
	generateSetData(assert, conn, "s:diff:b", "c")
	generateSetData(assert, conn, "s:diff:c", "acf")

	diff, err := conn.SDiff("s:diff:a", "s:diff:b", "s:diff:c")
	assert.Nil(err)
	assert.Length(diff, 30)

	generateSetData(assert, conn, "s:diff:d", "g")

	diff, err = conn.SDiff("s:diff:a", "s:diff:d")
	assert.Nil(err)
	assert.Length(diff, 50)

	card, err := conn.SDiffStore("s:diff:store", "s:diff:a", "s:diff:b", "s:diff:c")
	assert.Nil(err)
	assert.Equal(card, 30)
	card, err = conn.SCard("s:diff:store")
	assert.Nil(err)
	assert.Equal(card, 30)
}

func TestSInter(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	generateSetData(assert, conn, "s:inter:a", "abcde")
	generateSetData(assert, conn, "s:inter:b", "c")
	generateSetData(assert, conn, "s:inter:c", "acf")

	inter, err := conn.SInter("s:inter:a", "s:inter:b", "s:inter:c")
	assert.Nil(err)
	assert.Length(inter, 10)

	generateSetData(assert, conn, "s:inter:d", "g")

	inter, err = conn.SInter("s:inter:a", "s:inter:d")
	assert.Nil(err)
	assert.Length(inter, 0)

	card, err := conn.SInterStore("s:inter:store", "s:inter:a", "s:inter:b", "s:inter:c")
	assert.Nil(err)
	assert.Equal(card, 10)
	card, err = conn.SCard("s:inter:store")
	assert.Nil(err)
	assert.Equal(card, 10)
}

func TestSMember(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	generateSetData(assert, conn, "s:member", "abcde")

	isMember, err := conn.SIsMember("s:member", "a7")
	assert.Nil(err)
	assert.True(isMember)
	isMember, err = conn.SIsMember("s:member", "non existing member")
	assert.Nil(err)
	assert.False(isMember)

	members, err := conn.SMembers("s:member")
	assert.Nil(err)
	assert.Length(members, 50)

	members, err = conn.SRandMember("s:member", 0)
	assert.Nil(err)
	assert.Length(members, 1)
	members, err = conn.SRandMember("s:member", 15)
	assert.Nil(err)
	assert.Length(members, 15)
	members, err = conn.SRandMember("s:member", -100)
	assert.Nil(err)
	assert.Length(members, 100)
	added, err := conn.SAdd("s:member:rand", members)
	assert.Nil(err)
	assert.True(added <= 50)
	card, err := conn.SCard("s:member:rand")
	assert.Nil(err)
	assert.Equal(card, added)
}

func TestSMovePop(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	generateSetData(assert, conn, "s:move-pop:from", "abcde")

	moved, err := conn.SMove("s:move-pop:from", "s:move-pop:to", "c5")
	assert.Nil(err)
	assert.True(moved)
	card, err := conn.SCard("s:move-pop:from")
	assert.Nil(err)
	assert.Equal(card, 49)
	card, err = conn.SCard("s:move-pop:to")
	assert.Nil(err)
	assert.Equal(card, 1)
	member, err := conn.SIsMember("s:move-pop:to", "c5")
	assert.Nil(err)
	assert.True(member)

	popped, err := conn.SPop("s:move-pop:to")
	assert.Nil(err)
	assert.Equal(popped.String(), "c5")
	card, err = conn.SCard("s:move-pop:to")
	assert.Nil(err)
	assert.Equal(card, 0)
	popped, err = conn.SPop("s:move-pop:from")
	assert.Nil(err)
	assert.Match(popped.String(), "[a-e][0-9]")
	card, err = conn.SCard("s:move-pop:from")
	assert.Nil(err)
	assert.Equal(card, 48)
}

func TestSScan(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	generateSetData(assert, conn, "s:scan", "abcdefghij")

	assertScan := func(pattern string, count, total int) {
		var cursor int
		var result *redis.ResultSet
		var err error
		var max, all int

		if count == 0 {
			max = 20
		} else {
			max = count * 2
		}

		for {
			cursor, result, err = conn.SScan("s:scan", cursor, pattern, count)
			assert.Nil(err)
			all += result.Len()
			assert.True(result.Len() <= max)
			if cursor == 0 {
				break
			}
		}
		assert.Equal(all, total)
	}

	assertScan("", 0, 100)
	assertScan("", 20, 100)
	assertScan("a*", 0, 10)
	assertScan("a*", 5, 10)
	assertScan("-*-", 0, 0)
	assertScan("-*-", 20, 0)
}

func TestSUnion(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	conn, restore := connectDatabase(assert)
	defer restore()

	generateSetData(assert, conn, "s:union:a", "abcd")
	generateSetData(assert, conn, "s:union:b", "c")
	generateSetData(assert, conn, "s:union:c", "ace")

	union, err := conn.SUnion("s:union:a", "s:union:b", "s:union:c")
	assert.Nil(err)
	assert.Length(union, 50)

	card, err := conn.SUnionStore("s:union:store", "s:union:a", "s:union:b", "s:union:c")
	assert.Nil(err)
	assert.Equal(card, 50)
	card, err = conn.SCard("s:union:store")
	assert.Nil(err)
	assert.Equal(card, 50)
}

//--------------------
// TOOLS
//--------------------

// generateSetData generates a set of data at the given key.
func generateSetData(assert asserts.Assertion, conn *redis.Connection, key, charset string) {
	data := []interface{}{}
	for i := 0; i < 10; i++ {
		for _, c := range charset {
			d := fmt.Sprintf("%c%d", c, i)
			data = append(data, d)
		}
	}
	added, err := conn.SAdd(key, data...)
	assert.Nil(err)
	assert.Equal(added, len(data))
}

// EOF
