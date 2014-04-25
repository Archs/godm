// Tideland Go Data Management - Write once read multiple - Unit Tests
//
// Copyright (C) 2012-2014 Frank Mueller / Tideland / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package worm_test

//--------------------
// IMPORTS
//--------------------

import (
	"sort"
	"testing"

	"github.com/tideland/godm/v2/worm"
	"github.com/tideland/gots/v3/asserts"
	"github.com/tideland/gots/v3/generators"
)

//--------------------
// TESTS
//--------------------

// TestIntList tests the usage of int lists.
func TestIntList(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	input := worm.Ints{1, 5, 6, 2, 5, 2, 9}
	i := worm.NewIntList(input)

	// Test length.
	assert.Length(i, 7)

	// Tast retrieving the values.
	values := i.Values()
	assert.Length(values, 7)
	assert.Equal(values, input)

	// Tast retrieving the values sorted.
	values = i.SortedValues()
	assert.Length(values, 7)
	assert.Equal(values, worm.Ints{1, 2, 2, 5, 5, 6, 9})

	// Test appending more values.
	av := worm.Ints{2, 6, 1001, 1010, 1005}
	ai := i.Append(av)
	assert.Length(ai, i.Len()+5)
}

// TestStringList tests the usage of string lists.
func TestStringList(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	genA := generators.New(generators.FixedRand())
	genB := generators.New(generators.FixedRand())
	input := genA.Words(25)
	s := worm.NewStringList(input)

	// Test length.
	assert.Length(s, 25)

	// Tast retrieving the values.
	values := s.Values()
	assert.Length(values, 25)
	assert.Equal(values, worm.Strings(input))

	// Tast retrieving the values sorted.
	values = s.SortedValues()
	assert.Length(values, 25)
	raw := genB.Words(25)
	sort.Strings(raw)
	sortedInput := worm.Strings(raw)
	assert.Equal(values, sortedInput)

	// Test appending more values.
	av := genA.Words(10)
	as := s.Append(av)
	assert.Length(as, s.Len()+10)
	assert.Length(s, 25)
}

// TestIntSet tests the usage of int sets.
func TestIntSet(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	input := worm.Ints{1, 5, 6, 2, 5, 2, 9}
	i := worm.NewIntSet(input)

	// Test length.
	assert.Length(i, 5, "correct length")

	// Tast retrieving the values.
	values := i.Values()
	assert.Length(values, 5, "correct length")
	assert.Equal(values, worm.Ints{1, 2, 5, 6, 9}, "values are right")

	// Test containing test.
	assert.True(i.Contains(), "emtpy values are ok")
	assert.True(i.Contains(6, 5), "values detected")
	assert.False(i.Contains(1, 2, 7), "invalid values recognized")

	// Test applying more values.
	av := worm.Ints{2, 6, 1001, 1010, 1005}
	ai := i.Apply(av)
	assert.Length(ai, i.Len()+3, "three more values in the new set")
}

// TestStringSet tests the usage of string sets.
func TestStringSet(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	input := worm.Strings{"foo", "bar", "baz", "yadda", "foo", "yadda", "argle"}
	s := worm.NewStringSet(input)

	// Test length.
	assert.Length(s, 5, "correct length")

	// Tast retrieving the values.
	values := s.Values()
	assert.Length(values, 5, "correct length")
	assert.Equal(values, worm.Strings{"argle", "bar", "baz", "foo", "yadda"}, "values are right")

	// Test containing test.
	assert.True(s.Contains(), "emtpy values are ok")
	assert.True(s.Contains("baz", "foo"), "values detected")
	assert.False(s.Contains("argle", "yadda", "zapper"), "invalid values recognized")

	// Test applying more values.
	av := worm.Strings{"foo", "bar", "alpha", "bravo", "charlie"}
	as := s.Apply(av)
	assert.Length(as, s.Len()+3, "three more values in the new set")
}

// TestBoolMap tests the usage of the bool map.
func TestBoolMap(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	input := worm.BoolMapValues{"alpha": true, "bravo": false, "charlie": true}
	m := worm.NewBoolMap(input)

	// Test length.
	assert.Length(m, 3)

	// Test getting values.
	assert.True(m.Get("alpha"))
	assert.False(m.Get("bravo"))
	assert.True(m.Get("charlie"))
	assert.False(m.Get("delta"))

	// Test the keys.
	assert.Equal(m.Keys(), []string{"alpha", "bravo", "charlie"})
	assert.True(m.ContainsKeys("alpha", "charlie"))
	assert.False(m.ContainsKeys("bravo", "delta"))

	// Test copying.
	assert.True(m.Copy("alpha", "bravo").ContainsKeys("alpha", "bravo"))
	assert.Equal(m.CopyAll().Len(), 3)
	assert.Equal(m.CopyAllValues(), input)

	// Test applying more values.
	m = m.Apply(worm.BoolMapValues{"delta": true, "echo": false, "foxtrott": true})
	assert.Length(m, 6)
}

// TestIntMap tests the usage of the int map.
func TestIntMap(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	input := worm.IntMapValues{"alpha": 1, "bravo": 2, "charlie": 3}
	m := worm.NewIntMap(input)

	// Test length.
	assert.Length(m, 3)

	// Test getting values.
	assert.Equal(m.Get("alpha"), 1)
	assert.Equal(m.Get("bravo"), 2)
	assert.Equal(m.Get("charlie"), 3)
	assert.Equal(m.Get("delta"), 0)

	// Test the keys.
	assert.Equal(m.Keys(), []string{"alpha", "bravo", "charlie"})
	assert.True(m.ContainsKeys("alpha", "charlie"))
	assert.False(m.ContainsKeys("bravo", "delta"))

	// Test copying.
	assert.True(m.Copy("alpha", "bravo").ContainsKeys("alpha", "bravo"))
	assert.Equal(m.CopyAll().Len(), 3)
	assert.Equal(m.CopyAllValues(), input)

	// Test applying more values.
	m = m.Apply(worm.IntMapValues{"delta": 4, "echo": 5, "foxtrott": 6})
	assert.Length(m, 6)
}

// TestStringMap tests the usage of the string map.
func TestStringMap(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	input := worm.StringMapValues{"alpha": "a", "bravo": "b", "charlie": "c"}
	m := worm.NewStringMap(input)

	// Test length.
	assert.Length(m, 3)

	// Test getting values.
	assert.Equal(m.Get("alpha"), "a")
	assert.Equal(m.Get("bravo"), "b")
	assert.Equal(m.Get("charlie"), "c")
	assert.Equal(m.Get("delta"), "")

	// Test the keys.
	assert.Equal(m.Keys(), []string{"alpha", "bravo", "charlie"})
	assert.True(m.ContainsKeys("alpha", "charlie"))
	assert.False(m.ContainsKeys("bravo", "delta"))

	// Test copying.
	assert.True(m.Copy("alpha", "bravo").ContainsKeys("alpha", "bravo"))
	assert.Equal(m.CopyAll().Len(), 3)
	assert.Equal(m.CopyAllValues(), input)

	// Test applying more values.
	m = m.Apply(worm.StringMapValues{"delta": "d", "echo": "e", "foxtrott": "f"})
	assert.Length(m, 6)
}

// TestMultiMap tests the usage of the multi map.
func TestMultiMap(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	input := worm.MultiMapValues{"alpha": "true", "bravo": true, "charlie": 815, "delta": 47.11}
	m := worm.NewMultiMap(input)

	// Test length.
	assert.Length(m, 4)

	// Test getting values.
	assert.Equal(m.GetString("alpha"), "true")
	assert.Equal(m.GetString("bravo"), "true")
	assert.Equal(m.GetString("charlie"), "815")
	assert.Equal(m.GetString("delta"), "47.11")
	assert.Equal(m.GetString("echo"), "")

	assert.Equal(m.GetBool("alpha"), true)
	assert.Equal(m.GetBool("bravo"), true)
	assert.Equal(m.GetBool("charlie"), true)
	assert.Equal(m.GetBool("delta"), true)
	assert.Equal(m.GetBool("echo"), false)

	assert.Equal(m.GetInt64("alpha"), int64(0))
	assert.Equal(m.GetInt64("bravo"), int64(1))
	assert.Equal(m.GetInt64("charlie"), int64(815))
	assert.Equal(m.GetInt64("delta"), int64(47))
	assert.Equal(m.GetInt64("echo"), int64(0))

	assert.Equal(m.GetInt("bravo"), 1)
	assert.Equal(m.GetInt("alpha"), 0)
	assert.Equal(m.GetInt("charlie"), 815)
	assert.Equal(m.GetInt("delta"), 47)
	assert.Equal(m.GetInt("echo"), 0)

	assert.Equal(m.GetFloat64("alpha"), 0.0)
	assert.Equal(m.GetFloat64("bravo"), 1.0)
	assert.Equal(m.GetFloat64("charlie"), 815.0)
	assert.Equal(m.GetFloat64("delta"), 47.11)
	assert.Equal(m.GetFloat64("echo"), 0.0)

	// Test the keys.
	assert.Equal(m.Keys(), []string{"alpha", "bravo", "charlie", "delta"})
	assert.True(m.ContainsKeys("alpha", "charlie"))
	assert.False(m.ContainsKeys("bravo", "echo"))

	// Test copying.
	assert.True(m.Copy("alpha", "bravo").ContainsKeys("alpha", "bravo"))

	c := m.CopyAll()

	assert.Equal(c.Len(), 4)
	assert.Equal(m.CopyAllValues(), c.CopyAllValues())

	// Test applying more values.
	m = m.Apply(worm.MultiMapValues{"echo": "e", "foxtrott": 8.15})
	assert.Length(m, 6)
}

// EOF
