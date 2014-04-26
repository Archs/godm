// Tideland Go Data Management - Redis Client - Tools
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

	"github.com/tideland/goas/v2/logger"
	"github.com/tideland/goas/v3/errors"
)

//--------------------
// TOOLS
//--------------------

// valuer describes any type able to return a list of values.
type valuer interface {
	Len() int
	Values() []Value
}

// join builds a byte slice out of some parts.
func join(parts ...interface{}) []byte {
	tmp := []byte{}
	for _, part := range parts {
		switch typedPart := part.(type) {
		case []byte:
			tmp = append(tmp, typedPart...)
		case string:
			tmp = append(tmp, []byte(typedPart)...)
		case int:
			tmp = append(tmp, []byte(strconv.Itoa(typedPart))...)
		default:
			tmp = append(tmp, []byte(fmt.Sprintf("%v", typedPart))...)
		}
	}
	return tmp
}

// valueToBytes converts a value into a byte slice.
func valueToBytes(value interface{}) []byte {
	switch typedValue := value.(type) {
	case string:
		return []byte(typedValue)
	case []byte:
		return typedValue
	case []string:
		return []byte(strings.Join(typedValue, "\r\n"))
	case map[string]string:
		tmp := make([]string, len(typedValue))
		i := 0
		for k, v := range typedValue {
			tmp[i] = fmt.Sprintf("%v:%v", k, v)
			i++
		}
		return []byte(strings.Join(tmp, "\r\n"))
	case Hash:
		tmp := []byte{}
		for k, v := range typedValue {
			kb := valueToBytes(k)
			vb := valueToBytes(v)
			tmp = append(tmp, kb...)
			tmp = append(tmp, vb...)
		}
		return tmp
	}
	return []byte(fmt.Sprintf("%v", value))
}

// keyValueArgsToKeys converts a mixed number of keys and values
// into a slice containing the keys.
func keyValueArgsToKeys(kvs ...interface{}) []string {
	keys := []string{}
	ok := true
	for _, k := range kvs {
		if ok {
			key := string(valueToBytes(k))
			keys = append(keys, key)
		}
		ok = !ok
	}
	return keys
}

// buildInterfaces creates a slice of interfaces out
// of the passed arguments. Found string or interface
// slices are flattened.
func buildInterfaces(values ...interface{}) []interface{} {
	ifcs := []interface{}{}
	for _, value := range values {
		switch v := value.(type) {
		case []string:
			for _, s := range v {
				ifcs = append(ifcs, s)
			}
		case []interface{}:
			for _, i := range v {
				ifcs = append(ifcs, i)
			}
		default:
			ifcs = append(ifcs, v)
		}
	}
	return ifcs
}

// buildLengthPart creates the length part of a command.
func buildLengthPart(args []interface{}) []byte {
	length := 1
	for _, arg := range args {
		switch typedArg := arg.(type) {
		case valuer:
			length += typedArg.Len()
		case Hash:
			length += typedArg.Len() * 2
		case Hashable:
			length += typedArg.Len() * 2
		default:
			length++
		}
	}
	return join("*", length, "\r\n")
}

// buildValuePart creates one value part of a command.
func buildValuePart(value interface{}) []byte {
	var raw []byte
	if v, ok := value.(Value); ok {
		raw = []byte(v)
	} else {
		raw = valueToBytes(value)
	}
	return join("$", len(raw), "\r\n", raw, "\r\n")
}

// buildArgumentsPart creates the the arguments parts of a command.
func buildArgumentsPart(args []interface{}) []byte {
	buildValuesPart := func(vs valuer) []byte {
		tmp := []byte{}
		for _, value := range vs.Values() {
			tmp = append(tmp, buildValuePart(value)...)
		}
		return tmp
	}
	buildHashPart := func(h Hash) []byte {
		tmp := []byte{}
		for key, value := range h {
			tmp = append(tmp, buildValuePart(key)...)
			tmp = append(tmp, buildValuePart(value)...)
		}
		return tmp
	}
	tmp := []byte{}
	part := []byte{}
	for _, arg := range args {
		switch typedArg := arg.(type) {
		case valuer:
			part = buildValuesPart(typedArg)
		case Hash:
			part = buildHashPart(typedArg)
		case Hashable:
			part = buildHashPart(typedArg.GetHash())
		default:
			part = buildValuePart(arg)
		}
		tmp = append(tmp, part...)
	}
	return tmp
}

// containsPatterns checks, if the channel contains a pattern
// to subscribe to or unsubscribe from multiple channels.
func containsPattern(channel interface{}) bool {
	ch := channel.(string)
	if strings.IndexAny(ch, "*?[") != -1 {
		return true
	}
	return false
}

// logCommand logs a command and its execution status.
func logCommand(cmd string, args []interface{}, result *ResultSet, err error, log bool) {
	// Format the command for the log entry.
	formatArgs := func() string {
		output := make([]string, len(args))
		for i, arg := range args {
			output[i] = string(valueToBytes(arg))
		}
		return strings.Join(output, " / ")
	}
	formatResults := func() string {
		if result == nil {
			return "none"
		}
		return result.String()
	}
	logOutput := func() string {
		format := "CMD %s ARGS %s %s %v"
		if err == nil {
			return fmt.Sprintf(format, cmd, formatArgs(), "OK", formatResults())
		}
		return fmt.Sprintf(format, cmd, formatArgs(), "ERROR", err)
	}
	// Log positive commands only if wanted, errors always.
	if err != nil {
		if errors.IsError(err, ErrServerResponse) || errors.IsError(err, ErrTimeout) {
			return
		}
		logger.Errorf(logOutput())
	} else if log {
		logger.Infof(logOutput())
	}
}

// checkOK checks if the results are simply "OK", the standard reply
// of Redis on successful operations. In case it is not ok the according
// error is generated.
func checkOK(result *ResultSet, code int, args ...interface{}) error {
	ok, err := result.StringAt(0)
	if err != nil {
		return errors.Annotate(err, code, errorMessages, args...)
	}
	if ok != "OK" {
		return errors.New(code, errorMessages, args...)
	}
	return nil
}

// EOF
