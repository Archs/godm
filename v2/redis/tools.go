// Tideland Go Data Management - Redis Client - Tools
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
	"fmt"
	"strconv"
	"strings"

	"github.com/tideland/goas/v2/logger"
)

//--------------------
// TOOLS
//--------------------

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

// stringsToInterfaces converts a number of strings into a
// slice of interfaces.
func stringsToInterfaces(strs ...string) []interface{} {
	ifcs := make([]interface{}, len(strs))
	for i, str := range strs {
		ifcs[i] = interface{}(str)
	}
	return ifcs
}

// buildLengthPart creates the length part of a command.
func buildLengthPart(args []interface{}) []byte {
	length := 1
	for _, arg := range args {
		switch typedArg := arg.(type) {
		case Hash:
			length += len(typedArg) * 2
		case Hashable:
			length += len(typedArg.GetHash()) * 2
		default:
			length++
		}
	}
	return join("*", length, "\r\n")
}

// buildValuePart creates one value part of a command.
func buildValuePart(value interface{}) []byte {
	valueBytes := valueToBytes(value)
	return join("$", len(valueBytes), "\r\n", valueBytes, "\r\n")
}

// buildArgumentsPart creates the the arguments parts of a command.
func buildArgumentsPart(args []interface{}) []byte {
	buildHashPart := func(h Hash) []byte {
		tmp := []byte{}
		for k, v := range h {
			tmp = append(tmp, buildValuePart(k)...)
			tmp = append(tmp, buildValuePart(v)...)
		}
		return tmp
	}
	tmp := []byte{}
	part := []byte{}
	for _, arg := range args {
		switch typedArg := arg.(type) {
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
func logCommand(cmd string, args []interface{}, err error, log bool) {
	// Format the command for the log entry.
	formatCommand := func() string {
		msg := "CMD " + cmd
		for _, arg := range args {
			msg = fmt.Sprintf("%s %v", msg, arg)
		}
		return msg
	}
	// Log positive commands only if wanted, errors always.
	if err == nil {
		if log {
			logger.Infof("%s OK", formatCommand())
		}
	} else {
		logger.Errorf("%s ERROR %v", formatCommand(), err)
	}
}

//--------------------
// MULTI COMMAND
//--------------------

// MultiCommand enables the user to perform multiple commands
// in one call.
type MultiCommand interface {
	// Command performs a command inside the transaction.
	// It will be queued.
	Command(cmd string, args ...interface{}) error

	// Discard throws all so far queued commands away.
	Discard() error
}

// multiCommand implements the MultiCommand interface.
type multiCommand struct {
	cnctr *connector
	cmds  []string
}

// newMultiCommand creates a new multi command helper.
func newMultiCommand(cnctr *connector) (*multiCommand, error) {
	_, err := cnctr.command("multi")
	if err != nil {
		return nil, err
	}
	return &multiCommand{
		cnctr: cnctr,
		cmds:  []string{},
	}, nil
}

// Command performs a command inside the transaction. It will
// be queued.
func (mc *multiCommand) Command(cmd string, args ...interface{}) error {
	_, err := mc.cnctr.command(cmd, args...)
	if err != nil {
		return err
	}
	return nil
}

// Discard throws all so far queued commands away.
func (mc *multiCommand) Discard() error {
	// Send the discard command.
	_, err := mc.cnctr.command("discard")
	if err != nil {
		return err
	}
	// Now send the new multi command again.
	_, err = mc.cnctr.command("multi")
	return err
}

// EOF
