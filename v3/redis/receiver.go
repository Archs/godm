// Tideland Go Data Management - Redis Client - Receiver
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
	"bufio"
	"fmt"
	"net"
	"strconv"

	"github.com/tideland/goas/v2/loop"
	"github.com/tideland/goas/v3/errors"
)

//--------------------
// ENVELOPE
//--------------------

// redisReplyKind classifies the reply of redis.
type redisReplyKind int

const (
	receivingError redisReplyKind = iota
	timeoutError
	statusReply
	errorReply
	integerReply
	bulkReply
	nullBulkReply
	arrayReply
)

var redisReplyKindStrings = map[redisReplyKind]string{
	receivingError: "receiving error",
	timeoutError:   "timeout error",
	statusReply:    "status",
	errorReply:     "error",
	integerReply:   "integer",
	bulkReply:      "bulk",
	nullBulkReply:  "null-bulk",
	arrayReply:     "array",
}

// replyEnv encapsulates the Redis reply for further processing.
type replyEnv struct {
	kind   redisReplyKind
	length int
	data   []byte
	err    error
}

// value returns the data as value.
func (r *replyEnv) value() Value {
	return Value(r.data)
}

// errorValue returns the error as value.
func (r *replyEnv) errorValue() Value {
	errdata := []byte(r.err.Error())
	return Value(errdata)
}

// String creates a string representation of the reply.
func (r *replyEnv) String() string {
	return fmt.Sprintf("REPLY (Kind: %s / Length: %d / Value: %v / Error: %v)", redisReplyKindStrings[r.kind], r.length, r.value(), r.err)
}

//--------------------
// RECEIVER
//--------------------

// receiver gets data from Redis and passes it to
// its connector.
type receiver struct {
	conn    net.Conn
	reader  *bufio.Reader
	replies chan *replyEnv
	loop    loop.Loop
}

// newReceiver start a background receiver used by
// a connector.
func newReceiver(conn net.Conn) *receiver {
	rcvr := &receiver{
		conn:    conn,
		reader:  bufio.NewReader(conn),
		replies: make(chan *replyEnv, 25),
	}
	rcvr.loop = loop.Go(rcvr.backendLoop)
	return rcvr
}

// stop stops the receiver.
func (r *receiver) stop() error {
	return r.loop.Stop()
}

// backendLoop retrieves and sends replies.
func (r *receiver) backendLoop(loop loop.Loop) error {
	for {
		select {
		case <-loop.ShallStop():
			return nil
		case r.replies <- r.receiveReply():
		}
	}
}

// receiveReply receives one reply.
func (r *receiver) receiveReply() *replyEnv {
	// Receive first line.
	line, err := r.reader.ReadBytes('\n')
	if err != nil {
		return &replyEnv{receivingError, 0, nil, err}
	}
	content := line[1 : len(line)-2]
	// First byte defines kind.
	switch line[0] {
	case '+':
		// Status reply.
		return &replyEnv{statusReply, 0, content, nil}
	case '-':
		// Error reply.
		return &replyEnv{errorReply, 0, content, nil}
	case ':':
		// Integer reply.
		return &replyEnv{integerReply, 0, content, nil}
	case '$':
		// Bulk reply or null bulk reply.
		count, err := strconv.Atoi(string(content))
		if err != nil {
			return &replyEnv{receivingError, 0, nil, errors.Annotate(err, ErrServerResponse, errorMessages)}
		}
		if count == -1 {
			// Null bulk reply.
			return &replyEnv{nullBulkReply, 0, nil, nil}
		}
		// Receive the bulk data.
		toRead := count + 2
		buffer := make([]byte, toRead)
		read := 0
		for read < toRead {
			n, err := r.reader.Read(buffer[read:])
			if err != nil {
				return &replyEnv{receivingError, 0, nil, err}
			}
			read += n
		}
		return &replyEnv{bulkReply, 0, buffer[0:count], nil}
	case '*':
		// Array reply. Check for timeout.
		length, err := strconv.Atoi(string(content))
		if err != nil {
			return &replyEnv{receivingError, 0, nil, errors.Annotate(err, ErrServerResponse, errorMessages)}
		}
		if length == -1 {
			// Timeout.
			return &replyEnv{timeoutError, 0, nil, nil}
		}
		return &replyEnv{arrayReply, length, nil, nil}
	}
	return &replyEnv{receivingError, 0, nil, errors.New(ErrServerResponse, errorMessages, "invalid received data type: "+string(line))}
}

// EOF
