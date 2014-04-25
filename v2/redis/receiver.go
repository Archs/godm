// Tideland Go Data Management - Redis Client - Receiver
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
	multiBulkReply
)

var redisReplyKindStrings = map[redisReplyKind]string{
	receivingError: "receiving error",
	timeoutError:   "timeout error",
	statusReply:    "status",
	errorReply:     "error",
	integerReply:   "integer",
	bulkReply:      "bulk",
	nullBulkReply:  "null-bulk",
	multiBulkReply: "mult-bulk",
}

// replyEnv encapsulates the Redis reply for further processing.
type replyEnv struct {
	kind redisReplyKind
	data []byte
	err  error
}

// int returns the data interpreted as integer for
// further operations depending on the kind (e.g.
// multi-bulk replies).
func (r *replyEnv) int() (int, error) {
	return strconv.Atoi(string(r.data))
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
	return fmt.Sprintf("REPLY (K: %s / D: %v / E: %v)", redisReplyKindStrings[r.kind], r.value(), r.err)
}

//--------------------
// RECEIVER
//--------------------

// receiver gets data from Redis and passes it to
// its connector.
type receiver struct {
	id      int
	conn    net.Conn
	reader  *bufio.Reader
	replies chan *replyEnv
	loop    loop.Loop
}

// newReceiver start a background receiver used by
// a connector.
func newReceiver(id int, conn net.Conn) *receiver {
	rcvr := &receiver{
		id:      id,
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
		return &replyEnv{receivingError, nil, err}
	}
	content := line[1 : len(line)-2]
	// First byte defines kind.
	switch line[0] {
	case '+':
		// Status reply.
		return &replyEnv{statusReply, content, nil}
	case '-':
		// Error reply.
		return &replyEnv{errorReply, content, nil}
	case ':':
		// Integer reply.
		return &replyEnv{integerReply, content, nil}
	case '$':
		// Bulk reply or null bulk reply.
		count, err := strconv.Atoi(string(content))
		if err != nil {
			return &replyEnv{receivingError, nil, errors.Annotate(err, ErrServerResponse, errorMessages)}
		}
		if count == -1 {
			// Null bulk reply.
			return &replyEnv{nullBulkReply, nil, errors.New(ErrKeyNotFound, errorMessages)}
		}
		// Receive the bulk data.
		toRead := count + 2
		buffer := make([]byte, toRead)
		read := 0
		for read < toRead {
			n, err := r.reader.Read(buffer[read:])
			if err != nil {
				return &replyEnv{receivingError, nil, err}
			}
			read += n
		}
		return &replyEnv{bulkReply, buffer[0:count], nil}
	case '*':
		// Multi-bulk reply. Check for timeout.
		count, err := strconv.Atoi(string(content))
		if err != nil {
			return &replyEnv{receivingError, nil, errors.Annotate(err, ErrServerResponse, errorMessages)}
		}
		if count == -1 {
			// Timeout.
			return &replyEnv{timeoutError, nil, nil}
		}
		return &replyEnv{multiBulkReply, content, nil}
	}
	return &replyEnv{receivingError, nil, errors.New(ErrServerResponse, errorMessages, "invalid received data type")}
}

// EOF
