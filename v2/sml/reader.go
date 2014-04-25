// Tideland Go Data Management - Simple Markup Language - Reader
//
// Copyright (C) 2009-2014 Frank Mueller / Tideland / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package sml

//--------------------
// IMPORTS
//--------------------

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"unicode"

	"github.com/tideland/goas/v3/errors"
)

//--------------------
// SML READER
//--------------------

// Rune classes.
const (
	rcText int = iota + 1
	rcSpace
	rcOpen
	rcClose
	rcEscape
	rcExclamation
	rcTag
	rcEOF
	rcInvalid
)

// ReadSML parses a SML document and uses the passed builder
// for the callbacks.
func ReadSML(reader io.Reader, builder Builder) error {
	s := &mlReader{
		reader:  bufio.NewReader(reader),
		builder: builder,
		index:   -1,
	}
	if err := s.readPreliminary(); err != nil {
		return err
	}
	return s.readTagNode()
}

// mlReader is used by ReadSML to parse a SML document
// and return it as node structure.
type mlReader struct {
	reader  *bufio.Reader
	builder Builder
	index   int
}

// readPreliminary reads the content before the first node.
func (mr *mlReader) readPreliminary() error {
	for {
		_, rc, err := mr.readRune()
		switch {
		case err != nil:
			return err
		case rc == rcEOF:
			return errors.New(ErrReader, errorMessages, "unexpected end of file while reading preliminary")
		case rc == rcOpen:
			return nil
		}
	}
}

// readNode reads the next tag node.
func (mr *mlReader) readTagNode() error {
	tag, rc, err := mr.readTag()
	if err != nil {
		return err
	}
	if err = mr.builder.BeginTagNode(tag); err != nil {
		return err
	}
	// Read children.
	if rc != rcClose {
		if err = mr.readTagChildren(); err != nil {
			return err
		}
	}
	return mr.builder.EndTagNode()
}

// readTag reads the tag of a node. It als returns the class of the next rune.
func (mr *mlReader) readTag() (string, int, error) {
	var buf bytes.Buffer
	for {
		r, rc, err := mr.readRune()
		switch {
		case err != nil:
			return "", 0, err
		case rc == rcEOF:
			return "", 0, errors.New(ErrReader, errorMessages, "unexpected end of file while reading a tag")
		case rc == rcTag:
			buf.WriteRune(r)
		case rc == rcSpace || rc == rcClose:
			return buf.String(), rc, nil
		default:
			msg := fmt.Sprintf("invalid tag run at position %d", mr.index)
			return "", 0, errors.New(ErrReader, errorMessages, msg)
		}
	}
}

// readChildren reads the children of passed parent tag node.
func (mr *mlReader) readTagChildren() error {
	for {
		_, rc, err := mr.readRune()
		switch {
		case err != nil:
			return err
		case rc == rcEOF:
			return errors.New(ErrReader, errorMessages, "unexpected end of file while reading children")
		case rc == rcClose:
			return nil
		case rc == rcOpen:
			if err = mr.readTagOrRawNode(); err != nil {
				return err
			}
		default:
			mr.index--
			mr.reader.UnreadRune()
			if err = mr.readTextNode(); err != nil {
				return err
			}
		}
	}
}

// readTagOrRawNode checks if the opening is for a tag node or
// for a raw node and starts the reading of it.
func (mr *mlReader) readTagOrRawNode() error {
	_, rc, err := mr.readRune()
	switch {
	case err != nil:
		return err
	case rc == rcEOF:
		return errors.New(ErrReader, errorMessages, "unexpected end of file while reading a tag or raw node")
	case rc == rcTag:
		mr.index--
		mr.reader.UnreadRune()
		return mr.readTagNode()
	case rc == rcExclamation:
		return mr.readRawNode()
	}
	msg := fmt.Sprintf("invalid rune after opening at index %d", mr.index)
	return errors.New(ErrReader, errorMessages, msg)
}

// readRawNode reads a raw node.
func (mr *mlReader) readRawNode() error {
	var buf bytes.Buffer
	for {
		r, rc, err := mr.readRune()
		switch {
		case err != nil:
			return err
		case rc == rcEOF:
			return errors.New(ErrReader, errorMessages, "unexpected end of file while reading a raw node")
		case rc == rcExclamation:
			r, rc, err = mr.readRune()
			switch {
			case err != nil:
				return err
			case rc == rcEOF:
				return errors.New(ErrReader, errorMessages, "unexpected end of file while reading a raw node")
			case rc == rcClose:
				return mr.builder.RawNode(buf.String())
			}
			buf.WriteRune('!')
			buf.WriteRune(r)
		default:
			buf.WriteRune(r)
		}
	}
}

// readTextNode reads a text node.
func (mr *mlReader) readTextNode() error {
	var buf bytes.Buffer
	for {
		r, rc, err := mr.readRune()
		switch {
		case err != nil:
			return err
		case rc == rcEOF:
			return errors.New(ErrReader, errorMessages, "unexpected end of file while reading a text node")
		case rc == rcOpen || rc == rcClose:
			mr.index--
			mr.reader.UnreadRune()
			return mr.builder.TextNode(buf.String())
		case rc == rcEscape:
			r, rc, err = mr.readRune()
			switch {
			case err != nil:
				return err
			case rc == rcEOF:
				return errors.New(ErrReader, errorMessages, "unexpected end of file while reading a text node")
			case rc == rcOpen || rc == rcClose || rc == rcEscape:
				buf.WriteRune(r)
			default:
				msg := fmt.Sprintf("invalid rune after escaping at index %d", mr.index)
				return errors.New(ErrReader, errorMessages, msg)
			}
		default:
			buf.WriteRune(r)
		}
	}
}

// Reads one rune of the reader.
func (mr *mlReader) readRune() (r rune, rc int, err error) {
	var size int
	mr.index++
	r, size, err = mr.reader.ReadRune()
	if err != nil {
		return 0, 0, err
	}
	switch {
	case size == 0:
		rc = rcEOF
	case r == '{':
		rc = rcOpen
	case r == '}':
		rc = rcClose
	case r == '^':
		rc = rcEscape
	case r == '!':
		rc = rcExclamation
	case r >= 'a' && r <= 'z':
		rc = rcTag
	case r >= 'A' && r <= 'Z':
		rc = rcTag
	case r >= '0' && r <= '9':
		rc = rcTag
	case r == '-' || r == ':':
		rc = rcTag
	case unicode.IsSpace(r):
		rc = rcSpace
	default:
		rc = rcText
	}
	return
}

// EOF
