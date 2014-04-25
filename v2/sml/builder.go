// Tideland Go Data Management - Simple Markup Language - Builder
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
	"github.com/tideland/goas/v3/errors"
)

//--------------------
// NODE BUILDER
//--------------------

// NodeBuilder creates a node structure when a SML
// document is read.
type NodeBuilder struct {
	stack []*tagNode
	done  bool
}

// NewNodeBuilder return a new nnode builder.
func NewNodeBuilder() *NodeBuilder {
	return &NodeBuilder{[]*tagNode{}, false}
}

// Root returns the root node of the read document.
func (n *NodeBuilder) Root() (Node, error) {
	if !n.done {
		return nil, errors.New(ErrBuilder, errorMessages, "building is not yet done")
	}
	return n.stack[0], nil
}

// BeginTagNode opens a new tag node.
func (n *NodeBuilder) BeginTagNode(tag string) error {
	if n.done {
		return errors.New(ErrBuilder, errorMessages, "building is already done")
	}
	t, err := newTagNode(tag)
	if err != nil {
		return err
	}
	n.stack = append(n.stack, t)
	return nil
}

// EndTagNode closes a new tag node.
func (n *NodeBuilder) EndTagNode() error {
	if n.done {
		return errors.New(ErrBuilder, errorMessages, "building is already done")
	}
	switch l := len(n.stack); l {
	case 0:
		return errors.New(ErrBuilder, errorMessages, "no opening tag")
	case 1:
		n.done = true
	default:
		n.stack[l-2].appendChild(n.stack[l-1])
		n.stack = n.stack[:l-1]
	}
	return nil
}

// TextNode appends a text node to the current open node.
func (n *NodeBuilder) TextNode(text string) error {
	if n.done {
		return errors.New(ErrBuilder, errorMessages, "building is already done")
	}
	if len(n.stack) > 0 {
		n.stack[len(n.stack)-1].appendTextNode(text)
		return nil
	}
	return errors.New(ErrBuilder, errorMessages, "no opening tag")
}

// RawNode appends a raw node to the current open node.
func (n *NodeBuilder) RawNode(raw string) error {
	if n.done {
		return errors.New(ErrBuilder, errorMessages, "building is already done")
	}
	if len(n.stack) > 0 {
		n.stack[len(n.stack)-1].appendRawNode(raw)
		return nil
	}
	return errors.New(ErrBuilder, errorMessages, "no opening tag")
}

// EOF
