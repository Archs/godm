// Tideland Go Data Management - Simple Markup Language - Nodes
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
	"bytes"
	"fmt"
	"regexp"
	"strings"
)

//--------------------
// TAG NODE
//--------------------

// tagNode represents a node with one multipart tag and zero to many
// children nodes.
type tagNode struct {
	tag      []string
	children []Node
}

// newTagNode creates a node with the given tag.
func newTagNode(tag string) (*tagNode, error) {
	vtag, err := ValidateTag(tag)
	if err != nil {
		return nil, err
	}
	return &tagNode{
		tag:      vtag,
		children: []Node{},
	}, nil
}

// appendTagNode creates a new tag node, appends it as last child
// and returns it.
func (t *tagNode) appendTagNode(tag string) (*tagNode, error) {
	an, err := newTagNode(tag)
	if err != nil {
		return nil, err
	}
	t.appendChild(an)
	return an, nil
}

// appendTextNode create a text node, appends it as last child
// and returns it.
func (t *tagNode) appendTextNode(text string) *textNode {
	trimmedText := strings.TrimSpace(text)
	if trimmedText == "" {
		return nil
	}
	an := newTextNode(trimmedText)
	t.appendChild(an)
	return an
}

// appendRawNode create a raw node, appends it as last child
// and returns it.
func (t *tagNode) appendRawNode(raw string) *rawNode {
	an := newRawNode(raw)
	t.appendChild(an)
	return an
}

// appendChild adds a node as last child.
func (t *tagNode) appendChild(n Node) {
	t.children = append(t.children, n)
}

// Tag returns the tag parts.
func (t *tagNode) Tag() []string {
	out := make([]string, len(t.tag))
	copy(out, t.tag)
	return out
}

// Len return the number of children of this node.
func (t *tagNode) Len() int {
	return 1 + len(t.children)
}

// ProcessWith processes the node and all chidlren recursively
// with the passed processor.
func (t *tagNode) ProcessWith(p Processor) error {
	if err := p.OpenTag(t.tag); err != nil {
		return err
	}
	for _, child := range t.children {
		if err := child.ProcessWith(p); err != nil {
			return err
		}
	}
	return p.CloseTag(t.tag)
}

// String returns the tag node as string.
func (t *tagNode) String() string {
	var buf bytes.Buffer
	context := NewWriterContext(NewStandardSMLWriter(), &buf, true, "\t")
	WriteSML(t, context)
	return buf.String()
}

//--------------------
// TEXT NODE
//--------------------

// textNode is a node containing some text.
type textNode struct {
	text string
}

// newTextNode creates a new text node.
func newTextNode(text string) *textNode {
	return &textNode{strings.TrimSpace(text)}
}

// Tag returns nil.
func (t *textNode) Tag() []string {
	return nil
}

// Len returns the len of the text in the text node.
func (t *textNode) Len() int {
	return len(t.text)
}

// ProcessWith processes the text node with the given
// processor.
func (t *textNode) ProcessWith(p Processor) error {
	return p.Text(t.text)
}

// String returns the text node as string.
func (t *textNode) String() string {
	return t.text
}

//--------------------
// RAW NODE
//--------------------

// rawNode is a node containing some raw data.
type rawNode struct {
	raw string
}

// newRawNode creates a new raw node.
func newRawNode(raw string) *rawNode {
	return &rawNode{strings.TrimSpace(raw)}
}

// Tag returns nil.
func (t *rawNode) Tag() []string {
	return nil
}

// Len returns the len of the data in the raw node.
func (r *rawNode) Len() int {
	return len(r.raw)
}

// ProcessWith processes the raw node with the given
// processor.
func (r *rawNode) ProcessWith(p Processor) error {
	return p.Raw(r.raw)
}

// String returns the raw node as string.
func (r *rawNode) String() string {
	return r.raw
}

//--------------------
// PRIVATE FUNCTIONS
//--------------------

// validTagRe contains the regular expression for
// the validation of tags.
var validTagRe *regexp.Regexp

// init the regexp for valid tags.
func init() {
	var err error
	validTagRe, err = regexp.Compile(`^([a-z][a-z0-9]*(\-[a-z0-9]+)*)(:([a-z0-9]+(\-[a-z0-9]+)*))*$`)
	if err != nil {
		panic(err)
	}
}

// validateTag checks if a tag is valid. Only
// the chars 'a' to 'z', '0' to '9', '-' and ':' are
// accepted. It also transforms it to lowercase
// and splits the parts at the colons.
func ValidateTag(tag string) ([]string, error) {
	ltag := strings.ToLower(tag)
	if !validTagRe.MatchString(ltag) {
		return nil, fmt.Errorf("invalid tag: %q", tag)
	}
	ltags := strings.Split(ltag, ":")
	return ltags, nil
}

// EOF
