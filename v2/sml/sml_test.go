// Tideland Go Data Management - Simple Markup Language - Unit Tests
//
// Copyright (C) 2009-2014 Frank Mueller / Tideland / Oldenburg / Germany
//
// All rights reserved. Use of this source code is governed
// by the new BSD license.

package sml_test

//--------------------
// IMPORTS
//--------------------

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/tideland/godm/v2/sml"
	"github.com/tideland/gots/v3/asserts"
)

//--------------------
// TESTS
//--------------------

func TestTagValidation(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	tests := []struct {
		in  string
		out []string
		ok  bool
	}{
		{"-abc", nil, false},
		{"-", nil, false},
		{"abc-", nil, false},
		{"ab-c", []string{"ab-c"}, true},
		{"abc", []string{"abc"}, true},
		{"ab:cd", []string{"ab", "cd"}, true},
		{"1a", nil, false},
		{"a1", []string{"a1"}, true},
		{"a:1", []string{"a", "1"}, true},
		{"a-b:c-d", []string{"a-b", "c-d"}, true},
		{"a-:c-d", nil, false},
		{"-a:c-d", nil, false},
		{"ab:-c", nil, false},
		{"ab:c-", nil, false},
		{"a-b-1", []string{"a-b-1"}, true},
		{"a-b-1:c-d-2:e-f-3", []string{"a-b-1", "c-d-2", "e-f-3"}, true},
	}
	for i, test := range tests {
		msg := fmt.Sprintf("%q (test %d) ", test.in, i)
		tag, err := sml.ValidateTag(test.in)
		if err == nil {
			assert.Equal(tag, test.out, msg)
			assert.True(test.ok, msg)
		} else {
			assert.ErrorMatch(err, fmt.Sprintf("invalid tag: %q", test.in), msg)
			assert.False(test.ok, msg)
		}
	}
}

// Test creating.
func TestCreating(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	root := createNodeStructure(assert)
	assert.Equal(root.Tag(), []string{"root"}, "Root tag has to be 'root'.")
	assert.NotEmpty(root, "Root tag is not empty.")
}

// Test SML writer processing.
func TestWriterProcessing(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	root := createNodeStructure(assert)
	bufA := bytes.NewBufferString("")
	bufB := bytes.NewBufferString("")
	ctxA := sml.NewWriterContext(sml.NewStandardSMLWriter(), bufA, true, "\t")
	ctxB := sml.NewWriterContext(sml.NewStandardSMLWriter(), bufB, false, "")

	sml.WriteSML(root, ctxA)
	sml.WriteSML(root, ctxB)

	println("===== WITH INDENT =====")
	println(bufA.String())
	println("===== WITHOUT INDENT =====")
	println(bufB.String())
	println("===== DONE =====")

	assert.NotEmpty(bufA, "Buffer A must not be empty.")
	assert.NotEmpty(bufB, "Buffer B must not be empty.")
}

// Test positive reading.
func TestPositiveReading(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	text := "Before!   {foo:main {bar:1:first Yadda ^{Test^} 1} {! Raw: }} { ! ^^^ !}  {inbetween}  {bar:2:last Yadda {Test ^^} 2}}   After!"
	builder := sml.NewNodeBuilder()
	err := sml.ReadSML(strings.NewReader(text), builder)
	assert.Nil(err)
	root, err := builder.Root()
	assert.Nil(err)
	assert.Equal(root.Tag(), []string{"foo", "main"})
	assert.NotEmpty(root)

	buf := bytes.NewBufferString("")
	ctx := sml.NewWriterContext(sml.NewStandardSMLWriter(), buf, true, "\t")
	sml.WriteSML(root, ctx)

	println("===== PARSED ML =====")
	println(buf.String())
	println("===== DONE =====")
}

// Test negative reading.
func TestNegativeReading(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	text := "{Foo {bar:1 Yadda {test} {} 1} {bar:2 Yadda 2}}"
	builder := sml.NewNodeBuilder()
	err := sml.ReadSML(strings.NewReader(text), builder)
	assert.ErrorMatch(err, `\[E.*\] cannot read SML document: invalid rune after opening at index .*`)
}

// Test reading an ML document and write it as XML.
func TestML2XML(t *testing.T) {
	assert := asserts.NewTestingAssertion(t, true)
	text := `{html
{head {title A test document}}
{body
  {h1:title A test document}
  {p:intro:preface The is a simple sentence with an {em emphasized}
  and a {strong strong} text. We'll see how it renders.}
  {ul
    {li:1 It should be nice.}
    {li:2 It should be error free.}
    {li:3 It should be fast.}
  }
  {pre {!
for foo := 0; foo < 42; foo++ {
	println(foo)
}
  !}}
}}`
	builder := sml.NewNodeBuilder()
	err := sml.ReadSML(strings.NewReader(text), builder)
	assert.Nil(err)
	root, err := builder.Root()
	assert.Nil(err)

	buf := bytes.NewBufferString("")
	ctx := sml.NewWriterContext(sml.NewXMLWriter(), buf, true, "\t")
	ctx.Register("pre", NewPreWriter())
	sml.WriteSML(root, ctx)

	println("===== XML =====")
	println(buf.String())
	println("===== DONE =====")
}

//--------------------
// HELPERS
//--------------------

// Create a node structure.
func createNodeStructure(assert asserts.Assertion) sml.Node {
	builder := sml.NewNodeBuilder()

	builder.BeginTagNode("root")
	builder.TextNode("Text A")
	builder.TextNode("Text B")
	builder.BeginTagNode("comment")
	builder.TextNode("A first comment.")
	builder.EndTagNode()
	builder.BeginTagNode("sub-a:1st:important")
	builder.TextNode("Text A.A")
	builder.BeginTagNode("comment")
	builder.TextNode("A second comment.")
	builder.EndTagNode()
	builder.EndTagNode()
	builder.BeginTagNode("sub-b:2nd")
	builder.TextNode("Text B.A")
	builder.BeginTagNode("text")
	builder.TextNode("Any text with the special characters {, }, and ^.")
	builder.EndTagNode()
	builder.EndTagNode()
	builder.BeginTagNode("sub-c")
	builder.TextNode("Before raw.")
	builder.RawNode("func Test(i int) { println(i) }")
	builder.TextNode("After raw.")
	builder.EndTagNode()
	builder.EndTagNode()

	root, err := builder.Root()
	assert.Nil(err)

	return root
}

// preWriter handles the pre-tag of the document.
type preWriter struct {
	context *sml.WriterContext
}

// NewPreWriter creates a new writer for the pre-tag.
func NewPreWriter() sml.WriterProcessor {
	return &preWriter{}
}

// SetContext sets the writer context.
func (w *preWriter) SetContext(ctx *sml.WriterContext) {
	w.context = ctx
}

// OpenTag writes the opening of a tag.
func (w *preWriter) OpenTag(tag []string) error {
	return w.context.Writef("<pre>")
}

// CloseTag writes the closing of a tag.
func (w *preWriter) CloseTag(tag []string) error {
	return w.context.Writef("</pre>")
}

// Text writes a text with an encoding of special runes.
func (w *preWriter) Text(text string) error {
	return w.context.Writef("<!-- %s -->", text)
}

// Raw write a raw data without any encoding.
func (w *preWriter) Raw(raw string) error {
	return w.context.Writef("\n%s\n", raw)
}

// EOF
