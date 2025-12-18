/*
Copyright © 2025 Lewis Wynne <lew@ily.rs>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package cmd

import (
	"fmt"

	"github.com/jedib0t/go-pretty/v6/table"
)

// ListArgs tracks the resolved flag configuration for the list command.
type ListArgs struct {
	header  bool
	key     bool
	value   bool
	ttl     bool
	binary  bool
	secrets bool
	render  func(table.Writer)
}

// formatEnum implements pflag.Value for format selection.
type formatEnum string

func (e *formatEnum) String() string {
	return string(*e)
}

func (e *formatEnum) Set(v string) error {
	switch v {
	case "table", "tsv", "csv", "html", "markdown":
		*e = formatEnum(v)
		return nil
	default:
		return fmt.Errorf("must be one of \"table\", \"tsv\", \"csv\", \"html\", or \"markdown\"")
	}
}

func (e *formatEnum) Type() string {
	return "format"
}

var (
	binary   bool       = false
	secret   bool       = false
	noKeys   bool       = false
	noValues bool       = false
	ttl      bool       = false
	header   bool       = false
	format   formatEnum = "table"
)

func enrichFlags() (ListArgs, error) {
	var renderFunc func(tw table.Writer)
	switch format.String() {
	case "tsv":
		renderFunc = func(tw table.Writer) { tw.RenderTSV() }
	case "csv":
		renderFunc = func(tw table.Writer) { tw.RenderCSV() }
	case "html":
		renderFunc = func(tw table.Writer) { tw.RenderHTML() }
	case "markdown":
		renderFunc = func(tw table.Writer) { tw.RenderMarkdown() }
	case "table":
		renderFunc = func(tw table.Writer) { tw.Render() }
	}

	if noKeys && noValues && !ttl {
		return ListArgs{}, fmt.Errorf("no columns selected; disable --no-keys/--no-values or pass --ttl")
	}

	return ListArgs{
		header:  header,
		key:     !noKeys,
		value:   !noValues,
		ttl:     ttl,
		binary:  binary,
		render:  renderFunc,
		secrets: secret,
	}, nil
}
