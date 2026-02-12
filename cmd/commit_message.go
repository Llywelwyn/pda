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
	"bytes"
	"text/template"
)

// renderCommitMessage renders a commit message template. It extends the
// shared template FuncMap with {{ summary }}, which returns the action
// description for the current commit. On any error the raw template string
// is returned so that commits are never blocked by a bad template.
func renderCommitMessage(tmpl string, summary string) string {
	funcMap := templateFuncMap()
	funcMap["summary"] = func() string { return summary }

	t, err := template.New("commit").Option("missingkey=zero").Funcs(funcMap).Parse(tmpl)
	if err != nil {
		return tmpl
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, nil); err != nil {
		return tmpl
	}
	return buf.String()
}
