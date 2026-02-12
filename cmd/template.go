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
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"text/template"
	"time"
)

// templateFuncMap returns the shared FuncMap used by both value templates
// (pda get) and commit message templates.
func templateFuncMap() template.FuncMap {
	return template.FuncMap{
		"require": func(v any) (string, error) {
			s := fmt.Sprint(v)
			if s == "" {
				return "", fmt.Errorf("required value is missing or empty")
			}
			return s, nil
		},
		"default": func(def string, v any) string {
			s := fmt.Sprint(v)
			if s == "" {
				return def
			}
			return s
		},
		"env": os.Getenv,
		"enum": func(v any, allowed ...string) (string, error) {
			s := fmt.Sprint(v)
			if s == "" {
				return "", fmt.Errorf("enum value is missing or empty")
			}
			if slices.Contains(allowed, s) {
				return s, nil
			}
			return "", fmt.Errorf("invalid value '%s', allowed: %v", s, allowed)
		},
		"int": func(v any) (int, error) {
			s := fmt.Sprint(v)
			i, err := strconv.Atoi(s)
			if err != nil {
				return 0, fmt.Errorf("cannot convert to int: %w", err)
			}
			return i, nil
		},
		"list": func(v any) []string {
			s := fmt.Sprint(v)
			if s == "" {
				return nil
			}
			parts := strings.Split(s, ",")
			for i := range parts {
				parts[i] = strings.TrimSpace(parts[i])
			}
			return parts
		},
		"time": func() string { return time.Now().UTC().Format(time.RFC3339) },
		"shell": func(command string) (string, error) {
			sh := os.Getenv("SHELL")
			if sh == "" {
				sh = "/bin/sh"
			}
			out, err := exec.Command(sh, "-c", command).Output()
			if err != nil {
				if exitErr, ok := err.(*exec.ExitError); ok && len(exitErr.Stderr) > 0 {
					return "", fmt.Errorf("shell %q: %s", command, strings.TrimSpace(string(exitErr.Stderr)))
				}
				return "", fmt.Errorf("shell %q: %w", command, err)
			}
			return strings.TrimRight(string(out), "\n"), nil
		},
		"pda": func(key string) (string, error) {
			return pdaGet(key, nil)
		},
	}
}

// cleanTemplateError strips Go template engine internals from function call
// errors, returning just the inner error message. Template execution errors
// look like: "template: cmd:1:3: executing "cmd" at <func args>: error calling func: <inner>"
// We extract just <inner> for cleaner user-facing output.
func cleanTemplateError(err error) error {
	msg := err.Error()
	const marker = "error calling "
	if i := strings.Index(msg, marker); i >= 0 {
		rest := msg[i+len(marker):]
		if j := strings.Index(rest, ": "); j >= 0 {
			return fmt.Errorf("%s", rest[j+2:])
		}
	}
	return err
}

const maxTemplateDepth = 16

func templateDepth() int {
	s := os.Getenv("PDA_TEMPLATE_DEPTH")
	if s == "" {
		return 0
	}
	n, _ := strconv.Atoi(s)
	return n
}

func pdaGet(key string, substitutions []string) (string, error) {
	depth := templateDepth()
	if depth >= maxTemplateDepth {
		return "", fmt.Errorf("pda: max template depth (%d) exceeded", maxTemplateDepth)
	}
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("pda: %w", err)
	}
	args := append([]string{"get", key}, substitutions...)
	cmd := exec.Command(exe, args...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("PDA_TEMPLATE_DEPTH=%d", depth+1))
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok && len(exitErr.Stderr) > 0 {
			msg := strings.TrimSpace(string(exitErr.Stderr))
			msg = strings.TrimPrefix(msg, "FAIL ")
			if strings.Contains(msg, "max template depth") {
				return "", fmt.Errorf("pda: max template depth (%d) exceeded (possible circular reference involving %q)", maxTemplateDepth, key)
			}
			return "", fmt.Errorf("pda: %s", msg)
		}
		return "", fmt.Errorf("pda: %w", err)
	}
	return strings.TrimRight(string(out), "\n"), nil
}
