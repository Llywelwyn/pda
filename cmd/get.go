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
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"text/template"

	"github.com/dgraph-io/badger/v4"
	"github.com/spf13/cobra"
)

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get KEY[@DB]",
	Short: "Get the value of a key",
	Long: `Get the value of a key. Optionally specify a db.

{{ .TEMPLATES }} can be filled by passing TEMPLATE=VALUE as an
additional argument after the initial KEY being fetched.

For example:
	pda set greeting 'Hello, {{ .NAME }}!'
	pda get greeting NAME=World`,
	Aliases:      []string{"g"},
	Args:         cobra.MinimumNArgs(1),
	RunE:         get,
	SilenceUsage: true,
}

func get(cmd *cobra.Command, args []string) error {
	store := &Store{}

	var v []byte
	var meta byte
	trans := TransactionArgs{
		key:      args[0],
		readonly: true,
		sync:     false,
		transact: func(tx *badger.Txn, k []byte) error {
			item, err := tx.Get(k)
			if err != nil {
				return err
			}
			meta = item.UserMeta()
			v, err = item.ValueCopy(nil)
			return err
		},
	}

	if err := store.Transaction(trans); err != nil {
		return fmt.Errorf("cannot get '%s': %v", args[0], err)
	}

	includeSecret, err := cmd.Flags().GetBool("secret")
	if err != nil {
		return fmt.Errorf("cannot get '%s': %v", args[0], err)
	}
	if meta&metaSecret != 0 && !includeSecret {
		return fmt.Errorf("cannot get '%s': marked as secret, run with --secret", args[0])
	}

	binary, err := cmd.Flags().GetBool("include-binary")
	if err != nil {
		return fmt.Errorf("cannot get '%s': %v", args[0], err)
	}

	run, err := cmd.Flags().GetBool("run")
	if err != nil {
		return fmt.Errorf("cannot get '%s': %v", args[0], err)
	}

	noTemplate, err := cmd.Flags().GetBool("no-template")
	if err != nil {
		return fmt.Errorf("cannot get '%s': %v", args[0], err)
	}

	if !noTemplate {
		var substitutions []string
		if len(args) > 1 {
			substitutions = args[1:]
		}
		v, err = applyTemplate(v, substitutions)
		if err != nil {
			return fmt.Errorf("cannot get '%s': %v", args[0], err)
		}
	}

	if run {
		return runCmd(string(v))
	}

	store.Print("%s", binary, v)
	return nil
}

func applyTemplate(tplBytes []byte, substitutions []string) ([]byte, error) {
	vars := make(map[string]string, len(substitutions))
	for _, s := range substitutions {
		parts := strings.SplitN(s, "=", 2)
		if len(parts) != 2 || parts[0] == "" {
			fmt.Fprintf(os.Stderr, "invalid substitutions %q (expected KEY=VALUE)\n", s)
			continue
		}
		key := parts[0]
		val := parts[1]
		vars[key] = val
	}
	funcMap := template.FuncMap{
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
			return "", fmt.Errorf("invalid value %q (allowed: %v)", s, allowed)
		},
		"int": func(v any) (int, error) {
			s := fmt.Sprint(v)
			i, err := strconv.Atoi(s)
			if err != nil {
				return 0, fmt.Errorf("failed to convert to int: %w", err)
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
	}
	tpl, err := template.New("cmd").
		Delims("{{", "}}").
		// Render missing map keys as zero values so the default helper can decide on fallbacks.
		Option("missingkey=zero").
		Funcs(funcMap).
		Parse(string(tplBytes))
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, vars); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func runCmd(command string) error {
	shell := os.Getenv("SHELL")
	if shell == "" {
		shell = "/bin/sh"
	}

	cmd := exec.Command(shell, "-c", command)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	if err := cmd.Run(); err != nil {
		// ExitError indicates running the command was successful, but the command itself failed.
		// We only ever want to report on errors caused by the CLI tool itself.
		// An ExitError means this tool was successful in running the command, so return nil.
		// A non-ExitError means this tool failed, so return err.
		if _, ok := err.(*exec.ExitError); !ok {
			return err
		}
	}

	return nil
}

func init() {
	getCmd.Flags().BoolP("include-binary", "b", false, "include binary data in text output")
	getCmd.Flags().Bool("secret", false, "display values marked as secret")
	getCmd.Flags().BoolP("run", "c", false, "execute the result as a shell command")
	getCmd.Flags().Bool("no-template", false, "directly output template syntax")
	rootCmd.AddCommand(getCmd)
}
