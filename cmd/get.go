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
	"strings"
	"text/template"

	"github.com/spf13/cobra"
)

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get KEY[@STORE]",
	Short: "Get the value of a key",
	Long: `Get the value of a key. Optionally specify a store.

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

var runCmd = &cobra.Command{
	Use:   "run KEY[@STORE]",
	Short: "Get the value of a key and execute it",
	Long: `Get the value of a key and execute it as a shell command. Optionally specify a store.

{{ .TEMPLATES }} can be filled by passing TEMPLATE=VALUE as an
additional argument after the initial KEY being fetched.

For example:
	pda set greeting 'Hello, {{ .NAME }}!'
	pda run greeting NAME=World`,
	Args:         cobra.MinimumNArgs(1),
	RunE:         run,
	SilenceUsage: true,
}

func get(cmd *cobra.Command, args []string) error {
	store := &Store{}

	identity, _ := loadIdentity()

	spec, err := store.parseKey(args[0], true)
	if err != nil {
		return fmt.Errorf("cannot get '%s': %v", args[0], err)
	}
	p, err := store.storePath(spec.DB)
	if err != nil {
		return fmt.Errorf("cannot get '%s': %v", args[0], err)
	}
	entries, err := readStoreFile(p, identity)
	if err != nil {
		return fmt.Errorf("cannot get '%s': %v", args[0], err)
	}
	idx := findEntry(entries, spec.Key)

	existsOnly, _ := cmd.Flags().GetBool("exists")
	if existsOnly {
		if idx < 0 {
			os.Exit(1)
		}
		return nil
	}

	if idx < 0 {
		keys := make([]string, len(entries))
		for i, e := range entries {
			keys[i] = e.Key
		}
		return fmt.Errorf("cannot get '%s': %w", args[0], suggestKey(spec.Key, keys))
	}
	entry := entries[idx]
	if entry.Locked {
		return fmt.Errorf("cannot get '%s': secret is locked (identity file missing)", spec.Display())
	}
	v := entry.Value

	binary, err := cmd.Flags().GetBool("base64")
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

	if runFlag {
		return runShellCommand(string(v))
	}

	store.Print("%s", binary, v)
	return nil
}

func applyTemplate(tplBytes []byte, substitutions []string) ([]byte, error) {
	vars := make(map[string]string, len(substitutions))
	for _, s := range substitutions {
		parts := strings.SplitN(s, "=", 2)
		if len(parts) != 2 || parts[0] == "" {
			warnf("invalid substitution '%s', expected KEY=VALUE", s)
			continue
		}
		key := parts[0]
		val := parts[1]
		vars[key] = val
	}
	funcMap := templateFuncMap()
	funcMap["pda"] = func(key string) (string, error) {
		return pdaGet(key, substitutions)
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
		return nil, cleanTemplateError(err)
	}
	return buf.Bytes(), nil
}

func runShellCommand(command string) error {
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

func run(cmd *cobra.Command, args []string) error {
	runFlag = true
	return get(cmd, args)
}

var runFlag bool

func init() {
	getCmd.Flags().BoolP("base64", "b", false, "view binary data as base64")
	getCmd.Flags().BoolVarP(&runFlag, "run", "c", false, "execute the result as a shell command")
	getCmd.Flags().Bool("no-template", false, "directly output template syntax")
	getCmd.Flags().Bool("exists", false, "exit 0 if the key exists, exit 1 if not (no output)")
	rootCmd.AddCommand(getCmd)

	runCmd.Flags().BoolP("base64", "b", false, "view binary data as base64")
	runCmd.Flags().Bool("no-template", false, "directly output template syntax")
	rootCmd.AddCommand(runCmd)
}
