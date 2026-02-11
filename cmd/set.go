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
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// setCmd represents the set command
var setCmd = &cobra.Command{
	Use:   "set KEY[@STORE] [VALUE]",
	Short: "Set a key to a given value",
	Long: `Set a key to a given value or stdin. Optionally specify a store.

PDA supports parsing Go templates. Actions are delimited with {{ }}.

For example:
  'Hello, {{ .NAME }}'                 can be substituted with NAME="John Doe".
  'Hello, {{ env "USER" }}'            will fetch the USER env variable.
  'Hello, {{ default "World" .NAME }}' will default to World if NAME is blank.
  'Hello, {{ require .NAME }}'         will error if NAME is blank.
  '{{ enum .NAME "Alice" "Bob" }}'     allows only NAME=Alice or NAME=Bob.`,
	Aliases:      []string{"s"},
	Args:         cobra.RangeArgs(1, 2),
	RunE:         set,
	SilenceUsage: true,
}

func set(cmd *cobra.Command, args []string) error {
	store := &Store{}

	interactive, err := cmd.Flags().GetBool("interactive")
	if err != nil {
		return err
	}
	promptOverwrite := interactive || config.Key.AlwaysPromptOverwrite

	spec, err := store.parseKey(args[0], true)
	if err != nil {
		return fmt.Errorf("cannot set '%s': %v", args[0], err)
	}

	var value []byte
	if len(args) == 2 {
		value = []byte(args[1])
	} else {
		bytes, err := io.ReadAll(cmd.InOrStdin())
		if err != nil {
			return fmt.Errorf("cannot set '%s': %v", args[0], err)
		}
		value = bytes
	}

	ttl, err := cmd.Flags().GetDuration("ttl")
	if err != nil {
		return fmt.Errorf("cannot set '%s': %v", args[0], err)
	}

	p, err := store.storePath(spec.DB)
	if err != nil {
		return fmt.Errorf("cannot set '%s': %v", args[0], err)
	}
	entries, err := readStoreFile(p)
	if err != nil {
		return fmt.Errorf("cannot set '%s': %v", args[0], err)
	}

	idx := findEntry(entries, spec.Key)

	if promptOverwrite && idx >= 0 {
		promptf("overwrite '%s'? (y/n)", spec.Display())
		var confirm string
		if err := scanln(&confirm); err != nil {
			return fmt.Errorf("cannot set '%s': %v", args[0], err)
		}
		if strings.ToLower(confirm) != "y" {
			return nil
		}
	}

	entry := Entry{
		Key:   spec.Key,
		Value: value,
	}
	if ttl != 0 {
		entry.ExpiresAt = uint64(time.Now().Add(ttl).Unix())
	}

	if idx >= 0 {
		entries[idx] = entry
	} else {
		entries = append(entries, entry)
	}

	if err := writeStoreFile(p, entries); err != nil {
		return fmt.Errorf("cannot set '%s': %v", args[0], err)
	}

	return autoSync()
}

func init() {
	rootCmd.AddCommand(setCmd)
	setCmd.Flags().DurationP("ttl", "t", 0, "Expire the key after the provided duration (e.g. 24h, 30m)")
	setCmd.Flags().BoolP("interactive", "i", false, "Prompt before overwriting an existing key")
}
