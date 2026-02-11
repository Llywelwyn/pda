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
	"os"
	"strings"
	"time"

	"filippo.io/age"
	"github.com/spf13/cobra"
)

// setCmd represents the set command
var setCmd = &cobra.Command{
	Use:   "set KEY[@STORE] [VALUE]",
	Short: "Set a key to a given value",
	Long: `Set a key to a given value or stdin. Optionally specify a store.

Pass --encrypt to encrypt the value at rest using age. An identity file
is generated automatically on first use.

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
	safe, err := cmd.Flags().GetBool("safe")
	if err != nil {
		return err
	}
	promptOverwrite := interactive || config.Key.AlwaysPromptOverwrite

	secret, err := cmd.Flags().GetBool("encrypt")
	if err != nil {
		return err
	}

	spec, err := store.parseKey(args[0], true)
	if err != nil {
		return fmt.Errorf("cannot set '%s': %v", args[0], err)
	}

	filePath, err := cmd.Flags().GetString("file")
	if err != nil {
		return fmt.Errorf("cannot set '%s': %v", args[0], err)
	}

	var value []byte
	switch {
	case filePath != "" && len(args) == 2:
		return fmt.Errorf("cannot set '%s': --file and VALUE argument are mutually exclusive", args[0])
	case filePath != "":
		value, err = os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("cannot set '%s': %v", args[0], err)
		}
	case len(args) == 2:
		value = []byte(args[1])
	default:
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

	// Load or create identity depending on --encrypt flag
	var identity *age.X25519Identity
	if secret {
		identity, err = ensureIdentity()
		if err != nil {
			return fmt.Errorf("cannot set '%s': %v", args[0], err)
		}
	} else {
		identity, _ = loadIdentity()
	}
	var recipient *age.X25519Recipient
	if identity != nil {
		recipient = identity.Recipient()
	}

	p, err := store.storePath(spec.DB)
	if err != nil {
		return fmt.Errorf("cannot set '%s': %v", args[0], err)
	}
	entries, err := readStoreFile(p, identity)
	if err != nil {
		return fmt.Errorf("cannot set '%s': %v", args[0], err)
	}

	idx := findEntry(entries, spec.Key)

	if safe && idx >= 0 {
		infof("skipped '%s': already exists", spec.Display())
		return nil
	}

	// Warn if overwriting an encrypted key without --encrypt
	if idx >= 0 && entries[idx].Secret && !secret {
		warnf("overwriting encrypted key '%s' as plaintext", spec.Display())
		printHint("pass --encrypt to keep it encrypted")
	}

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
		Key:    spec.Key,
		Value:  value,
		Secret: secret,
	}
	if ttl != 0 {
		entry.ExpiresAt = uint64(time.Now().Add(ttl).Unix())
	}

	if idx >= 0 {
		entries[idx] = entry
	} else {
		entries = append(entries, entry)
	}

	if err := writeStoreFile(p, entries, recipient); err != nil {
		return fmt.Errorf("cannot set '%s': %v", args[0], err)
	}

	return autoSync()
}

func init() {
	rootCmd.AddCommand(setCmd)
	setCmd.Flags().DurationP("ttl", "t", 0, "expire the key after the provided duration (e.g. 24h, 30m)")
	setCmd.Flags().BoolP("interactive", "i", false, "prompt before overwriting an existing key")
	setCmd.Flags().BoolP("encrypt", "e", false, "encrypt the value at rest using age")
	setCmd.Flags().Bool("safe", false, "do not overwrite if the key already exists")
	setCmd.Flags().StringP("file", "f", "", "read value from a file")
}
