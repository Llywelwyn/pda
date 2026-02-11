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
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"filippo.io/age"
	"github.com/gobwas/glob"
	"github.com/spf13/cobra"
)

var restoreCmd = &cobra.Command{
	Use:          "import [STORE]",
	Short:        "Restore key/value pairs from an NDJSON dump",
	Aliases:      []string{"restore"},
	Args:         cobra.MaximumNArgs(1),
	RunE:         restore,
	SilenceUsage: true,
}

func restore(cmd *cobra.Command, args []string) error {
	store := &Store{}
	dbName := config.Store.DefaultStoreName
	if len(args) == 1 {
		parsed, err := store.parseDB(args[0], false)
		if err != nil {
			return fmt.Errorf("cannot restore '%s': %v", args[0], err)
		}
		dbName = parsed
	}
	displayTarget := "@" + dbName

	globPatterns, err := cmd.Flags().GetStringSlice("glob")
	if err != nil {
		return fmt.Errorf("cannot restore '%s': %v", displayTarget, err)
	}
	separators, err := parseGlobSeparators(cmd)
	if err != nil {
		return fmt.Errorf("cannot restore '%s': %v", displayTarget, err)
	}
	matchers, err := compileGlobMatchers(globPatterns, separators)
	if err != nil {
		return fmt.Errorf("cannot restore '%s': %v", displayTarget, err)
	}

	reader, closer, err := restoreInput(cmd)
	if err != nil {
		return fmt.Errorf("cannot restore '%s': %v", displayTarget, err)
	}
	if closer != nil {
		defer closer.Close()
	}

	p, err := store.storePath(dbName)
	if err != nil {
		return fmt.Errorf("cannot restore '%s': %v", displayTarget, err)
	}

	decoder := json.NewDecoder(bufio.NewReaderSize(reader, 8*1024*1024))

	interactive, err := cmd.Flags().GetBool("interactive")
	if err != nil {
		return fmt.Errorf("cannot restore '%s': %v", displayTarget, err)
	}
	promptOverwrite := interactive || config.Key.AlwaysPromptOverwrite

	drop, err := cmd.Flags().GetBool("drop")
	if err != nil {
		return fmt.Errorf("cannot restore '%s': %v", displayTarget, err)
	}

	identity, _ := loadIdentity()
	var recipient *age.X25519Recipient
	if identity != nil {
		recipient = identity.Recipient()
	}

	restored, err := restoreEntries(decoder, p, restoreOpts{
		matchers:        matchers,
		promptOverwrite: promptOverwrite,
		drop:            drop,
		identity:        identity,
		recipient:       recipient,
	})
	if err != nil {
		return fmt.Errorf("cannot restore '%s': %v", displayTarget, err)
	}

	if len(matchers) > 0 && restored == 0 {
		return fmt.Errorf("cannot restore '%s': no matches for pattern %s", displayTarget, formatGlobPatterns(globPatterns))
	}

	okf("restored %d entries into @%s", restored, dbName)
	return autoSync()
}

func restoreInput(cmd *cobra.Command) (io.Reader, io.Closer, error) {
	filePath, err := cmd.Flags().GetString("file")
	if err != nil {
		return nil, nil, err
	}
	if strings.TrimSpace(filePath) == "" {
		return cmd.InOrStdin(), nil, nil
	}
	f, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	return f, f, nil
}

type restoreOpts struct {
	matchers        []glob.Glob
	promptOverwrite bool
	drop            bool
	identity        *age.X25519Identity
	recipient       *age.X25519Recipient
}

func restoreEntries(decoder *json.Decoder, storePath string, opts restoreOpts) (int, error) {
	var existing []Entry
	if !opts.drop {
		var err error
		existing, err = readStoreFile(storePath, opts.identity)
		if err != nil {
			return 0, err
		}
	}

	entryNo := 0
	restored := 0

	for {
		var je jsonEntry
		if err := decoder.Decode(&je); err != nil {
			if err == io.EOF {
				break
			}
			return 0, fmt.Errorf("entry %d: %w", entryNo+1, err)
		}
		entryNo++
		if je.Key == "" {
			return 0, fmt.Errorf("entry %d: missing key", entryNo)
		}
		if !globMatch(opts.matchers, je.Key) {
			continue
		}

		entry, err := decodeJsonEntry(je, opts.identity)
		if err != nil {
			return 0, fmt.Errorf("entry %d: %w", entryNo, err)
		}

		idx := findEntry(existing, entry.Key)

		if opts.promptOverwrite && idx >= 0 {
			promptf("overwrite '%s'? (y/n)", entry.Key)
			var confirm string
			if err := scanln(&confirm); err != nil {
				return 0, fmt.Errorf("entry %d: %v", entryNo, err)
			}
			if strings.ToLower(confirm) != "y" {
				continue
			}
		}

		if idx >= 0 {
			existing[idx] = entry
		} else {
			existing = append(existing, entry)
		}
		restored++
	}

	if restored > 0 || opts.drop {
		if err := writeStoreFile(storePath, existing, opts.recipient); err != nil {
			return 0, err
		}
	}
	return restored, nil
}

func init() {
	restoreCmd.Flags().StringP("file", "f", "", "Path to an NDJSON dump (defaults to stdin)")
	restoreCmd.Flags().StringSliceP("glob", "g", nil, "Restore keys matching glob pattern (repeatable)")
	restoreCmd.Flags().String("glob-sep", "", fmt.Sprintf("Characters treated as separators for globbing (default '%s')", defaultGlobSeparatorsDisplay()))
	restoreCmd.Flags().BoolP("interactive", "i", false, "Prompt before overwriting existing keys")
	restoreCmd.Flags().Bool("drop", false, "Drop existing entries before restoring (full replace)")
	rootCmd.AddCommand(restoreCmd)
}
