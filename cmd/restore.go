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
	Use:               "import [STORE]",
	Short:             "Restore key/value pairs from an NDJSON dump",
	Aliases:           []string{},
	Args:              cobra.MaximumNArgs(1),
	ValidArgsFunction: completeStores,
	RunE:              restore,
	SilenceUsage:      true,
}

func restore(cmd *cobra.Command, args []string) error {
	store := &Store{}
	explicitStore := len(args) == 1
	targetDB := config.Store.DefaultStoreName
	if explicitStore {
		parsed, err := store.parseDB(args[0], false)
		if err != nil {
			return fmt.Errorf("cannot restore '%s': %v", args[0], err)
		}
		targetDB = parsed
	}
	displayTarget := "@" + targetDB

	keyPatterns, err := cmd.Flags().GetStringSlice("key")
	if err != nil {
		return fmt.Errorf("cannot restore '%s': %v", displayTarget, err)
	}
	matchers, err := compileGlobMatchers(keyPatterns)
	if err != nil {
		return fmt.Errorf("cannot restore '%s': %v", displayTarget, err)
	}

	storePatterns, err := cmd.Flags().GetStringSlice("store")
	if err != nil {
		return fmt.Errorf("cannot restore '%s': %v", displayTarget, err)
	}
	storeMatchers, err := compileGlobMatchers(storePatterns)
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
	recipients, err := allRecipients(identity)
	if err != nil {
		return fmt.Errorf("cannot restore '%s': %v", displayTarget, err)
	}

	var promptReader io.Reader
	if promptOverwrite {
		filePath, _ := cmd.Flags().GetString("file")
		if strings.TrimSpace(filePath) == "" {
			tty, err := os.Open("/dev/tty")
			if err != nil {
				return fmt.Errorf("cannot restore '%s': --interactive requires --file (-f) when reading from stdin on this platform", displayTarget)
			}
			defer tty.Close()
			promptReader = tty
		}
	}

	opts := restoreOpts{
		matchers:        matchers,
		storeMatchers:   storeMatchers,
		promptOverwrite: promptOverwrite,
		drop:            drop,
		identity:        identity,
		recipients:      recipients,
		promptReader:    promptReader,
	}

	// When a specific store is given, all entries go there (original behaviour).
	// Otherwise, route entries to their original store via the "store" field.
	var summary string
	if explicitStore {
		p, err := store.storePath(targetDB)
		if err != nil {
			return fmt.Errorf("cannot restore '%s': %v", displayTarget, err)
		}
		restored, err := restoreEntries(decoder, map[string]string{targetDB: p}, targetDB, opts)
		if err != nil {
			return fmt.Errorf("cannot restore '%s': %v", displayTarget, err)
		}
		if err := reportRestoreFilters(displayTarget, restored, matchers, keyPatterns, storeMatchers, storePatterns); err != nil {
			return err
		}
		okf("restored %d entries into @%s", restored, targetDB)
		summary = fmt.Sprintf("imported %d entries into @%s", restored, targetDB)
	} else {
		restored, err := restoreEntries(decoder, nil, targetDB, opts)
		if err != nil {
			return fmt.Errorf("cannot restore: %v", err)
		}
		if err := reportRestoreFilters(displayTarget, restored, matchers, keyPatterns, storeMatchers, storePatterns); err != nil {
			return err
		}
		okf("restored %d entries", restored)
		summary = fmt.Sprintf("imported %d entries", restored)
	}

	return autoSync(summary)
}

func reportRestoreFilters(displayTarget string, restored int, matchers []glob.Glob, keyPatterns []string, storeMatchers []glob.Glob, storePatterns []string) error {
	hasFilters := len(matchers) > 0 || len(storeMatchers) > 0
	if hasFilters && restored == 0 {
		var parts []string
		if len(matchers) > 0 {
			parts = append(parts, fmt.Sprintf("key pattern %s", formatGlobPatterns(keyPatterns)))
		}
		if len(storeMatchers) > 0 {
			parts = append(parts, fmt.Sprintf("store pattern %s", formatGlobPatterns(storePatterns)))
		}
		return fmt.Errorf("cannot restore '%s': no matches for %s", displayTarget, strings.Join(parts, " and "))
	}
	return nil
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
	storeMatchers   []glob.Glob
	promptOverwrite bool
	drop            bool
	identity        *age.X25519Identity
	recipients      []age.Recipient
	promptReader    io.Reader
}

// restoreEntries decodes NDJSON entries and writes them to store files.
// storePaths maps store names to file paths. If nil, entries are routed to
// their original store (from the "store" field), falling back to defaultDB.
func restoreEntries(decoder *json.Decoder, storePaths map[string]string, defaultDB string, opts restoreOpts) (int, error) {
	s := &Store{}

	// Per-store accumulator.
	type storeAcc struct {
		path    string
		entries []Entry
		loaded  bool
	}
	stores := make(map[string]*storeAcc)

	getStore := func(dbName string) (*storeAcc, error) {
		if acc, ok := stores[dbName]; ok {
			return acc, nil
		}
		var p string
		if storePaths != nil {
			var ok bool
			p, ok = storePaths[dbName]
			if !ok {
				return nil, fmt.Errorf("unexpected store '%s'", dbName)
			}
		} else {
			var err error
			p, err = s.storePath(dbName)
			if err != nil {
				return nil, err
			}
		}
		acc := &storeAcc{path: p}
		if !opts.drop {
			existing, err := readStoreFile(p, opts.identity)
			if err != nil {
				return nil, err
			}
			acc.entries = existing
		}
		acc.loaded = true
		stores[dbName] = acc
		return acc, nil
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
		if !globMatch(opts.storeMatchers, je.Store) {
			continue
		}

		// Determine target store.
		targetDB := defaultDB
		if storePaths == nil && je.Store != "" {
			targetDB = je.Store
		}

		entry, err := decodeJsonEntry(je, opts.identity)
		if err != nil {
			return 0, fmt.Errorf("entry %d: %w", entryNo, err)
		}

		acc, err := getStore(targetDB)
		if err != nil {
			return 0, fmt.Errorf("entry %d: %v", entryNo, err)
		}

		idx := findEntry(acc.entries, entry.Key)

		if opts.promptOverwrite && idx >= 0 {
			promptf("overwrite '%s'? (y/n)", entry.Key)
			var confirm string
			if opts.promptReader != nil {
				fmt.Fprintf(os.Stdout, "%s  ", keyword("2", "==>", stdoutIsTerminal()))
				if _, err := fmt.Fscanln(opts.promptReader, &confirm); err != nil {
					return 0, fmt.Errorf("entry %d: %v", entryNo, err)
				}
			} else {
				if err := scanln(&confirm); err != nil {
					return 0, fmt.Errorf("entry %d: %v", entryNo, err)
				}
			}
			if strings.ToLower(confirm) != "y" {
				continue
			}
		}

		if idx >= 0 {
			acc.entries[idx] = entry
		} else {
			acc.entries = append(acc.entries, entry)
		}
		restored++
	}

	for _, acc := range stores {
		if restored > 0 || opts.drop {
			if err := writeStoreFile(acc.path, acc.entries, opts.recipients); err != nil {
				return 0, err
			}
		}
	}
	return restored, nil
}

func init() {
	restoreCmd.Flags().StringP("file", "f", "", "path to an NDJSON dump (defaults to stdin)")
	restoreCmd.Flags().StringSliceP("key", "k", nil, "restore keys matching glob pattern (repeatable)")
	restoreCmd.Flags().StringSliceP("store", "s", nil, "restore entries from stores matching glob pattern (repeatable)")
	restoreCmd.RegisterFlagCompletionFunc("store", completeStoreFlag)
	restoreCmd.Flags().BoolP("interactive", "i", false, "prompt before overwriting existing keys")
	restoreCmd.Flags().Bool("drop", false, "drop existing entries before restoring (full replace)")
	rootCmd.AddCommand(restoreCmd)
}
