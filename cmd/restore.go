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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/spf13/cobra"
)

var restoreCmd = &cobra.Command{
	Use:          "restore [DB]",
	Short:        "Restore key/value pairs from an NDJSON dump",
	Aliases:      []string{"import"},
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

	db, err := store.open(dbName)
	if err != nil {
		return fmt.Errorf("cannot restore '%s': %v", displayTarget, err)
	}
	defer db.Close()

	decoder := json.NewDecoder(bufio.NewReaderSize(reader, 8*1024*1024))

	wb := db.NewWriteBatch()
	defer wb.Cancel()

	interactive, err := cmd.Flags().GetBool("interactive")
	if err != nil {
		return fmt.Errorf("cannot restore '%s': %v", displayTarget, err)
	}
	promptOverwrite := interactive || config.Key.AlwaysPromptOverwrite

	entryNo := 0
	var restored int
	var matched bool

	for {
		var entry dumpEntry
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("cannot restore '%s': entry %d: %w", displayTarget, entryNo+1, err)
		}
		entryNo++
		if entry.Key == "" {
			return fmt.Errorf("cannot restore '%s': entry %d: missing key", displayTarget, entryNo)
		}
		if !globMatch(matchers, entry.Key) {
			continue
		}

		if promptOverwrite {
			exists, err := keyExistsInDB(db, entry.Key)
			if err != nil {
				return fmt.Errorf("cannot restore '%s': entry %d: %v", displayTarget, entryNo, err)
			}
			if exists {
				fmt.Printf("overwrite '%s'? (y/n)\n", entry.Key)
				var confirm string
				if _, err := fmt.Scanln(&confirm); err != nil {
					return fmt.Errorf("cannot restore '%s': entry %d: %v", displayTarget, entryNo, err)
				}
				if strings.ToLower(confirm) != "y" {
					continue
				}
			}
		}

		value, err := decodeEntryValue(entry)
		if err != nil {
			return fmt.Errorf("cannot restore '%s': entry %d: %w", displayTarget, entryNo, err)
		}

		entryMeta := byte(0x0)
		if entry.Secret {
			entryMeta = metaSecret
		}

		writeEntry := badger.NewEntry([]byte(entry.Key), value).WithMeta(entryMeta)
		if entry.ExpiresAt != nil {
			if *entry.ExpiresAt < 0 {
				return fmt.Errorf("cannot restore '%s': entry %d: expires_at must be >= 0", displayTarget, entryNo)
			}
			writeEntry.ExpiresAt = uint64(*entry.ExpiresAt)
		}

		if err := wb.SetEntry(writeEntry); err != nil {
			return fmt.Errorf("cannot restore '%s': entry %d: %w", displayTarget, entryNo, err)
		}
		restored++
		matched = true
	}

	if err := wb.Flush(); err != nil {
		return fmt.Errorf("cannot restore '%s': %v", displayTarget, err)
	}

	if len(matchers) > 0 && !matched {
		return fmt.Errorf("cannot restore '%s': No matches for pattern %s", displayTarget, formatGlobPatterns(globPatterns))
	}

	fmt.Fprintf(cmd.ErrOrStderr(), "Restored %d entries into @%s\n", restored, dbName)
	msg := fmt.Sprintf("restore @%s (%d entries)", dbName, restored)
	return autoCommit(store, []string{dbName}, msg)
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

func decodeEntryValue(entry dumpEntry) ([]byte, error) {
	switch entry.Encoding {
	case "", "text":
		return []byte(entry.Value), nil
	case "base64":
		b, err := base64.StdEncoding.DecodeString(entry.Value)
		if err != nil {
			return nil, err
		}
		return b, nil
	default:
		return nil, fmt.Errorf("unsupported encoding %q", entry.Encoding)
	}
}

func init() {
	restoreCmd.Flags().StringP("file", "f", "", "Path to an NDJSON dump (defaults to stdin)")
	restoreCmd.Flags().StringSliceP("glob", "g", nil, "Restore keys matching glob pattern (repeatable)")
	restoreCmd.Flags().String("glob-sep", "", fmt.Sprintf("Characters treated as separators for globbing (default %q)", defaultGlobSeparatorsDisplay()))
	restoreCmd.Flags().BoolP("interactive", "i", false, "Prompt before overwriting existing keys")
	rootCmd.AddCommand(restoreCmd)
}

func keyExistsInDB(db *badger.DB, key string) (bool, error) {
	var exists bool
	err := db.View(func(tx *badger.Txn) error {
		_, err := tx.Get([]byte(key))
		if err == nil {
			exists = true
			return nil
		}
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
	return exists, err
}
