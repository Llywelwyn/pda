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
	dbName := "default"
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
		return fmt.Errorf("cannot restore '%s': No matches for pattern", displayTarget)
	}

	fmt.Fprintf(cmd.ErrOrStderr(), "Restored %d entries into @%s\n", restored, dbName)
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
	rootCmd.AddCommand(restoreCmd)
}
