package cmd

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var restoreCmd = &cobra.Command{
	Use:   "restore [DB]",
	Short: "Restore key/value pairs from an NDJSON dump",
	Args:  cobra.MaximumNArgs(1),
	RunE:  restore,
}

func restore(cmd *cobra.Command, args []string) error {
	store := &Store{}
	dbName := "default"
	if len(args) == 1 {
		parsed, err := store.parseDB(args[0], false)
		if err != nil {
			return err
		}
		dbName = parsed
	}

	reader, closer, err := restoreInput(cmd)
	if err != nil {
		return err
	}
	if closer != nil {
		defer closer.Close()
	}

	db, err := store.open(dbName)
	if err != nil {
		return err
	}
	defer db.Close()

	scanner := bufio.NewScanner(reader)
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, 8*1024*1024)

	wb := db.NewWriteBatch()
	defer wb.Cancel()

	lineNo := 0
	var restored int

	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var entry dumpEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return fmt.Errorf("line %d: %w", lineNo, err)
		}
		if entry.Key == "" {
			return fmt.Errorf("line %d: missing key", lineNo)
		}

		value, err := decodeEntryValue(entry)
		if err != nil {
			return fmt.Errorf("line %d: %w", lineNo, err)
		}

		if err := wb.Set([]byte(entry.Key), value); err != nil {
			return fmt.Errorf("line %d: %w", lineNo, err)
		}
		restored++
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	if err := wb.Flush(); err != nil {
		return err
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
	rootCmd.AddCommand(restoreCmd)
}
