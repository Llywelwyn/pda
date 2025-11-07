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
	"errors"
	"fmt"
	"strings"
	"text/tabwriter"

	"github.com/dgraph-io/badger/v4"
	"github.com/spf13/cobra"
)

// listCmd represents the set command
var listCmd = &cobra.Command{
	Use:   "list [DB]",
	Short: "List the contents of a db.",
	Args:  cobra.MaximumNArgs(1),
	RunE:  list,
}

func list(cmd *cobra.Command, args []string) error {
	store := &Store{}
	targetDB := "@default"
	if len(args) == 1 {
		rawArg := args[0]
		dbName, err := store.parseDB(rawArg, false)
		if err != nil {
			return err
		}
		if _, err := store.FindStore(dbName); err != nil {
			var notFound errNotFound
			if errors.As(err, &notFound) {
				return fmt.Errorf("%q does not exist, %s", rawArg, err.Error())
			}
			return err
		}
		targetDB = "@" + dbName
	}

	delimiter, err := cmd.Flags().GetString("delimiter")
	if err != nil {
		return err
	}

	includeSecret, err := cmd.Flags().GetBool("include-secret")
	if err != nil {
		return err
	}

	keysOnly, err := cmd.Flags().GetBool("only-keys")
	if err != nil {
		return err
	}
	valuesOnly, err := cmd.Flags().GetBool("only-values")
	if err != nil {
		return err
	}
	if keysOnly && valuesOnly {
		return fmt.Errorf("--only-keys and --only-values are mutually exclusive")
	}
	showExpiry, err := cmd.Flags().GetBool("show-expiry")
	if err != nil {
		return err
	}
	binary, err := cmd.Flags().GetBool("include-binary")
	if err != nil {
		return err
	}

	includeKey := !valuesOnly
	includeValue := !keysOnly
	prefetchVals := includeValue

	columnKinds := selectColumns(includeKey, includeValue, showExpiry)
	if len(columnKinds) == 0 {
		return fmt.Errorf("no columns selected; enable keys or values")
	}

	delimiterBytes := []byte(delimiter)
	columnCount := len(columnKinds)
	if len(delimiterBytes) > 0 && columnCount > 1 {
		columnCount = columnCount*2 - 1
	}
	format := buildTabbedFormat(columnCount)

	writer := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	defer writer.Flush()

	placeholder := []byte("[secret: pass --include-secret to view]")

	trans := TransactionArgs{
		key:      targetDB,
		readonly: true,
		sync:     true,
		transact: func(tx *badger.Txn, k []byte) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 10
			opts.PrefetchValues = prefetchVals
			it := tx.NewIterator(opts)
			defer it.Close()
			var valueBuf []byte
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				key := item.KeyCopy(nil)
				meta := item.UserMeta()
				isSecret := meta&metaSecret != 0
				valueBuf = valueBuf[:0]
				if includeValue && (!isSecret || includeSecret) {
					if err := item.Value(func(v []byte) error {
						valueBuf = append(valueBuf[:0], v...)
						return nil
					}); err != nil {
						return err
					}
				}
				columns := make([][]byte, 0, len(columnKinds))
				for _, column := range columnKinds {
					switch column {
					case columnKey:
						columns = append(columns, key)
					case columnValue:
						if isSecret && !includeSecret {
							columns = append(columns, placeholder)
						} else {
							columns = append(columns, valueBuf)
						}
					case columnExpiry:
						columns = append(columns, []byte(formatExpiry(item.ExpiresAt())))
					}
				}
				row := insertDelimiters(columns, delimiterBytes)
				store.PrintTo(writer, format, binary, row...)
			}
			return nil
		},
	}

	return store.Transaction(trans)
}

func init() {
	listCmd.Flags().BoolP("include-binary", "b", false, "include binary data in text output")
	listCmd.Flags().StringP("delimiter", "d", "", "string inserted between columns")
	listCmd.Flags().Bool("include-secret", false, "include entries marked as secret")
	listCmd.Flags().BoolP("only-keys", "k", false, "only print keys")
	listCmd.Flags().BoolP("only-values", "v", false, "only print values")
	listCmd.Flags().Bool("show-expiry", false, "append an expiry column when entries have TTLs")
	rootCmd.AddCommand(listCmd)
}

type columnKind int

const (
	columnKey columnKind = iota
	columnValue
	columnExpiry
)

func selectColumns(includeKey, includeValue, showExpiry bool) []columnKind {
	var columns []columnKind
	if includeKey {
		columns = append(columns, columnKey)
	}
	if includeValue {
		columns = append(columns, columnValue)
	}
	if showExpiry {
		columns = append(columns, columnExpiry)
	}
	return columns
}

func buildTabbedFormat(cols int) string {
	if cols <= 0 {
		return "\n"
	}
	var b strings.Builder
	for i := 0; i < cols; i++ {
		if i > 0 {
			b.WriteByte('\t')
		}
		b.WriteString("%s")
	}
	b.WriteByte('\n')
	return b.String()
}

func insertDelimiters(columns [][]byte, delimiter []byte) [][]byte {
	if len(delimiter) == 0 || len(columns) <= 1 {
		return columns
	}
	out := make([][]byte, 0, len(columns)*2-1)
	for i, col := range columns {
		out = append(out, col)
		if i < len(columns)-1 {
			out = append(out, delimiter)
		}
	}
	return out
}
