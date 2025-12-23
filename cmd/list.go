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

	"github.com/dgraph-io/badger/v4"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
)

var listCmd = &cobra.Command{
	Use:          "list [DB]",
	Short:        "List the contents of a database",
	Aliases:      []string{"ls"},
	Args:         cobra.MaximumNArgs(1),
	RunE:         list,
	SilenceUsage: true,
}

func list(cmd *cobra.Command, args []string) error {
	store := &Store{}
	targetDB := "@" + config.Store.DefaultStoreName
	if len(args) == 1 {
		rawArg := args[0]
		dbName, err := store.parseDB(rawArg, false)
		if err != nil {
			return fmt.Errorf("cannot ls '%s': %v", args[0], err)
		}
		if _, err := store.FindStore(dbName); err != nil {
			var notFound errNotFound
			if errors.As(err, &notFound) {
				return fmt.Errorf("cannot ls '%s': No such DB", args[0])
			}
			return fmt.Errorf("cannot ls '%s': %v", args[0], err)
		}
		targetDB = "@" + dbName
	}

	flags, err := enrichFlags()
	if err != nil {
		return fmt.Errorf("cannot ls '%s': %v", targetDB, err)
	}

	globPatterns, err := cmd.Flags().GetStringSlice("glob")
	if err != nil {
		return fmt.Errorf("cannot ls '%s': %v", targetDB, err)
	}
	separators, err := parseGlobSeparators(cmd)
	if err != nil {
		return fmt.Errorf("cannot ls '%s': %v", targetDB, err)
	}
	matchers, err := compileGlobMatchers(globPatterns, separators)
	if err != nil {
		return fmt.Errorf("cannot ls '%s': %v", targetDB, err)
	}

	columnKinds, err := requireColumns(flags)
	if err != nil {
		return fmt.Errorf("cannot ls '%s': %v", targetDB, err)
	}

	output := cmd.OutOrStdout()
	tw := table.NewWriter()
	tw.SetOutputMirror(output)
	tw.SetStyle(table.StyleDefault)
	// Should these be settable flags?
	tw.Style().Options.SeparateHeader = false
	tw.Style().Options.SeparateFooter = false
	tw.Style().Options.DrawBorder = false
	tw.Style().Options.SeparateRows = false
	tw.Style().Options.SeparateColumns = false

	var maxContentWidths []int
	maxContentWidths = make([]int, len(columnKinds))

	if flags.header {
		header := buildHeaderCells(columnKinds)
		updateMaxContentWidths(maxContentWidths, header)
		tw.AppendHeader(stringSliceToRow(header))
	}

	placeholder := "**********"
	var matchedCount int
	trans := TransactionArgs{
		key:      targetDB,
		readonly: true,
		sync:     true,
		transact: func(tx *badger.Txn, k []byte) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 10
			opts.PrefetchValues = flags.value
			it := tx.NewIterator(opts)
			defer it.Close()
			var valueBuf []byte
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				key := string(item.KeyCopy(nil))
				if !globMatch(matchers, key) {
					continue
				}
				matchedCount++
				meta := item.UserMeta()
				isSecret := meta&metaSecret != 0

				var valueStr string
				if flags.value && (!isSecret || flags.secrets) {
					if err := item.Value(func(v []byte) error {
						valueBuf = append(valueBuf[:0], v...)
						return nil
					}); err != nil {
						return fmt.Errorf("cannot ls '%s': %v", targetDB, err)
					}
					valueStr = store.FormatBytes(flags.binary, valueBuf)
				}

				columns := make([]string, 0, len(columnKinds))
				for _, column := range columnKinds {
					switch column {
					case columnKey:
						columns = append(columns, key)
					case columnValue:
						if isSecret && !flags.secrets {
							columns = append(columns, placeholder)
						} else {
							columns = append(columns, valueStr)
						}
					case columnTTL:
						columns = append(columns, formatExpiry(item.ExpiresAt()))
					}
				}
				updateMaxContentWidths(maxContentWidths, columns)
				tw.AppendRow(stringSliceToRow(columns))
			}
			return nil
		},
	}

	if err := store.Transaction(trans); err != nil {
		return err
	}

	if len(matchers) > 0 && matchedCount == 0 {
		return fmt.Errorf("cannot ls '%s': No matches for pattern %s", targetDB, formatGlobPatterns(globPatterns))
	}

	applyColumnConstraints(tw, columnKinds, output, maxContentWidths)

	flags.render(tw)
	return nil
}

func init() {
	listCmd.Flags().BoolVarP(&binary, "binary", "b", false, "include binary data in text output")
	listCmd.Flags().BoolVarP(&secret, "secret", "S", false, "display values marked as secret")
	listCmd.Flags().BoolVar(&noKeys, "no-keys", false, "suppress the key column")
	listCmd.Flags().BoolVar(&noValues, "no-values", false, "suppress the value column")
	listCmd.Flags().BoolVarP(&ttl, "ttl", "t", false, "append a TTL column when entries expire")
	listCmd.Flags().BoolVar(&header, "header", false, "include header row")
	listCmd.Flags().VarP(&format, "format", "o", "output format (table|tsv|csv|markdown|html)")
	listCmd.Flags().StringSliceP("glob", "g", nil, "Filter keys with glob pattern (repeatable)")
	listCmd.Flags().String("glob-sep", "", fmt.Sprintf("Characters treated as separators for globbing (default %q)", defaultGlobSeparatorsDisplay()))
	rootCmd.AddCommand(listCmd)
}
