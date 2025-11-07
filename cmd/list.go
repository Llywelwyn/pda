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
	"io"
	"os"
	"strconv"

	"github.com/dgraph-io/badger/v4"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	"golang.org/x/term"
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

	showSecrets, err := cmd.Flags().GetBool("secret")
	if err != nil {
		return err
	}
	noKeys, err := cmd.Flags().GetBool("no-keys")
	if err != nil {
		return err
	}
	noValues, err := cmd.Flags().GetBool("no-values")
	if err != nil {
		return err
	}
	showTTL, err := cmd.Flags().GetBool("ttl")
	if err != nil {
		return err
	}
	noHeader, err := cmd.Flags().GetBool("no-header")
	if err != nil {
		return err
	}
	includeBinary, err := cmd.Flags().GetBool("binary")
	if err != nil {
		return err
	}
	format, err := cmd.Flags().GetString("format")
	if err != nil {
		return err
	}
	switch format {
	case "auto", "table", "tabular", "csv", "html", "markdown", "md":
	default:
		return fmt.Errorf("unsupported format %q", format)
	}

	includeKey := !noKeys
	includeValue := !noValues
	if !includeKey && !includeValue && !showTTL {
		return fmt.Errorf("no columns selected; disable --no-keys/--no-values or pass --ttl")
	}

	columnKinds := selectColumns(includeKey, includeValue, showTTL)
	if len(columnKinds) == 0 {
		return fmt.Errorf("no columns selected; enable key, value, or ttl output")
	}

	output := cmd.OutOrStdout()
	tw := table.NewWriter()
	tw.SetOutputMirror(output)
	configureListTable(tw)
	if shouldLimitColumns(format) {
		applyColumnConstraints(tw, columnKinds, output)
	}

	if !noHeader {
		header := buildHeaderCells(columnKinds)
		tw.AppendHeader(stringSliceToRow(header))
	}

	placeholder := "[secret: pass --secret to view]"

	trans := TransactionArgs{
		key:      targetDB,
		readonly: true,
		sync:     true,
		transact: func(tx *badger.Txn, k []byte) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 10
			opts.PrefetchValues = includeValue
			it := tx.NewIterator(opts)
			defer it.Close()
			var valueBuf []byte
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				key := string(item.KeyCopy(nil))
				meta := item.UserMeta()
				isSecret := meta&metaSecret != 0

				var valueStr string
				if includeValue && (!isSecret || showSecrets) {
					if err := item.Value(func(v []byte) error {
						valueBuf = append(valueBuf[:0], v...)
						return nil
					}); err != nil {
						return err
					}
					valueStr = store.FormatBytes(includeBinary, valueBuf)
				}

				columns := make([]string, 0, len(columnKinds))
				for _, column := range columnKinds {
					switch column {
					case columnKey:
						columns = append(columns, key)
					case columnValue:
						if isSecret && !showSecrets {
							columns = append(columns, placeholder)
						} else {
							columns = append(columns, valueStr)
						}
					case columnTTL:
						columns = append(columns, formatExpiry(item.ExpiresAt()))
					}
				}

				tw.AppendRow(stringSliceToRow(columns))
			}
			return nil
		},
	}

	if err := store.Transaction(trans); err != nil {
		return err
	}

	switch format {
	case "auto", "table", "tabular":
		tw.Render()
	case "csv":
		tw.RenderCSV()
	case "html":
		tw.RenderHTML()
	case "markdown", "md":
		tw.RenderMarkdown()
	}
	return nil
}

func init() {
	listCmd.Flags().BoolP("binary", "b", false, "include binary data in text output")
	listCmd.Flags().BoolP("secret", "S", false, "display values marked as secret")
	listCmd.Flags().Bool("no-keys", false, "suppress the key column")
	listCmd.Flags().Bool("no-values", false, "suppress the value column")
	listCmd.Flags().BoolP("ttl", "t", false, "append a TTL column when entries expire")
	listCmd.Flags().Bool("no-header", false, "omit the header rows")
	listCmd.Flags().StringP("format", "f", "table", "supports: table, csv, html, markdown")
	rootCmd.AddCommand(listCmd)
}

type columnKind int

const (
	columnKey columnKind = iota
	columnValue
	columnTTL
)

func selectColumns(includeKey, includeValue, showTTL bool) []columnKind {
	var columns []columnKind
	if includeKey {
		columns = append(columns, columnKey)
	}
	if includeValue {
		columns = append(columns, columnValue)
	}
	if showTTL {
		columns = append(columns, columnTTL)
	}
	return columns
}

func buildHeaderCells(columnKinds []columnKind) []string {
	labels := make([]string, 0, len(columnKinds))
	for _, column := range columnKinds {
		switch column {
		case columnKey:
			labels = append(labels, "Key")
		case columnValue:
			labels = append(labels, "Value")
		case columnTTL:
			labels = append(labels, "TTL")
		}
	}
	return labels
}

func stringSliceToRow(values []string) table.Row {
	row := make(table.Row, len(values))
	for i, val := range values {
		row[i] = val
	}
	return row
}

func configureListTable(tw table.Writer) {
	tw.SetStyle(table.StyleColoredBlackOnGreenWhite)
}

func shouldLimitColumns(format string) bool {
	switch format {
	case "auto", "table", "tabular":
		return true
	default:
		return false
	}
}

func applyColumnConstraints(tw table.Writer, columns []columnKind, out io.Writer) {
	totalWidth := detectTerminalWidth(out)
	if totalWidth <= 0 {
		totalWidth = 100
	}
	widths := distributeWidths(totalWidth, columns)
	configs := make([]table.ColumnConfig, 0, len(columns))
	for idx, width := range widths {
		configs = append(configs, table.ColumnConfig{
			Number:           idx + 1,
			WidthMax:         width,
			WidthMaxEnforcer: text.WrapText,
		})
	}
	tw.SetColumnConfigs(configs)
	tw.SetAllowedRowLength(totalWidth)
}

type fdWriter interface {
	Fd() uintptr
}

func detectTerminalWidth(out io.Writer) int {
	if f, ok := out.(fdWriter); ok {
		if w, _, err := term.GetSize(int(f.Fd())); err == nil && w > 0 {
			return w
		}
	}
	if w, _, err := term.GetSize(int(os.Stdout.Fd())); err == nil && w > 0 {
		return w
	}
	if cols := os.Getenv("COLUMNS"); cols != "" {
		if parsed, err := strconv.Atoi(cols); err == nil && parsed > 0 {
			return parsed
		}
	}
	return 0
}

func distributeWidths(total int, columns []columnKind) []int {
	if total <= 0 {
		total = 100
	}
	hasTTL := false
	for _, c := range columns {
		if c == columnTTL {
			hasTTL = true
			break
		}
	}
	base := make([]float64, len(columns))
	sum := 0.0
	for i, c := range columns {
		pct := basePercentageForColumn(c, hasTTL)
		base[i] = pct
		sum += pct
	}
	if sum == 0 {
		sum = 1
	}
	widths := make([]int, len(columns))
	remaining := total
	const minColWidth = 10
	for i := range columns {
		width := int((base[i] / sum) * float64(total))
		if width < minColWidth {
			width = minColWidth
		}
		widths[i] = width
		remaining -= width
	}
	for i := 0; remaining > 0 && len(columns) > 0; i++ {
		idx := i % len(columns)
		widths[idx]++
		remaining--
	}
	return widths
}

func basePercentageForColumn(c columnKind, hasTTL bool) float64 {
	switch c {
	case columnKey:
		return 0.25
	case columnValue:
		if hasTTL {
			return 0.5
		}
		return 0.75
	case columnTTL:
		return 0.25
	default:
		return 0.25
	}
}
