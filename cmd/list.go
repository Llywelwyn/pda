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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// formatEnum implements pflag.Value for format selection.
type formatEnum string

func (e *formatEnum) String() string { return string(*e) }

func (e *formatEnum) Set(v string) error {
	switch v {
	case "table", "tsv", "csv", "html", "markdown", "ndjson":
		*e = formatEnum(v)
		return nil
	default:
		return fmt.Errorf("must be one of 'table', 'tsv', 'csv', 'html', 'markdown', or 'ndjson'")
	}
}

func (e *formatEnum) Type() string { return "format" }

var (
	listBinary   bool
	listNoKeys   bool
	listNoValues bool
	listTTL      bool
	listHeader bool
	listFormat formatEnum = "table"
)

type columnKind int

const (
	columnKey columnKind = iota
	columnValue
	columnTTL
)

var listCmd = &cobra.Command{
	Use:          "list [STORE]",
	Short:        "List the contents of a store",
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
				return fmt.Errorf("cannot ls '%s': %w", args[0], err)
			}
			return fmt.Errorf("cannot ls '%s': %v", args[0], err)
		}
		targetDB = "@" + dbName
	}

	if listNoKeys && listNoValues && !listTTL {
		return withHint(fmt.Errorf("cannot ls '%s': no columns selected", targetDB), "disable --no-keys/--no-values or pass --ttl")
	}

	var columns []columnKind
	if !listNoKeys {
		columns = append(columns, columnKey)
	}
	if !listNoValues {
		columns = append(columns, columnValue)
	}
	if listTTL {
		columns = append(columns, columnTTL)
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

	dbName := targetDB[1:] // strip leading '@'
	p, err := store.storePath(dbName)
	if err != nil {
		return fmt.Errorf("cannot ls '%s': %v", targetDB, err)
	}
	entries, err := readStoreFile(p)
	if err != nil {
		return fmt.Errorf("cannot ls '%s': %v", targetDB, err)
	}

	// Filter by glob
	var filtered []Entry
	for _, e := range entries {
		if globMatch(matchers, e.Key) {
			filtered = append(filtered, e)
		}
	}

	if len(matchers) > 0 && len(filtered) == 0 {
		return fmt.Errorf("cannot ls '%s': no matches for pattern %s", targetDB, formatGlobPatterns(globPatterns))
	}

	output := cmd.OutOrStdout()

	// NDJSON format: emit JSON lines directly
	if listFormat.String() == "ndjson" {
		for _, e := range filtered {
			data, err := json.Marshal(encodeJsonEntry(e))
			if err != nil {
				return fmt.Errorf("cannot ls '%s': %v", targetDB, err)
			}
			fmt.Fprintln(output, string(data))
		}
		return nil
	}

	// Table-based formats
	showValues := !listNoValues
	tw := table.NewWriter()
	tw.SetOutputMirror(output)
	tw.SetStyle(table.StyleDefault)
	tw.Style().Options.SeparateHeader = false
	tw.Style().Options.SeparateFooter = false
	tw.Style().Options.DrawBorder = false
	tw.Style().Options.SeparateRows = false
	tw.Style().Options.SeparateColumns = false

	if listHeader {
		tw.AppendHeader(headerRow(columns))
	}

	for _, e := range filtered {
		var valueStr string
		if showValues {
			valueStr = store.FormatBytes(listBinary, e.Value)
		}
		row := make(table.Row, 0, len(columns))
		for _, col := range columns {
			switch col {
			case columnKey:
				row = append(row, e.Key)
			case columnValue:
				row = append(row, valueStr)
			case columnTTL:
				row = append(row, formatExpiry(e.ExpiresAt))
			}
		}
		tw.AppendRow(row)
	}

	applyColumnWidths(tw, columns, output)
	renderTable(tw)
	return nil
}

func headerRow(columns []columnKind) table.Row {
	row := make(table.Row, 0, len(columns))
	for _, col := range columns {
		switch col {
		case columnKey:
			row = append(row, "Key")
		case columnValue:
			row = append(row, "Value")
		case columnTTL:
			row = append(row, "TTL")
		}
	}
	return row
}

func applyColumnWidths(tw table.Writer, columns []columnKind, out io.Writer) {
	termWidth := detectTerminalWidth(out)
	if termWidth <= 0 {
		return
	}
	tw.SetAllowedRowLength(termWidth)

	// Padding per column: go-pretty's default is one space each side.
	padding := len(columns) * 2
	available := termWidth - padding
	if available < len(columns) {
		return
	}

	// Give key and TTL columns a fixed budget; value gets the rest.
	const keyWidth = 30
	const ttlWidth = 40
	valueWidth := available
	for _, col := range columns {
		switch col {
		case columnKey:
			valueWidth -= keyWidth
		case columnTTL:
			valueWidth -= ttlWidth
		}
	}
	if valueWidth < 10 {
		valueWidth = 10
	}

	var configs []table.ColumnConfig
	for i, col := range columns {
		var maxW int
		switch col {
		case columnKey:
			maxW = keyWidth
		case columnValue:
			maxW = valueWidth
		case columnTTL:
			maxW = ttlWidth
		}
		configs = append(configs, table.ColumnConfig{
			Number:           i + 1,
			WidthMax:         maxW,
			WidthMaxEnforcer: text.WrapText,
		})
	}
	tw.SetColumnConfigs(configs)
}

func detectTerminalWidth(out io.Writer) int {
	type fd interface{ Fd() uintptr }
	if f, ok := out.(fd); ok {
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

func renderTable(tw table.Writer) {
	switch listFormat.String() {
	case "tsv":
		tw.RenderTSV()
	case "csv":
		tw.RenderCSV()
	case "html":
		tw.RenderHTML()
	case "markdown":
		tw.RenderMarkdown()
	default:
		tw.Render()
	}
}

func init() {
	listCmd.Flags().BoolVarP(&listBinary, "binary", "b", false, "include binary data in text output")
	listCmd.Flags().BoolVar(&listNoKeys, "no-keys", false, "suppress the key column")
	listCmd.Flags().BoolVar(&listNoValues, "no-values", false, "suppress the value column")
	listCmd.Flags().BoolVarP(&listTTL, "ttl", "t", false, "append a TTL column when entries expire")
	listCmd.Flags().BoolVar(&listHeader, "header", false, "include header row")
	listCmd.Flags().VarP(&listFormat, "format", "o", "output format (table|tsv|csv|markdown|html|ndjson)")
	listCmd.Flags().StringSliceP("glob", "g", nil, "Filter keys with glob pattern (repeatable)")
	listCmd.Flags().String("glob-sep", "", fmt.Sprintf("Characters treated as separators for globbing (default %q)", defaultGlobSeparatorsDisplay()))
	rootCmd.AddCommand(listCmd)
}
