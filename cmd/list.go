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
	"strings"
	"unicode/utf8"

	"filippo.io/age"
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
	listBase64   bool
	listNoKeys   bool
	listNoValues bool
	listNoTTL    bool
	listFull     bool
	listNoHeader bool
	listFormat   formatEnum = "table"

	dimStyle = text.Colors{text.Faint, text.Italic}
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

	if listNoKeys && listNoValues && listNoTTL {
		return withHint(fmt.Errorf("cannot ls '%s': no columns selected", targetDB), "disable --no-keys, --no-values, or --no-ttl")
	}

	var columns []columnKind
	if !listNoKeys {
		columns = append(columns, columnKey)
	}
	if !listNoValues {
		columns = append(columns, columnValue)
	}
	if !listNoTTL {
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

	identity, _ := loadIdentity()
	var recipient *age.X25519Recipient
	if identity != nil {
		recipient = identity.Recipient()
	}

	dbName := targetDB[1:] // strip leading '@'
	p, err := store.storePath(dbName)
	if err != nil {
		return fmt.Errorf("cannot ls '%s': %v", targetDB, err)
	}
	entries, err := readStoreFile(p, identity)
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

	// NDJSON format: emit JSON lines directly (encrypted form for secrets)
	if listFormat.String() == "ndjson" {
		for _, e := range filtered {
			je, err := encodeJsonEntry(e, recipient)
			if err != nil {
				return fmt.Errorf("cannot ls '%s': %v", targetDB, err)
			}
			data, err := json.Marshal(je)
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
	tw.Style().Box.PaddingLeft = ""
	tw.Style().Box.PaddingRight = " "

	tty := stdoutIsTerminal() && listFormat.String() == "table"

	if !listNoHeader {
		tw.AppendHeader(headerRow(columns, tty))
		tw.Style().Format.Header = text.FormatDefault
	}
	lay := computeLayout(columns, output, filtered)

	for _, e := range filtered {
		var valueStr string
		dimValue := false
		if showValues {
			if e.Locked {
				valueStr = "locked (identity file missing)"
				dimValue = true
			} else {
				valueStr = store.FormatBytes(listBase64, e.Value)
				if !utf8.Valid(e.Value) && !listBase64 {
					dimValue = true
				}
			}
			if !listFull {
				valueStr = summariseValue(valueStr, lay.value, tty)
			}
		}
		row := make(table.Row, 0, len(columns))
		for _, col := range columns {
			switch col {
			case columnKey:
				if tty {
					row = append(row, text.Bold.Sprint(e.Key))
				} else {
					row = append(row, e.Key)
				}
			case columnValue:
				if tty && dimValue {
					row = append(row, dimStyle.Sprint(valueStr))
				} else {
					row = append(row, valueStr)
				}
			case columnTTL:
				ttlStr := formatExpiry(e.ExpiresAt)
			if tty && e.ExpiresAt == 0 {
				ttlStr = dimStyle.Sprint(ttlStr)
			}
			row = append(row, ttlStr)
			}
		}
		tw.AppendRow(row)
	}

	applyColumnWidths(tw, columns, output, lay, listFull)
	renderTable(tw)
	return nil
}

// summariseValue flattens a value to its first line and, when maxWidth > 0,
// truncates to fit. In both cases it appends "(..N more chars)" showing the
// total number of omitted characters.
func summariseValue(s string, maxWidth int, tty bool) string {
	first := s
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		first = s[:i]
	}

	totalRunes := utf8.RuneCountInString(s)
	firstRunes := utf8.RuneCountInString(first)

	// Nothing omitted and fits (or no width constraint).
	if firstRunes == totalRunes && (maxWidth <= 0 || firstRunes <= maxWidth) {
		return first
	}

	// How many runes of first can we show?
	showRunes := firstRunes
	if maxWidth > 0 && showRunes > maxWidth {
		showRunes = maxWidth
	}

	style := func(s string) string {
		if tty {
			return dimStyle.Sprint(s)
		}
		return s
	}

	// Iteratively make room for the suffix (at most two passes since
	// the digit count can change by one at a boundary like 9→10).
	for range 2 {
		omitted := totalRunes - showRunes
		if omitted <= 0 {
			return first
		}
		suffix := fmt.Sprintf(" (..%d more chars)", omitted)
		suffixRunes := utf8.RuneCountInString(suffix)
		if maxWidth <= 0 {
			return first + style(suffix)
		}
		if showRunes+suffixRunes <= maxWidth {
			runes := []rune(first)
			if showRunes < len(runes) {
				first = string(runes[:showRunes])
			}
			return first + style(suffix)
		}
		avail := maxWidth - suffixRunes
		if avail <= 0 {
			// Suffix alone exceeds maxWidth; fall through to hard trim.
			break
		}
		showRunes = avail
	}

	// Column too narrow for the suffix — just truncate with an ellipsis.
	if maxWidth >= 2 {
		return text.Trim(first, maxWidth-1) + style("…")
	}
	return text.Trim(first, maxWidth)
}

func headerRow(columns []columnKind, tty bool) table.Row {
	h := func(s string) interface{} {
		if tty {
			return text.Underline.Sprint(s)
		}
		return s
	}
	row := make(table.Row, 0, len(columns))
	for _, col := range columns {
		switch col {
		case columnKey:
			row = append(row, h("Key"))
		case columnValue:
			row = append(row, h("Value"))
		case columnTTL:
			row = append(row, h("TTL"))
		}
	}
	return row
}

const (
	keyColumnWidthCap = 30
	ttlColumnWidthCap = 20
)

// columnLayout holds the resolved max widths for each column kind.
type columnLayout struct {
	key, value, ttl int
}

// computeLayout derives column widths from the terminal size and actual
// content widths of the key/TTL columns (capped at fixed maximums). This
// avoids reserving 30+40 chars for key+TTL when the real content is narrower.
func computeLayout(columns []columnKind, out io.Writer, entries []Entry) columnLayout {
	var lay columnLayout
	termWidth := detectTerminalWidth(out)

	// Scan entries for actual max key/TTL content widths.
	for _, e := range entries {
		if w := utf8.RuneCountInString(e.Key); w > lay.key {
			lay.key = w
		}
		if w := utf8.RuneCountInString(formatExpiry(e.ExpiresAt)); w > lay.ttl {
			lay.ttl = w
		}
	}
	if lay.key > keyColumnWidthCap {
		lay.key = keyColumnWidthCap
	}
	if lay.ttl > ttlColumnWidthCap {
		lay.ttl = ttlColumnWidthCap
	}

	if termWidth <= 0 {
		return lay
	}

	padding := len(columns) * 2
	available := termWidth - padding
	if available < len(columns) {
		return lay
	}

	// Give the value column whatever is left after key and TTL.
	lay.value = available
	for _, col := range columns {
		switch col {
		case columnKey:
			lay.value -= lay.key
		case columnTTL:
			lay.value -= lay.ttl
		}
	}
	if lay.value < 10 {
		lay.value = 10
	}
	return lay
}

func applyColumnWidths(tw table.Writer, columns []columnKind, out io.Writer, lay columnLayout, full bool) {
	termWidth := detectTerminalWidth(out)
	if termWidth <= 0 {
		return
	}
	tw.SetAllowedRowLength(termWidth)

	var configs []table.ColumnConfig
	for i, col := range columns {
		var maxW int
		var enforcer func(string, int) string
		switch col {
		case columnKey:
			maxW = lay.key
			enforcer = text.Trim
		case columnValue:
			maxW = lay.value
			if full {
				enforcer = text.WrapText
			}
			// When !full, values are already pre-truncated by
			// summariseValue — no enforcer needed.
		case columnTTL:
			maxW = lay.ttl
			enforcer = text.Trim
		}
		configs = append(configs, table.ColumnConfig{
			Number:           i + 1,
			WidthMax:         maxW,
			WidthMaxEnforcer: enforcer,
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
	listCmd.Flags().BoolVarP(&listBase64, "base64", "b", false, "view binary data as base64")
	listCmd.Flags().BoolVar(&listNoKeys, "no-keys", false, "suppress the key column")
	listCmd.Flags().BoolVar(&listNoValues, "no-values", false, "suppress the value column")
	listCmd.Flags().BoolVar(&listNoTTL, "no-ttl", false, "suppress the TTL column")
	listCmd.Flags().BoolVarP(&listFull, "full", "f", false, "show full values without truncation")
	listCmd.Flags().BoolVar(&listNoHeader, "no-header", false, "suppress the header row")
	listCmd.Flags().VarP(&listFormat, "format", "o", "output format (table|tsv|csv|markdown|html|ndjson)")
	listCmd.Flags().StringSliceP("glob", "g", nil, "Filter keys with glob pattern (repeatable)")
	listCmd.Flags().String("glob-sep", "", fmt.Sprintf("Characters treated as separators for globbing (default '%s')", defaultGlobSeparatorsDisplay()))
	rootCmd.AddCommand(listCmd)
}
