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

var listCmd = &cobra.Command{
	Use:   "list [DB]",
	Short: "List the contents of a db.",
	Args:  cobra.MaximumNArgs(1),
	RunE:  list,
}

type ListArgs struct {
	header  bool
	key     bool
	value   bool
	ttl     bool
	binary  bool
	secrets bool
	format  *listFormat
}

type listFormat struct {
	limitColumns bool
	style        *table.Style
	render       func(table.Writer)
}

var (
	defaultTableStyle = table.StyleDefault
	plainTableStyle   = table.StyleDefault

	tableStylePresets = map[string]*table.Style{
		"table":        &defaultTableStyle,
		"tabular":      &defaultTableStyle,
		"table-dark":   &table.StyleColoredDark,
		"table-bright": &table.StyleColoredBright,
	}

	supportedListFormats = buildSupportedListFormats()
)

func buildSupportedListFormats() map[string]*listFormat {
	markdownSpec := &listFormat{
		style: &plainTableStyle,
		render: func(tw table.Writer) {
			tw.RenderMarkdown()
		},
	}
	formats := map[string]*listFormat{
		"csv": {
			style: &plainTableStyle,
			render: func(tw table.Writer) {
				tw.RenderCSV()
			},
		},
		"html": {
			style: &plainTableStyle,
			render: func(tw table.Writer) {
				tw.RenderHTML()
			},
		},
		"markdown": markdownSpec,
		"md":       markdownSpec,
	}
	for name, style := range tableStylePresets {
		formats[name] = &listFormat{
			limitColumns: true,
			style:        style,
			render: func(tw table.Writer) {
				tw.Render()
			},
		}
	}
	if defaultSpec, ok := formats["table"]; ok {
		formats["auto"] = defaultSpec
	}
	return formats
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

	flags, err := parseFlags(cmd)
	if err != nil {
		return err
	}

	columnKinds, err := requireColumns(flags)
	if err != nil {
		return err
	}

	output := cmd.OutOrStdout()
	tw := table.NewWriter()
	tw.SetOutputMirror(output)

	formatSpec := flags.format
	if formatSpec != nil && formatSpec.style != nil {
		tw.SetStyle(*formatSpec.style)
	} else {
		tw.SetStyle(defaultTableStyle)
	}
	limitColumns := formatSpec != nil && formatSpec.limitColumns
	var maxContentWidths []int
	if limitColumns {
		maxContentWidths = make([]int, len(columnKinds))
	}

	if flags.header {
		header := buildHeaderCells(columnKinds)
		if limitColumns {
			updateMaxContentWidths(maxContentWidths, header)
		}
		tw.AppendHeader(stringSliceToRow(header))
	}

	placeholder := "**********"
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
				meta := item.UserMeta()
				isSecret := meta&metaSecret != 0

				var valueStr string
				if flags.value && (!isSecret || flags.secrets) {
					if err := item.Value(func(v []byte) error {
						valueBuf = append(valueBuf[:0], v...)
						return nil
					}); err != nil {
						return err
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
				if limitColumns {
					updateMaxContentWidths(maxContentWidths, columns)
				}
				tw.AppendRow(stringSliceToRow(columns))
			}
			return nil
		},
	}

	if err := store.Transaction(trans); err != nil {
		return err
	}

	if limitColumns {
		applyColumnConstraints(tw, columnKinds, output, maxContentWidths)
	}

	if formatSpec != nil && formatSpec.render != nil {
		formatSpec.render(tw)
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
	listCmd.Flags().StringP("format", "f", "table", "supports: table[-dark/-bright], csv, html, markdown")
	rootCmd.AddCommand(listCmd)
}

func parseFlags(cmd *cobra.Command) (ListArgs, error) {
	secrets, err := cmd.Flags().GetBool("secret")
	if err != nil {
		return ListArgs{}, err
	}
	noKeys, err := cmd.Flags().GetBool("no-keys")
	if err != nil {
		return ListArgs{}, err
	}
	noValues, err := cmd.Flags().GetBool("no-values")
	if err != nil {
		return ListArgs{}, err
	}
	ttl, err := cmd.Flags().GetBool("ttl")
	if err != nil {
		return ListArgs{}, err
	}
	noHeader, err := cmd.Flags().GetBool("no-header")
	if err != nil {
		return ListArgs{}, err
	}
	binary, err := cmd.Flags().GetBool("binary")
	if err != nil {
		return ListArgs{}, err
	}
	formatName, err := cmd.Flags().GetString("format")
	if err != nil {
		return ListArgs{}, err
	}
	formatSpec, err := resolveListFormat(formatName)
	if err != nil {
		return ListArgs{}, err
	}

	if noKeys && noValues && !ttl {
		return ListArgs{}, fmt.Errorf("no columns selected; disable --no-keys/--no-values or pass --ttl")
	}

	return ListArgs{
		header:  !noHeader,
		key:     !noKeys,
		value:   !noValues,
		ttl:     ttl,
		binary:  binary,
		format:  formatSpec,
		secrets: secrets,
	}, nil
}

func resolveListFormat(name string) (*listFormat, error) {
	if spec, ok := supportedListFormats[name]; ok {
		return spec, nil
	}
	return nil, fmt.Errorf("unsupported format %q", name)
}

type columnKind int

const (
	columnKey columnKind = iota
	columnValue
	columnTTL
)

func requireColumns(args ListArgs) ([]columnKind, error) {
	var columns []columnKind
	if args.key {
		columns = append(columns, columnKey)
	}
	if args.value {
		columns = append(columns, columnValue)
	}
	if args.ttl {
		columns = append(columns, columnTTL)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns selected; enable key, value, or ttl output")
	}
	return columns, nil
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

func updateMaxContentWidths(maxWidths []int, values []string) {
	if len(maxWidths) == 0 {
		return
	}
	limit := min(len(values), len(maxWidths))
	for i := range limit {
		width := text.LongestLineLen(values[i])
		if width > maxWidths[i] {
			maxWidths[i] = width
		}
	}
}

func applyColumnConstraints(tw table.Writer, columns []columnKind, out io.Writer, maxContentWidths []int) {
	totalWidth := detectTerminalWidth(out)
	if totalWidth <= 0 {
		totalWidth = 100
	}
	contentWidth := contentWidthForStyle(totalWidth, tw, len(columns))
	widths := distributeWidths(contentWidth, columns)

	used := 0
	for idx, width := range widths {
		if width <= 0 {
			width = 1
		}
		if idx < len(maxContentWidths) {
			if actual := maxContentWidths[idx]; actual > 0 && width > actual {
				width = actual
			}
		}
		widths[idx] = width
		used += width
	}

	remaining := contentWidth - used
	for remaining > 0 {
		progressed := false
		for idx := range widths {
			actual := 0
			if idx < len(maxContentWidths) {
				actual = maxContentWidths[idx]
			}
			if actual > 0 && widths[idx] >= actual {
				continue
			}
			widths[idx]++
			remaining--
			progressed = true
			if remaining == 0 {
				break
			}
		}
		if !progressed {
			break
		}
	}

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

func contentWidthForStyle(totalWidth int, tw table.Writer, columnCount int) int {
	if columnCount == 0 {
		return totalWidth
	}
	style := tw.Style()
	if style != nil {
		totalWidth -= tableRowOverhead(style, columnCount)
	}
	if totalWidth < columnCount {
		totalWidth = columnCount
	}
	return totalWidth
}

func tableRowOverhead(style *table.Style, columnCount int) int {
	if style == nil || columnCount == 0 {
		return 0
	}
	paddingWidth := text.StringWidthWithoutEscSequences(style.Box.PaddingLeft + style.Box.PaddingRight)
	overhead := paddingWidth * columnCount
	if style.Options.SeparateColumns && columnCount > 1 {
		overhead += (columnCount - 1) * maxSeparatorWidth(style)
	}
	if style.Options.DrawBorder {
		overhead += text.StringWidthWithoutEscSequences(style.Box.Left + style.Box.Right)
	}
	return overhead
}

func maxSeparatorWidth(style *table.Style) int {
	widest := 0
	separators := []string{
		style.Box.MiddleSeparator,
		style.Box.EmptySeparator,
		style.Box.MiddleHorizontal,
		style.Box.TopSeparator,
		style.Box.BottomSeparator,
		style.Box.MiddleVertical,
		style.Box.LeftSeparator,
		style.Box.RightSeparator,
	}
	for _, sep := range separators {
		if width := text.StringWidthWithoutEscSequences(sep); width > widest {
			widest = width
		}
	}
	return widest
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
