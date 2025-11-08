package cmd

import (
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"golang.org/x/term"
)

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
	hasTTL := slices.Contains(columns, columnTTL)
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
		width := max(int((base[i]/sum)*float64(total)), minColWidth)
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
