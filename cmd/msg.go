package cmd

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/term"
)

// hinted wraps an error with an actionable hint shown on a separate line.
type hinted struct {
	err  error
	hint string
}

func (h hinted) Error() string { return h.err.Error() }
func (h hinted) Unwrap() error { return h.err }

func withHint(err error, hint string) error {
	return hinted{err: err, hint: hint}
}

func stderrIsTerminal() bool {
	return term.IsTerminal(int(os.Stderr.Fd()))
}

func stdoutIsTerminal() bool {
	return term.IsTerminal(int(os.Stdout.Fd()))
}

// keyword returns a right-aligned, colored keyword (color only on TTY).
// All keywords are bold except dim (code "2").
//
//	FAIL  bold red    (stderr)
//	hint  dim         (stderr)
//	WARN  bold yellow (stderr)
//	info  bold blue   (stderr)
//	  ok  bold green  (stderr)
//	   ?  bold cyan   (stdout)
//	   >  dim         (stdout)
func keyword(code, word string, tty bool) string {
	padded := fmt.Sprintf("%4s", word)
	if tty {
		if code != "2" {
			code = "1;" + code
		}
		return fmt.Sprintf("\033[%sm%s\033[0m", code, padded)
	}
	return padded
}

func printError(err error) {
	tty := stderrIsTerminal()
	if tty {
		fmt.Fprintf(os.Stderr, "%s \033[1m%s\033[0m\n", keyword("31", "FAIL", true), err)
	} else {
		fmt.Fprintf(os.Stderr, "%s %s\n", keyword("31", "FAIL", false), err)
	}
}

func printHint(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "%s %s\n", keyword("2", "hint", stderrIsTerminal()), msg)
}

func warnf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "%s %s\n", keyword("33", "WARN", stderrIsTerminal()), msg)
}

func infof(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "%s %s\n", keyword("34", "info", stderrIsTerminal()), msg)
}

func okf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "%s %s\n", keyword("32", "ok", stderrIsTerminal()), msg)
}

func promptf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stdout, "%s %s\n", keyword("36", "???", stdoutIsTerminal()), msg)
}

func progressf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stdout, "%s %s\n", keyword("2", ">", stdoutIsTerminal()), msg)
}

func scanln(dest *string) error {
	fmt.Fprintf(os.Stdout, "%s ", keyword("2", "==>", stdoutIsTerminal()))
	_, err := fmt.Scanln(dest)
	return err
}

// printErrorWithHints prints the error and any hints found in the error chain.
func printErrorWithHints(err error) {
	printError(err)
	var h hinted
	if errors.As(err, &h) {
		printHint("%s", h.hint)
	}
	var nf errNotFound
	if errors.As(err, &nf) && len(nf.suggestions) > 0 {
		printHint("did you mean '%s'?", strings.Join(nf.suggestions, "', '"))
	}
}
