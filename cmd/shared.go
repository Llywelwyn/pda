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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/agnivade/levenshtein"
	gap "github.com/muesli/go-app-paths"
	"golang.org/x/term"
)

type errNotFound struct {
	what        string // "key" or "store"
	suggestions []string
}

func (err errNotFound) Error() string {
	return fmt.Sprintf("no such %s", err.what)
}

type Store struct{}

func (s *Store) Print(pf string, includeBinary bool, vs ...[]byte) {
	s.PrintTo(os.Stdout, pf, includeBinary, vs...)
}

func (s *Store) PrintTo(w io.Writer, pf string, includeBinary bool, vs ...[]byte) {
	tty := term.IsTerminal(int(os.Stdout.Fd()))
	fvs := make([]any, 0, len(vs))
	for _, v := range vs {
		fvs = append(fvs, s.formatBytes(includeBinary, v))
	}
	fmt.Fprintf(w, pf, fvs...)
	if w == os.Stdout && tty && !strings.HasSuffix(pf, "\n") {
		fmt.Fprintln(os.Stdout)
	}
}

func (s *Store) FormatBytes(includeBinary bool, v []byte) string {
	return s.formatBytes(includeBinary, v)
}

func (s *Store) formatBytes(includeBinary bool, v []byte) string {
	tty := term.IsTerminal(int(os.Stdout.Fd()))
	if tty && !includeBinary && !utf8.Valid(v) {
		return "(omitted binary data)"
	}
	return string(v)
}

func (s *Store) storePath(name string) (string, error) {
	if name == "" {
		name = config.Store.DefaultStoreName
	}
	dir, err := s.path()
	if err != nil {
		return "", err
	}
	target := filepath.Join(dir, name+".ndjson")
	if err := ensureSubpath(dir, target); err != nil {
		return "", err
	}
	return target, nil
}

func (s *Store) AllStores() ([]string, error) {
	dir, err := s.path()
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var stores []string
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".ndjson" {
			continue
		}
		stores = append(stores, strings.TrimSuffix(e.Name(), ".ndjson"))
	}
	return stores, nil
}

func (s *Store) FindStore(k string) (string, error) {
	n, err := s.parseDB(k, false)
	if err != nil {
		return "", err
	}
	p, err := s.storePath(n)
	if err != nil {
		return "", err
	}
	_, statErr := os.Stat(p)
	if strings.TrimSpace(n) == "" || os.IsNotExist(statErr) {
		suggestions, err := s.suggestStores(n)
		if err != nil {
			return "", err
		}
		return "", errNotFound{what: "store", suggestions: suggestions}
	}
	if statErr != nil {
		return "", statErr
	}
	return p, nil
}

func (s *Store) parseKey(raw string, defaults bool) (KeySpec, error) {
	return ParseKey(raw, defaults)
}

func (s *Store) parseDB(v string, defaults bool) (string, error) {
	db := strings.TrimSpace(v)
	if after, ok := strings.CutPrefix(db, "@"); ok {
		db = after
	}
	if db == "" {
		if defaults {
			return config.Store.DefaultStoreName, nil
		}
		return "", fmt.Errorf("cannot parse store: bad store format, use STORE or @STORE")
	}
	if err := validateDBName(db); err != nil {
		return "", fmt.Errorf("cannot parse store: %w", err)
	}
	return strings.ToLower(db), nil
}

func (s *Store) path() (string, error) {
	if override := os.Getenv("PDA_DATA"); override != "" {
		if err := os.MkdirAll(override, 0o750); err != nil {
			return "", err
		}
		return override, nil
	}
	scope := gap.NewVendorScope(gap.User, "pda", "stores")
	dir, err := scope.DataPath("")
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	return dir, nil
}

func (s *Store) suggestStores(target string) ([]string, error) {
	stores, err := s.AllStores()
	if err != nil {
		return nil, err
	}
	target = strings.TrimSpace(target)
	minThreshold := 1
	maxThreshold := 4
	threshold := min(max(len(target)/3, minThreshold), maxThreshold)
	var suggestions []string
	for _, store := range stores {
		distance := levenshtein.ComputeDistance(target, store)
		if distance <= threshold {
			suggestions = append(suggestions, store)
		}
	}
	return suggestions, nil
}

func suggestKey(target string, keys []string) error {
	minThreshold := 1
	maxThreshold := 4
	threshold := min(max(len(target)/3, minThreshold), maxThreshold)
	var suggestions []string
	for _, k := range keys {
		if levenshtein.ComputeDistance(target, k) <= threshold {
			suggestions = append(suggestions, k)
		}
	}
	return errNotFound{what: "key", suggestions: suggestions}
}

func ensureSubpath(base, target string) error {
	absBase, err := filepath.Abs(base)
	if err != nil {
		return err
	}
	absTarget, err := filepath.Abs(target)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(absBase, absTarget)
	if err != nil {
		return err
	}
	sep := string(filepath.Separator)
	if rel == ".." || strings.HasPrefix(rel, ".."+sep) {
		return fmt.Errorf("path escapes store root")
	}
	return nil
}

func validateDBName(name string) error {
	if strings.ContainsAny(name, `/\~`) {
		return fmt.Errorf("bad store format, use STORE or @STORE")
	}
	return nil
}

func formatExpiry(expiresAt uint64) string {
	if expiresAt == 0 {
		return "never"
	}
	expiry := time.Unix(int64(expiresAt), 0).UTC()
	remaining := time.Until(expiry)
	if remaining <= 0 {
		return fmt.Sprintf("%s (expired)", expiry.Format(time.RFC3339))
	}
	return fmt.Sprintf("%s (in %s)", expiry.Format(time.RFC3339), remaining.Round(time.Second))
}

// Keys returns all keys for the provided store name (or default if empty).
// Keys are returned in lowercase to mirror stored key format.
func (s *Store) Keys(dbName string) ([]string, error) {
	p, err := s.storePath(dbName)
	if err != nil {
		return nil, err
	}
	entries, err := readStoreFile(p)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(entries))
	for i, e := range entries {
		keys[i] = e.Key
	}
	return keys, nil
}
