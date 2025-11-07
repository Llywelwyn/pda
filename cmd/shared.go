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
	"github.com/dgraph-io/badger/v4"
	gap "github.com/muesli/go-app-paths"
	"golang.org/x/term"
)

type errNotFound struct {
	suggestions []string
}

const (
	metaSecret byte = 0x1
)

func (err errNotFound) Error() string {
	if len(err.suggestions) == 0 {
		return "no suggestions found"
	}
	return fmt.Sprintf("did you mean %q", strings.Join(err.suggestions, ", "))
}

type Store struct{}

type TransactionArgs struct {
	key      string
	readonly bool
	sync     bool
	transact func(tx *badger.Txn, key []byte) error
}

func (s *Store) Transaction(args TransactionArgs) error {
	k, dbName, err := s.parse(args.key, true)
	if err != nil {
		return err
	}

	db, err := s.open(dbName)
	if err != nil {
		return err
	}
	defer db.Close()

	if args.sync {
		err = db.Sync()
		if err != nil {
			return err
		}
	}

	tx := db.NewTransaction(!args.readonly)
	defer tx.Discard()

	if err := args.transact(tx, k); err != nil {
		return err
	}

	if args.readonly {
		return nil
	}

	return tx.Commit()
}

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

func (s *Store) AllStores() ([]string, error) {
	path, err := s.path()
	if err != nil {
		return nil, err
	}
	dirs, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var stores []string
	for _, e := range dirs {
		if e.IsDir() {
			stores = append(stores, e.Name())
		}
	}
	return stores, nil
}

func (s *Store) FindStore(k string) (string, error) {
	n, err := s.parseDB(k, false)
	if err != nil {
		return "", err
	}
	path, err := s.path(n)
	if err != nil {
		return "", err
	}
	info, statErr := os.Stat(path)
	if strings.TrimSpace(n) == "" || os.IsNotExist(statErr) || (statErr == nil && !info.IsDir()) {
		suggestions, err := s.suggestStores(n)
		if err != nil {
			return "", err
		}
		return "", errNotFound{suggestions}
	}
	if statErr != nil {
		return "", statErr
	}
	return path, nil
}

func (s *Store) parse(k string, defaults bool) ([]byte, string, error) {
	var key, db string
	ps := strings.Split(k, "@")
	switch len(ps) {
	case 1:
		key = strings.ToLower(ps[0])
		if defaults {
			db = "default"
		}
	case 2:
		key = strings.ToLower(ps[0])
		db = strings.ToLower(ps[1])
	default:
		return nil, "", fmt.Errorf("bad key format, use KEY@DB")
	}
	return []byte(key), db, nil
}

func (s *Store) parseDB(v string, defaults bool) (string, error) {
	db := strings.TrimSpace(v)
	if strings.HasPrefix(db, "@") {
		db = strings.TrimPrefix(db, "@")
	}
	if db == "" {
		if defaults {
			return "default", nil
		}
		return "", fmt.Errorf("bad db format, use DB or @DB")
	}
	return strings.ToLower(db), nil
}

func (s *Store) open(name string) (*badger.DB, error) {
	if name == "" {
		name = "default"
	}
	path, err := s.path(name)
	if err != nil {
		return nil, err
	}
	return badger.Open(badger.DefaultOptions(path).WithLoggingLevel(badger.ERROR))
}

func (s *Store) path(args ...string) (string, error) {
	scope := gap.NewVendorScope(gap.User, "pda", "stores")
	dir, err := scope.DataPath("")
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	return filepath.Join(append([]string{dir}, args...)...), nil
}

func (s *Store) suggestStores(target string) ([]string, error) {
	stores, err := s.AllStores()
	if err != nil {
		return nil, err
	}
	target = strings.TrimSpace(target)
	minThreshold := 1
	maxThreshold := 4
	threshold := len(target) / 3
	if threshold < minThreshold {
		threshold = minThreshold
	}
	if threshold > maxThreshold {
		threshold = maxThreshold
	}
	var suggestions []string
	for _, store := range stores {
		distance := levenshtein.ComputeDistance(target, store)
		if distance <= threshold {
			suggestions = append(suggestions, store)
		}
	}
	return suggestions, nil
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
