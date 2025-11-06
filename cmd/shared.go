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
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/dgraph-io/badger/v4"
	gap "github.com/muesli/go-app-paths"
	"golang.org/x/term"
)

type Store struct{}

func (s *Store) parse(k string) ([]byte, string, error) {
	var key, db string
	ps := strings.Split(k, "@")
	switch len(ps) {
	case 1:
		key = strings.ToLower(ps[0])
	case 2:
		key = strings.ToLower(ps[0])
		db = strings.ToLower(ps[1])
	default:
		return nil, "", fmt.Errorf("bad key format, use KEY@DB")
	}
	return []byte(key), db, nil
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

func (s *Store) Print(pf string, vs ...[]byte) {
	nb := "(omitted binary data)"
	fvs := make([]any, 0)
	tty := term.IsTerminal(int(os.Stdin.Fd()))
	for _, v := range vs {
		if tty && !utf8.Valid(v) {
			fvs = append(fvs, nb)
		} else {
			fvs = append(fvs, string(v))
		}
	}
	fmt.Printf(pf, fvs...)
	if tty && !strings.HasSuffix(pf, "\n") {
		fmt.Println()
	}
}

type TransactionArgs struct {
	key      string
	readonly bool
	sync     bool
	transact func(tx *badger.Txn, key []byte) error
}

func (s *Store) Transaction(args TransactionArgs) error {
	k, dbName, err := s.parse(args.key)
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
	if err := args.transact(tx, k); err != nil {
		tx.Discard()
		return err
	}
	return tx.Commit()
}
