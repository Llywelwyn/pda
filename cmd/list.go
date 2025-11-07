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
	"github.com/spf13/cobra"
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

	delimiter, err := cmd.Flags().GetString("delimiter")
	if err != nil {
		return err
	}
	if delimiter == "" {
		delimiter = "\t\t"
	}

	includeSecret, err := cmd.Flags().GetBool("include-secret")
	if err != nil {
		return err
	}

	keysOnly, err := cmd.Flags().GetBool("only-keys")
	if err != nil {
		return err
	}
	valuesOnly, err := cmd.Flags().GetBool("only-values")
	if err != nil {
		return err
	}
	if keysOnly && valuesOnly {
		return fmt.Errorf("--only-keys and --only-values are mutually exclusive")
	}

	prefetchVals := !keysOnly
	placeholder := []byte("[secret: pass --include-secret to view]")

	trans := TransactionArgs{
		key:      targetDB,
		readonly: true,
		sync:     true,
		transact: func(tx *badger.Txn, k []byte) error {
			binary, err := cmd.Flags().GetBool("include-binary")
			if err != nil {
				return err
			}
			format := fmt.Sprintf("%%s%s%%s\n", delimiter)
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 10
			opts.PrefetchValues = prefetchVals
			it := tx.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				key := item.Key()
				meta := item.UserMeta()
				if meta&metaSecret != 0 && !includeSecret {
					switch {
					case keysOnly:
						store.Print("%s\n", false, key)
					case valuesOnly:
						store.Print("%s\n", false, placeholder)
					default:
						store.Print(format, false, key, placeholder)
					}
					continue
				}
				var preparedValue []byte
				if !keysOnly {
					if err := item.Value(func(v []byte) error {
						preparedValue = append([]byte(nil), v...)
						return nil
					}); err != nil {
						return err
					}
				}
				switch {
				case keysOnly:
					store.Print("%s\n", false, key)
				case valuesOnly:
					store.Print("%s\n", binary, preparedValue)
				default:
					store.Print(format, binary, key, preparedValue)
				}
			}
			return nil
		},
	}

	return store.Transaction(trans)
}

func init() {
	listCmd.Flags().BoolP("include-binary", "b", false, "include binary data in text output")
	listCmd.Flags().StringP("delimiter", "d", "\t\t", "string written between key and value columns")
	listCmd.Flags().Bool("include-secret", false, "include entries marked as secret")
	listCmd.Flags().BoolP("only-keys", "k", false, "only print keys")
	listCmd.Flags().BoolP("only-values", "v", false, "only print values")
	rootCmd.AddCommand(listCmd)
}
