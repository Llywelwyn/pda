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
	"io"

	"github.com/dgraph-io/badger/v4"
	"github.com/spf13/cobra"
)

// setCmd represents the set command
var setCmd = &cobra.Command{
	Use:   "set KEY[@DB] [VALUE]",
	Short: "Set a value for a key by passing VALUE or from Stdin. Optionally specify a db.",
	Args:  cobra.RangeArgs(1, 2),
	RunE:  set,
}

func set(cmd *cobra.Command, args []string) error {
	store := &Store{}

	var value []byte
	if len(args) == 2 {
		value = []byte(args[1])
	} else {
		bytes, err := io.ReadAll(cmd.InOrStdin())
		if err != nil {
			return err
		}
		value = bytes
	}

	secret, err := cmd.Flags().GetBool("secret")
	if err != nil {
		return err
	}

	trans := TransactionArgs{
		key:      args[0],
		readonly: false,
		sync:     false,
		transact: func(tx *badger.Txn, k []byte) error {
			entry := badger.NewEntry(k, value)
			if secret {
				entry = entry.WithMeta(metaSecret)
			}
			return tx.SetEntry(entry)
		},
	}

	return store.Transaction(trans)
}

func init() {
	rootCmd.AddCommand(setCmd)
	setCmd.Flags().Bool("secret", false, "Mark the stored value as a secret")
}
