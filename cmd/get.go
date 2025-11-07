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

	"github.com/dgraph-io/badger/v4"
	"github.com/spf13/cobra"
)

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get KEY[@DB]",
	Short: "Get a value for a key. Optionally specify a db.",
	Args:  cobra.ExactArgs(1),
	RunE:  get,
}

func get(cmd *cobra.Command, args []string) error {
	store := &Store{}

	var v []byte
	var meta byte
	trans := TransactionArgs{
		key:      args[0],
		readonly: true,
		sync:     false,
		transact: func(tx *badger.Txn, k []byte) error {
			item, err := tx.Get(k)
			if err != nil {
				return err
			}
			meta = item.UserMeta()
			v, err = item.ValueCopy(nil)
			return err
		},
	}

	if err := store.Transaction(trans); err != nil {
		return err
	}

	includeSecret, err := cmd.Flags().GetBool("include-secret")
	if err != nil {
		return err
	}
	if meta&metaSecret != 0 && !includeSecret {
		return fmt.Errorf("%q is marked secret; re-run with --secret to display it", args[0])
	}

	binary, err := cmd.Flags().GetBool("include-binary")
	if err != nil {
		return err
	}

	store.Print("%s", binary, v)
	return nil
}

func init() {
	getCmd.Flags().BoolP("include-binary", "b", false, "include binary data in text output")
	getCmd.Flags().Bool("include-secret", false, "display values marked as secret")
	rootCmd.AddCommand(getCmd)
}
