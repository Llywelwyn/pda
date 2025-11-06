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
	if err := store.Transaction(args[0], true, func(tx *badger.Txn, k []byte) error {
		item, err := tx.Get(k)
		if err != nil {
			return err
		}
		v, err = item.ValueCopy(nil)
		return err
	}); err != nil {
		return err
	}
	store.Print("%s", v)
	return nil
}

func init() {
	rootCmd.AddCommand(getCmd)
}
