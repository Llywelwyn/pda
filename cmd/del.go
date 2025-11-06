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
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/spf13/cobra"
)

// delCmd represents the set command
var delCmd = &cobra.Command{
	Use:   "del KEY[@DB]",
	Short: "Delete a key. Optionally specify a db.",
	Args:  cobra.ExactArgs(1),
	RunE:  del,
}

func del(cmd *cobra.Command, args []string) error {
	store := &Store{}

	force, err := cmd.Flags().GetBool("force")
	if err != nil {
		return err
	}

	targetKey, err := formatKeyForPrompt(store, args[0])
	if err != nil {
		return err
	}

	if !force {
		var confirm string
		message := fmt.Sprintf("Are you sure you want to delete %q? (y/n)", targetKey)
		fmt.Println(message)
		if _, err := fmt.Scanln(&confirm); err != nil {
			return err
		}
		if strings.ToLower(confirm) != "y" {
			fmt.Fprintf(os.Stderr, "Did not delete %q\n", targetKey)
			return nil
		}
	}

	trans := TransactionArgs{
		key:      args[0],
		readonly: false,
		sync:     false,
		transact: func(tx *badger.Txn, k []byte) error {
			return tx.Delete(k)
		},
	}

	return store.Transaction(trans)
}

func init() {
	delCmd.Flags().BoolP("force", "f", false, "Force delete without confirmation")
	rootCmd.AddCommand(delCmd)
}

func formatKeyForPrompt(store *Store, arg string) (string, error) {
	_, db, err := store.parse(arg, true)
	if err != nil {
		return "", err
	}
	if strings.Contains(arg, "@") {
		return arg, nil
	}
	if db == "" {
		return arg, nil
	}
	return fmt.Sprintf("%s@%s", arg, db), nil
}
