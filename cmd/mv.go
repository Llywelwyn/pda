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

var cpCmd = &cobra.Command{
	Use:   "cp FROM[@DB] TO[@DB]",
	Short: "Make a copy of a key.",
	Args:  cobra.ExactArgs(2),
	RunE:  cp,
}

var mvCmd = &cobra.Command{
	Use:          "mv FROM[@DB] TO[@DB]",
	Short:        "Move a key between (or within) databases.",
	Args:         cobra.ExactArgs(2),
	RunE:         mv,
	SilenceUsage: true,
}

func cp(cmd *cobra.Command, args []string) error {
	copy = true
	return mv(cmd, args)
}

func mv(cmd *cobra.Command, args []string) error {
	store := &Store{}

	fromSpec, err := store.parseKey(args[0], true)
	if err != nil {
		return err
	}
	toSpec, err := store.parseKey(args[1], true)
	if err != nil {
		return err
	}

	var srcVal []byte
	var srcMeta byte
	var srcExpires uint64
	fromRef := fromSpec.Full()
	toRef := toSpec.Full()

	readErr := store.Transaction(TransactionArgs{
		key:      fromRef,
		readonly: true,
		transact: func(tx *badger.Txn, k []byte) error {
			item, err := tx.Get(k)
			if err != nil {
				return fmt.Errorf("cannot move '%s': %v", fromSpec.Key, err)
			}
			srcMeta = item.UserMeta()
			srcExpires = item.ExpiresAt()
			return item.Value(func(v []byte) error {
				srcVal = append(srcVal[:0], v...)
				return nil
			})
		},
	})
	if readErr != nil {
		return readErr
	}

	writeErr := store.Transaction(TransactionArgs{
		key:      toRef,
		readonly: false,
		sync:     false,
		transact: func(tx *badger.Txn, k []byte) error {
			if !force && config.WarnOnOverwrite {
				if _, err := tx.Get(k); err == nil {
					return fmt.Errorf("cannot move '%s': '%s' already exists > run with --force to overwrite", fromSpec.Key, toSpec.Key)
				} else if err != badger.ErrKeyNotFound {
					return fmt.Errorf("cannot move '%s': %v", fromSpec.Key, err)
				}
			}
			entry := badger.NewEntry(k, srcVal).WithMeta(srcMeta)
			if srcExpires > 0 {
				entry.ExpiresAt = srcExpires
			}
			return tx.SetEntry(entry)
		},
	})
	if writeErr != nil {
		return writeErr
	}

	if copy {
		return nil
	}

	return store.Transaction(TransactionArgs{
		key:      fromRef,
		readonly: false,
		sync:     false,
		transact: func(tx *badger.Txn, k []byte) error {
			return tx.Delete(k)
		},
	})
}

var (
	copy  bool = false
	force bool = false
)

func init() {
	mvCmd.Flags().BoolVar(&copy, "copy", false, "Copy instead of move (keeps source)")
	mvCmd.Flags().BoolVarP(&force, "force", "f", false, "Overwrite destination if it exists")
	rootCmd.AddCommand(mvCmd)
	cpCmd.Flags().BoolVarP(&force, "force", "f", false, "Overwrite destination if it exists")
	rootCmd.AddCommand(cpCmd)
}
