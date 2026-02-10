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
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/spf13/cobra"
)

var cpCmd = &cobra.Command{
	Use:     "copy FROM[@STORE] TO[@STORE]",
	Aliases: []string{"cp"},
	Short:   "Make a copy of a key",
	Args:    cobra.ExactArgs(2),
	RunE:    cp,
}

var mvCmd = &cobra.Command{
	Use:          "move FROM[@STORE] TO[@STORE]",
	Aliases:      []string{"mv"},
	Short:        "Move a key",
	Args:         cobra.ExactArgs(2),
	RunE:         mv,
	SilenceUsage: true,
}

func cp(cmd *cobra.Command, args []string) error {
	copyMode = true
	return mv(cmd, args)
}

func mv(cmd *cobra.Command, args []string) error {
	store := &Store{}

	interactive, err := cmd.Flags().GetBool("interactive")
	if err != nil {
		return err
	}
	promptOverwrite := interactive || config.Key.AlwaysPromptOverwrite

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

	var destExists bool
	if promptOverwrite {
		existsErr := store.Transaction(TransactionArgs{
			key:      toRef,
			readonly: true,
			transact: func(tx *badger.Txn, k []byte) error {
				if _, err := tx.Get(k); err == nil {
					destExists = true
					return nil
				} else if err == badger.ErrKeyNotFound {
					return nil
				}
				return err
			},
		})
		if existsErr != nil {
			return fmt.Errorf("cannot move '%s': %v", fromSpec.Key, existsErr)
		}
	}

	if promptOverwrite && destExists {
		var confirm string
		fmt.Printf("overwrite '%s'? (y/n)\n", toSpec.Display())
		if _, err := fmt.Scanln(&confirm); err != nil {
			return fmt.Errorf("cannot move '%s': %v", fromSpec.Key, err)
		}
		if strings.ToLower(confirm) != "y" {
			return nil
		}
	}

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

	if copyMode {
		return autoSync()
	}

	if err := store.Transaction(TransactionArgs{
		key:      fromRef,
		readonly: false,
		sync:     false,
		transact: func(tx *badger.Txn, k []byte) error {
			return tx.Delete(k)
		},
	}); err != nil {
		return err
	}

	return autoSync()
}

var (
	copyMode bool = false
)

func init() {
	mvCmd.Flags().BoolVar(&copyMode, "copy", false, "Copy instead of move (keeps source)")
	mvCmd.Flags().BoolP("interactive", "i", false, "Prompt before overwriting destination")
	rootCmd.AddCommand(mvCmd)
	cpCmd.Flags().BoolP("interactive", "i", false, "Prompt before overwriting destination")
	rootCmd.AddCommand(cpCmd)
}
