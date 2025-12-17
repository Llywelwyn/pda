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

	fromKey, fromDB, err := store.parse(args[0], true)
	if err != nil {
		return err
	}
	toKey, toDB, err := store.parse(args[1], true)
	if err != nil {
		return err
	}

	var srcVal []byte
	var srcMeta byte
	var srcExpires uint64
	readErr := store.Transaction(TransactionArgs{
		key:      fmt.Sprintf("%s@%s", fromKey, fromDB),
		readonly: true,
		transact: func(tx *badger.Txn, k []byte) error {
			item, err := tx.Get(k)
			if err != nil {
				return fmt.Errorf("cannot move '%s': %v", fromKey, err)
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
		key:      fmt.Sprintf("%s@%s", toKey, toDB),
		readonly: false,
		sync:     false,
		transact: func(tx *badger.Txn, k []byte) error {
			if !force {
				if _, err := tx.Get(k); err == nil {
					return fmt.Errorf("cannot move '%s': '%s' already exists > run with --force to overwrite", fromKey, toKey)
				} else if err != badger.ErrKeyNotFound {
					return err
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
		key:      fmt.Sprintf("%s@%s", fromKey, fromDB),
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
