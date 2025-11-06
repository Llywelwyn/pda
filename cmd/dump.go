package cmd

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/spf13/cobra"
)

type dumpEntry struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Encoding string `json:"encoding,omitempty"`
}

var dumpCmd = &cobra.Command{
	Use:   "dump [DB]",
	Short: "Dump all key/value pairs as NDJSON",
	Args:  cobra.MaximumNArgs(1),
	RunE:  dump,
}

func dump(cmd *cobra.Command, args []string) error {
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

	encoding, err := cmd.Flags().GetString("encoding")
	if err != nil {
		return err
	}
	if encoding != "base64" && encoding != "text" {
		return fmt.Errorf("unsupported encoding %q", encoding)
	}

	trans := TransactionArgs{
		key:      targetDB,
		readonly: true,
		sync:     true,
		transact: func(tx *badger.Txn, k []byte) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 64
			it := tx.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				key := item.KeyCopy(nil)
				if err := item.Value(func(v []byte) error {
					entry := dumpEntry{Key: string(key)}
					switch encoding {
					case "base64":
						entry.Value = base64.StdEncoding.EncodeToString(v)
						entry.Encoding = "base64"
					case "text":
						entry.Value = string(v)
					}
					payload, err := json.Marshal(entry)
					if err != nil {
						return err
					}
					fmt.Fprintln(cmd.OutOrStdout(), string(payload))
					return nil
				}); err != nil {
					return err
				}
			}
			return nil
		},
	}

	return store.Transaction(trans)
}

func init() {
	dumpCmd.Flags().StringP("encoding", "e", "base64", "value encoding: base64 or text")
	rootCmd.AddCommand(dumpCmd)
}
