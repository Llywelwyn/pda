package cmd

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/dgraph-io/badger/v4"
	"github.com/spf13/cobra"
)

type dumpEntry struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Encoding string `json:"encoding,omitempty"`
	Secret   bool   `json:"secret,omitempty"`
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

	mode, err := cmd.Flags().GetString("encoding")
	if err != nil {
		return err
	}
	switch mode {
	case "auto", "base64", "text":
	default:
		return fmt.Errorf("unsupported encoding %q", mode)
	}

	includeSecret, err := cmd.Flags().GetBool("secret")
	if err != nil {
		return err
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
				meta := item.UserMeta()
				isSecret := meta&metaSecret != 0
				if isSecret && !includeSecret {
					continue
				}
				if err := item.Value(func(v []byte) error {
					entry := dumpEntry{
						Key:    string(key),
						Secret: isSecret,
					}
					switch mode {
					case "base64":
						encodeBase64(&entry, v)
					case "text":
						if err := encodeText(&entry, key, v); err != nil {
							return err
						}
					case "auto":
						if utf8.Valid(v) {
							entry.Encoding = "text"
							entry.Value = string(v)
						} else {
							encodeBase64(&entry, v)
						}
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
	dumpCmd.Flags().StringP("encoding", "e", "auto", "value encoding: auto, base64, or text")
	dumpCmd.Flags().Bool("secret", false, "Include entries marked as secret")
	rootCmd.AddCommand(dumpCmd)
}

func encodeBase64(entry *dumpEntry, v []byte) {
	entry.Value = base64.StdEncoding.EncodeToString(v)
	entry.Encoding = "base64"
}

func encodeText(entry *dumpEntry, key []byte, v []byte) error {
	if !utf8.Valid(v) {
		return fmt.Errorf("key %q contains non-UTF8 data; use --encoding=auto or base64", key)
	}
	entry.Value = string(v)
	entry.Encoding = "text"
	return nil
}
