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
	Key       string `json:"key"`
	Value     string `json:"value"`
	Encoding  string `json:"encoding,omitempty"`
	Secret    bool   `json:"secret,omitempty"`
	ExpiresAt *int64 `json:"expires_at,omitempty"`
}

var dumpCmd = &cobra.Command{
	Use:          "dump [DB]",
	Short:        "Dump all key/value pairs as NDJSON",
	Aliases:      []string{"export"},
	Args:         cobra.MaximumNArgs(1),
	RunE:         dump,
	SilenceUsage: true,
}

func dump(cmd *cobra.Command, args []string) error {
	store := &Store{}
	targetDB := "@default"
	displayTarget := targetDB
	if len(args) == 1 {
		rawArg := args[0]
		dbName, err := store.parseDB(rawArg, false)
		if err != nil {
			return fmt.Errorf("cannot dump '%s': %v", rawArg, err)
		}
		if _, err := store.FindStore(dbName); err != nil {
			var notFound errNotFound
			if errors.As(err, &notFound) {
				return fmt.Errorf("cannot dump '%s': %v", rawArg, err)
			}
			return err
		}
		targetDB = "@" + dbName
		displayTarget = targetDB
	}

	mode, err := cmd.Flags().GetString("encoding")
	if err != nil {
		return fmt.Errorf("cannot dump '%s': %v", displayTarget, err)
	}
	switch mode {
	case "auto", "base64", "text":
	default:
		return fmt.Errorf("cannot dump '%s': unsupported encoding '%s'", displayTarget, mode)
	}

	includeSecret, err := cmd.Flags().GetBool("secret")
	if err != nil {
		return err
	}
	globPatterns, err := cmd.Flags().GetStringSlice("glob")
	if err != nil {
		return fmt.Errorf("cannot dump '%s': %v", displayTarget, err)
	}
	separators, err := parseGlobSeparators(cmd)
	if err != nil {
		return fmt.Errorf("cannot dump '%s': %v", displayTarget, err)
	}
	matchers, err := compileGlobMatchers(globPatterns, separators)
	if err != nil {
		return fmt.Errorf("cannot dump '%s': %v", displayTarget, err)
	}

	var matched bool
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
				if !globMatch(matchers, string(key)) {
					continue
				}
				meta := item.UserMeta()
				isSecret := meta&metaSecret != 0
				if isSecret && !includeSecret {
					continue
				}
				expiresAt := item.ExpiresAt()
				if err := item.Value(func(v []byte) error {
					entry := dumpEntry{
						Key:    string(key),
						Secret: isSecret,
					}
					if expiresAt > 0 {
						ts := int64(expiresAt)
						entry.ExpiresAt = &ts
					}
					switch mode {
					case "base64":
						encodeBase64(&entry, v)
					case "text":
						if err := encodeText(&entry, key, v); err != nil {
							return fmt.Errorf("cannot dump '%s': %v", displayTarget, err)
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
						return fmt.Errorf("cannot dump '%s': %v", displayTarget, err)
					}
					fmt.Fprintln(cmd.OutOrStdout(), string(payload))
					matched = true
					return nil
				}); err != nil {
					return fmt.Errorf("cannot dump '%s': %v", displayTarget, err)
				}
			}
			return nil
		},
	}

	if err := store.Transaction(trans); err != nil {
		return err
	}

	if len(matchers) > 0 && !matched {
		return fmt.Errorf("cannot dump '%s': No matches for pattern %s", displayTarget, formatGlobPatterns(globPatterns))
	}
	return nil
}

func init() {
	dumpCmd.Flags().StringP("encoding", "e", "auto", "value encoding: auto, base64, or text")
	dumpCmd.Flags().Bool("secret", false, "Include entries marked as secret")
	dumpCmd.Flags().StringSliceP("glob", "g", nil, "Filter keys with glob pattern (repeatable)")
	dumpCmd.Flags().String("glob-sep", "", fmt.Sprintf("Characters treated as separators for globbing (default %q)", defaultGlobSeparatorsDisplay()))
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
