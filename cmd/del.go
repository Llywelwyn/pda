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
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/gobwas/glob"
	"github.com/spf13/cobra"
)

// delCmd represents the set command
var delCmd = &cobra.Command{
	Use:          "del KEY[@DB] [KEY[@DB] ...]",
	Short:        "Delete one or more keys. Optionally specify a db.",
	Aliases:      []string{"delete", "rm", "remove"},
	Args:         cobra.ArbitraryArgs,
	RunE:         del,
	SilenceUsage: true,
}

func del(cmd *cobra.Command, args []string) error {
	store := &Store{}

	force, err := cmd.Flags().GetBool("force")
	if err != nil {
		return err
	}
	globPatterns, err := cmd.Flags().GetStringSlice("glob")
	if err != nil {
		return err
	}
	separators, err := parseGlobSeparators(cmd)
	if err != nil {
		return err
	}

	if len(args) == 0 && len(globPatterns) == 0 {
		return fmt.Errorf("cannot remove: no keys provided")
	}

	targetKeys, deleteTargets, err := resolveDeleteTargets(store, args, globPatterns, separators)
	if err != nil {
		return err
	}

	if len(targetKeys) == 0 {
		return fmt.Errorf("cannot remove: No such key")
	}

	if !force && config.WarnOnDelete {
		var confirm string
		quotedTargets := make([]string, 0, len(targetKeys))
		for _, t := range targetKeys {
			quotedTargets = append(quotedTargets, fmt.Sprintf("%q", t))
		}
		message := fmt.Sprintf("remove %s: are you sure? (y/n)", strings.Join(quotedTargets, ", "))
		fmt.Println(message)
		if _, err := fmt.Scanln(&confirm); err != nil {
			return fmt.Errorf("cannot remove '%s': %v", args[0], err)
		}
		if strings.ToLower(confirm) != "y" {
			return nil
		}
	}

	for _, target := range deleteTargets {
		trans := TransactionArgs{
			key:      target,
			readonly: false,
			sync:     false,
			transact: func(tx *badger.Txn, k []byte) error {
				if err := tx.Delete(k); errors.Is(err, badger.ErrKeyNotFound) {
					return fmt.Errorf("cannot remove '%s': No such key", target)
				}
				if err != nil {
					return fmt.Errorf("cannot remove '%s': %v", target, err)
				}
				return nil
			},
		}

		if err := store.Transaction(trans); err != nil {
			return err
		}
	}

	return nil
}

func init() {
	delCmd.Flags().BoolP("force", "f", false, "Force delete without confirmation")
	delCmd.Flags().StringSliceP("glob", "g", nil, "Delete keys matching glob pattern (repeatable)")
	delCmd.Flags().String("glob-sep", "", fmt.Sprintf("Characters treated as separators for globbing (default %q)", defaultGlobSeparatorsDisplay()))
	rootCmd.AddCommand(delCmd)
}

func keyExists(store *Store, arg string) (bool, error) {
	var notFound bool
	trans := TransactionArgs{
		key:      arg,
		readonly: true,
		sync:     false,
		transact: func(tx *badger.Txn, k []byte) error {
			if _, err := tx.Get(k); errors.Is(err, badger.ErrKeyNotFound) {
				notFound = true
				return nil
			} else {
				return err
			}
		},
	}
	if err := store.Transaction(trans); err != nil {
		return false, err
	}
	return !notFound, nil
}

func formatKeyForPrompt(store *Store, arg string) (string, error) {
	spec, err := store.parseKey(arg, true)
	if err != nil {
		return "", err
	}
	return spec.Display(), nil
}

func resolveDeleteTargets(store *Store, exactArgs []string, globPatterns []string, separators []rune) ([]string, []string, error) {
	targetSet := make(map[string]struct{})
	var targetKeys []string
	var deleteTargets []string

	for _, arg := range exactArgs {
		exists, err := keyExists(store, arg)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot remove '%s': %v", arg, err)
		}
		if !exists {
			continue
		}
		formatted, err := formatKeyForPrompt(store, arg)
		if err != nil {
			return nil, nil, err
		}
		if _, seen := targetSet[arg]; !seen {
			targetSet[arg] = struct{}{}
			targetKeys = append(targetKeys, formatted)
			deleteTargets = append(deleteTargets, arg)
		}
	}

	if len(globPatterns) == 0 {
		return targetKeys, deleteTargets, nil
	}

	type compiledPattern struct {
		rawArg  string
		db      string
		matcher glob.Glob
	}

	var compiled []compiledPattern
	for _, raw := range globPatterns {
		spec, err := store.parseKey(raw, true)
		if err != nil {
			return nil, nil, err
		}
		pattern := spec.Key
		m, err := glob.Compile(pattern, separators...)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot remove '%s': %v", raw, err)
		}
		compiled = append(compiled, compiledPattern{
			rawArg:  raw,
			db:      spec.DB,
			matcher: m,
		})
	}

	keysByDB := make(map[string][]string)
	getKeys := func(db string) ([]string, error) {
		if keys, ok := keysByDB[db]; ok {
			return keys, nil
		}
		keys, err := store.Keys(db)
		if err != nil {
			return nil, err
		}
		keysByDB[db] = keys
		return keys, nil
	}

	for _, p := range compiled {
		keys, err := getKeys(p.db)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot remove '%s': %v", p.rawArg, err)
		}
		for _, k := range keys {
			if p.matcher.Match(k) {
				full := fmt.Sprintf("%s@%s", k, p.db)
				if _, seen := targetSet[full]; seen {
					continue
				}
				targetSet[full] = struct{}{}
				display, err := formatKeyForPrompt(store, full)
				if err != nil {
					return nil, nil, err
				}
				targetKeys = append(targetKeys, display)
				deleteTargets = append(deleteTargets, full)
			}
		}
	}

	return targetKeys, deleteTargets, nil
}
