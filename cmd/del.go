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
	Use:          "del KEY[@STORE] [KEY[@STORE] ...]",
	Short:        "Delete one or more keys",
	Aliases:      []string{"delete", "rm", "remove"},
	Args:         cobra.ArbitraryArgs,
	RunE:         del,
	SilenceUsage: true,
}

func del(cmd *cobra.Command, args []string) error {
	store := &Store{}

	interactive, err := cmd.Flags().GetBool("interactive")
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

	targets, err := resolveDeleteTargets(store, args, globPatterns, separators)
	if err != nil {
		return err
	}

	if len(targets) == 0 {
		return fmt.Errorf("cannot remove: No such key")
	}

	var processed []resolvedTarget
	for _, target := range targets {
		if interactive || config.Key.AlwaysPromptDelete {
			var confirm string
			message := fmt.Sprintf("remove %q: are you sure? (y/n)", target.display)
			fmt.Println(message)
			if _, err := fmt.Scanln(&confirm); err != nil {
				return fmt.Errorf("cannot remove '%s': %v", target.full, err)
			}
			if strings.ToLower(confirm) != "y" {
				continue
			}
		}
		trans := TransactionArgs{
			key:      target.full,
			readonly: false,
			sync:     false,
			transact: func(tx *badger.Txn, k []byte) error {
				if err := tx.Delete(k); errors.Is(err, badger.ErrKeyNotFound) {
					return fmt.Errorf("cannot remove '%s': No such key", target.full)
				}
				if err != nil {
					return fmt.Errorf("cannot remove '%s': %v", target.full, err)
				}
				return nil
			},
		}

		if err := store.Transaction(trans); err != nil {
			return err
		}
		processed = append(processed, target)
	}

	if len(processed) == 0 {
		return nil
	}

	var dbs []string
	var labels []string
	for _, t := range processed {
		spec, err := store.parseKey(t.full, true)
		if err != nil {
			return err
		}
		dbs = append(dbs, spec.DB)
		labels = append(labels, t.display)
	}
	return autoSync()
}

func init() {
	delCmd.Flags().BoolP("interactive", "i", false, "Prompt yes/no for each deletion")
	delCmd.Flags().StringSliceP("glob", "g", nil, "Delete keys matching glob pattern (repeatable)")
	delCmd.Flags().String("glob-sep", "", fmt.Sprintf("Characters treated as separators for globbing (default %q)", defaultGlobSeparatorsDisplay()))
	rootCmd.AddCommand(delCmd)
}

type resolvedTarget struct {
	full    string
	display string
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

func resolveDeleteTargets(store *Store, exactArgs []string, globPatterns []string, separators []rune) ([]resolvedTarget, error) {
	targetSet := make(map[string]struct{})
	var targets []resolvedTarget

	addTarget := func(spec KeySpec) {
		full := spec.Full()
		if _, seen := targetSet[full]; seen {
			return
		}
		targetSet[full] = struct{}{}
		targets = append(targets, resolvedTarget{
			full:    full,
			display: spec.Display(),
		})
	}

	for _, arg := range exactArgs {
		exists, err := keyExists(store, arg)
		if err != nil {
			return nil, fmt.Errorf("cannot remove '%s': %v", arg, err)
		}
		if !exists {
			continue
		}
		spec, err := store.parseKey(arg, true)
		if err != nil {
			return nil, err
		}
		addTarget(spec)
	}

	if len(globPatterns) == 0 {
		return targets, nil
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
			return nil, err
		}
		pattern := spec.Key
		m, err := glob.Compile(pattern, separators...)
		if err != nil {
			return nil, fmt.Errorf("cannot remove '%s': %v", raw, err)
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
			return nil, fmt.Errorf("cannot remove '%s': %v", p.rawArg, err)
		}
		for _, k := range keys {
			if p.matcher.Match(k) {
				addTarget(KeySpec{
					Raw:    k,
					RawKey: k,
					Key:    k,
					DB:     p.db,
				})
			}
		}
	}

	return targets, nil
}
