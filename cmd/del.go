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

	"github.com/gobwas/glob"
	"github.com/spf13/cobra"
)

// delCmd represents the remove command
var delCmd = &cobra.Command{
	Use:          "remove KEY[@STORE] [KEY[@STORE] ...]",
	Short:        "Delete one or more keys",
	Aliases:      []string{"rm"},
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
	yes, err := cmd.Flags().GetBool("yes")
	if err != nil {
		return err
	}
	keyPatterns, err := cmd.Flags().GetStringSlice("key")
	if err != nil {
		return err
	}
	valuePatterns, err := cmd.Flags().GetStringSlice("value")
	if err != nil {
		return err
	}
	storePatterns, err := cmd.Flags().GetStringSlice("store")
	if err != nil {
		return err
	}

	hasFilters := len(keyPatterns) > 0 || len(valuePatterns) > 0 || len(storePatterns) > 0
	if len(args) == 0 && !hasFilters {
		return fmt.Errorf("cannot remove: no keys provided")
	}

	targets, err := resolveDeleteTargets(store, args, keyPatterns, valuePatterns, storePatterns)
	if err != nil {
		return err
	}

	if len(targets) == 0 {
		return fmt.Errorf("cannot remove: no such key")
	}

	// Group targets by store for batch deletes.
	type storeTargets struct {
		targets []resolvedTarget
	}
	byStore := make(map[string]*storeTargets)
	var storeOrder []string
	promptGlob := hasFilters && config.Key.AlwaysPromptGlobDelete
	for _, target := range targets {
		if !yes && (interactive || config.Key.AlwaysPromptDelete || promptGlob) {
			var confirm string
			promptf("remove '%s'? (y/n)", target.display)
			if err := scanln(&confirm); err != nil {
				return fmt.Errorf("cannot remove '%s': %v", target.full, err)
			}
			if strings.ToLower(confirm) != "y" {
				continue
			}
		}
		if _, ok := byStore[target.db]; !ok {
			byStore[target.db] = &storeTargets{}
			storeOrder = append(storeOrder, target.db)
		}
		byStore[target.db].targets = append(byStore[target.db].targets, target)
	}

	if len(byStore) == 0 {
		return nil
	}

	var removedNames []string
	for _, dbName := range storeOrder {
		st := byStore[dbName]
		p, err := store.storePath(dbName)
		if err != nil {
			return err
		}
		entries, err := readStoreFile(p, nil)
		if err != nil {
			return err
		}
		for _, t := range st.targets {
			idx := findEntry(entries, t.key)
			if idx < 0 {
				return fmt.Errorf("cannot remove '%s': no such key", t.full)
			}
			entries = append(entries[:idx], entries[idx+1:]...)
			removedNames = append(removedNames, t.display)
		}
		if err := writeStoreFile(p, entries, nil); err != nil {
			return err
		}
	}

	return autoSync("removed " + strings.Join(removedNames, ", "))
}

func init() {
	delCmd.Flags().BoolP("interactive", "i", false, "prompt yes/no for each deletion")
	delCmd.Flags().BoolP("yes", "y", false, "skip all confirmation prompts")
	delCmd.Flags().StringSliceP("key", "k", nil, "delete keys matching glob pattern (repeatable)")
	delCmd.Flags().StringSliceP("store", "s", nil, "target stores matching glob pattern (repeatable)")
	delCmd.Flags().StringSliceP("value", "v", nil, "delete entries matching value glob pattern (repeatable)")
	rootCmd.AddCommand(delCmd)
}

type resolvedTarget struct {
	full    string
	display string
	key     string
	db      string
}

func keyExists(store *Store, arg string) (bool, error) {
	spec, err := store.parseKey(arg, true)
	if err != nil {
		return false, err
	}
	p, err := store.storePath(spec.DB)
	if err != nil {
		return false, err
	}
	entries, err := readStoreFile(p, nil)
	if err != nil {
		return false, err
	}
	return findEntry(entries, spec.Key) >= 0, nil
}

func resolveDeleteTargets(store *Store, exactArgs []string, globPatterns []string, valuePatterns []string, storePatterns []string) ([]resolvedTarget, error) {
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
			key:     spec.Key,
			db:      spec.DB,
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

	if len(globPatterns) == 0 && len(valuePatterns) == 0 && len(storePatterns) == 0 {
		return targets, nil
	}

	// Resolve --store patterns into a list of target stores.
	storeMatchers, err := compileGlobMatchers(storePatterns)
	if err != nil {
		return nil, fmt.Errorf("cannot remove: %v", err)
	}

	valueMatchers, err := compileValueMatchers(valuePatterns)
	if err != nil {
		return nil, fmt.Errorf("cannot remove: %v", err)
	}

	type compiledPattern struct {
		rawArg  string
		db      string
		matcher glob.Glob
	}

	// When --store or --value is given without --key, match all keys.
	if len(globPatterns) == 0 {
		globPatterns = []string{"**"}
	}

	var compiled []compiledPattern
	for _, raw := range globPatterns {
		spec, err := store.parseKey(raw, true)
		if err != nil {
			return nil, err
		}
		pattern := spec.Key
		m, err := glob.Compile(pattern, defaultGlobSeparators...)
		if err != nil {
			return nil, fmt.Errorf("cannot remove '%s': %v", raw, err)
		}
		if len(storeMatchers) > 0 && !strings.Contains(raw, "@") {
			// --store given and pattern has no explicit @STORE: expand across matching stores.
			allStores, err := store.AllStores()
			if err != nil {
				return nil, fmt.Errorf("cannot remove: %v", err)
			}
			for _, s := range allStores {
				if globMatch(storeMatchers, s) {
					compiled = append(compiled, compiledPattern{rawArg: raw, db: s, matcher: m})
				}
			}
		} else {
			compiled = append(compiled, compiledPattern{rawArg: raw, db: spec.DB, matcher: m})
		}
	}

	entriesByDB := make(map[string][]Entry)
	getEntries := func(db string) ([]Entry, error) {
		if entries, ok := entriesByDB[db]; ok {
			return entries, nil
		}
		p, err := store.storePath(db)
		if err != nil {
			return nil, err
		}
		entries, err := readStoreFile(p, nil)
		if err != nil {
			return nil, err
		}
		entriesByDB[db] = entries
		return entries, nil
	}

	for _, p := range compiled {
		entries, err := getEntries(p.db)
		if err != nil {
			return nil, fmt.Errorf("cannot remove '%s': %v", p.rawArg, err)
		}
		for _, e := range entries {
			if p.matcher.Match(e.Key) && valueMatch(valueMatchers, e) {
				addTarget(KeySpec{
					Raw:    e.Key,
					RawKey: e.Key,
					Key:    e.Key,
					DB:     p.db,
				})
			}
		}
	}

	return targets, nil
}
