package cmd

import (
	"strings"

	"github.com/spf13/cobra"
)

// completeKeys returns key[@store] completions for the current toComplete prefix.
// It handles three cases:
//   - No "@" typed yet: return all keys from all stores (as "key@store")
//   - "@" typed with partial store: return store-scoped completions
//   - "key@store" with known store: return keys from that store
func completeKeys(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	store := &Store{}
	stores, err := store.AllStores()
	if err != nil || len(stores) == 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	var completions []string
	parts := strings.SplitN(toComplete, "@", 2)

	if len(parts) == 2 {
		// User typed "something@" — complete keys within matching stores.
		prefix := parts[0]
		dbFilter := strings.ToLower(parts[1])
		for _, db := range stores {
			if !strings.HasPrefix(db, dbFilter) {
				continue
			}
			keys, err := store.Keys(db)
			if err != nil {
				continue
			}
			for _, k := range keys {
				if prefix == "" || strings.HasPrefix(k, strings.ToLower(prefix)) {
					completions = append(completions, k+"@"+db)
				}
			}
		}
	} else {
		// No "@" yet — offer key@store for every key in every store.
		lowerPrefix := strings.ToLower(toComplete)
		for _, db := range stores {
			keys, err := store.Keys(db)
			if err != nil {
				continue
			}
			for _, k := range keys {
				full := k + "@" + db
				if strings.HasPrefix(full, lowerPrefix) || strings.HasPrefix(k, lowerPrefix) {
					completions = append(completions, full)
				}
			}
		}
	}

	return completions, cobra.ShellCompDirectiveNoFileComp
}

// completeStores returns store name completions.
func completeStores(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	store := &Store{}
	stores, err := store.AllStores()
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	var completions []string
	lowerPrefix := strings.ToLower(toComplete)
	for _, db := range stores {
		if strings.HasPrefix(db, lowerPrefix) {
			completions = append(completions, db)
		}
	}
	return completions, cobra.ShellCompDirectiveNoFileComp
}

// completeStoreFlag is a completion function for --store / -s string slice flags.
func completeStoreFlag(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return completeStores(cmd, args, toComplete)
}
