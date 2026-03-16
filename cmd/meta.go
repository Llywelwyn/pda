package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

var metaCmd = &cobra.Command{
	Use:   "meta KEY[@STORE]",
	Short: "View or modify metadata for a key",
	Long: `View or modify metadata (TTL, encryption, read-only, pinned) for a key
without changing its value.

With no flags, displays the key's current metadata. Pass flags to modify.`,
	Args:               cobra.ExactArgs(1),
	ValidArgsFunction:  completeKeys,
	RunE:               meta,
	SilenceUsage: true,
}

func meta(cmd *cobra.Command, args []string) error {
	store := &Store{}

	spec, err := store.parseKey(args[0], true)
	if err != nil {
		return fmt.Errorf("cannot meta '%s': %v", args[0], err)
	}

	identity, _ := loadIdentity()

	p, err := store.storePath(spec.DB)
	if err != nil {
		return fmt.Errorf("cannot meta '%s': %v", args[0], err)
	}
	entries, err := readStoreFile(p, identity)
	if err != nil {
		return fmt.Errorf("cannot meta '%s': %v", args[0], err)
	}
	idx := findEntry(entries, spec.Key)
	if idx < 0 {
		keys := make([]string, len(entries))
		for i, e := range entries {
			keys[i] = e.Key
		}
		return fmt.Errorf("cannot meta '%s': %w", args[0], suggestKey(spec.Key, keys))
	}
	entry := &entries[idx]

	ttlStr, _ := cmd.Flags().GetString("ttl")
	encryptFlag, _ := cmd.Flags().GetBool("encrypt")
	decryptFlag, _ := cmd.Flags().GetBool("decrypt")
	readonlyFlag, _ := cmd.Flags().GetBool("readonly")
	writableFlag, _ := cmd.Flags().GetBool("writable")
	pinFlag, _ := cmd.Flags().GetBool("pin")
	unpinFlag, _ := cmd.Flags().GetBool("unpin")
	force, _ := cmd.Flags().GetBool("force")

	if encryptFlag && decryptFlag {
		return fmt.Errorf("cannot meta '%s': --encrypt and --decrypt are mutually exclusive", args[0])
	}
	if readonlyFlag && writableFlag {
		return fmt.Errorf("cannot meta '%s': --readonly and --writable are mutually exclusive", args[0])
	}
	if pinFlag && unpinFlag {
		return fmt.Errorf("cannot meta '%s': --pin and --unpin are mutually exclusive", args[0])
	}

	// View mode: no flags set
	isModify := ttlStr != "" || encryptFlag || decryptFlag || readonlyFlag || writableFlag || pinFlag || unpinFlag
	if !isModify {
		expiresStr := "never"
		if entry.ExpiresAt > 0 {
			expiresStr = formatExpiry(entry.ExpiresAt)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "  key: %s\n", spec.Full())
		fmt.Fprintf(cmd.OutOrStdout(), "  secret: %v\n", entry.Secret)
		fmt.Fprintf(cmd.OutOrStdout(), "  writable: %v\n", !entry.ReadOnly)
		fmt.Fprintf(cmd.OutOrStdout(), "  pinned: %v\n", entry.Pinned)
		fmt.Fprintf(cmd.OutOrStdout(), "  expires: %s\n", expiresStr)
		return nil
	}

	// Read-only enforcement: --readonly and --writable always work without --force,
	// but other modifications on a read-only key require --force.
	if entry.ReadOnly && !force && !readonlyFlag && !writableFlag {
		onlyPinChange := !encryptFlag && !decryptFlag && ttlStr == "" && (pinFlag || unpinFlag)
		if !onlyPinChange {
			return fmt.Errorf("cannot meta '%s': key is read-only", args[0])
		}
	}

	// Modification mode — may need identity for encrypt
	if encryptFlag {
		identity, err = ensureIdentity()
		if err != nil {
			return fmt.Errorf("cannot meta '%s': %v", args[0], err)
		}
	}
	recipients, err := allRecipients(identity)
	if err != nil {
		return fmt.Errorf("cannot meta '%s': %v", args[0], err)
	}

	var changes []string

	if ttlStr != "" {
		expiresAt, err := parseTTLString(ttlStr)
		if err != nil {
			return fmt.Errorf("cannot meta '%s': %v", args[0], err)
		}
		entry.ExpiresAt = expiresAt
		if expiresAt == 0 {
			changes = append(changes, "cleared ttl")
		} else {
			changes = append(changes, "set ttl to "+ttlStr)
		}
	}

	if encryptFlag {
		if entry.Secret {
			return fmt.Errorf("cannot meta '%s': already encrypted", args[0])
		}
		if entry.Locked {
			return fmt.Errorf("cannot meta '%s': secret is locked (identity file missing)", args[0])
		}
		entry.Secret = true
		changes = append(changes, "encrypted")
	}

	if decryptFlag {
		if !entry.Secret {
			return fmt.Errorf("cannot meta '%s': not encrypted", args[0])
		}
		if entry.Locked {
			return fmt.Errorf("cannot meta '%s': secret is locked (identity file missing)", args[0])
		}
		entry.Secret = false
		changes = append(changes, "decrypted")
	}

	if readonlyFlag {
		entry.ReadOnly = true
		changes = append(changes, "made readonly")
	}
	if writableFlag {
		entry.ReadOnly = false
		changes = append(changes, "made writable")
	}
	if pinFlag {
		entry.Pinned = true
		changes = append(changes, "pinned")
	}
	if unpinFlag {
		entry.Pinned = false
		changes = append(changes, "unpinned")
	}

	if err := writeStoreFile(p, entries, recipients); err != nil {
		return fmt.Errorf("cannot meta '%s': %v", args[0], err)
	}

	summary := strings.Join(changes, ", ")
	okf("%s %s", summary, spec.Display())
	return autoSync(summary + " " + spec.Display())
}

func init() {
	metaCmd.Flags().String("ttl", "", "set expiry (e.g. 30m, 2h) or 'never' to clear")
	metaCmd.Flags().BoolP("encrypt", "e", false, "encrypt the value at rest")
	metaCmd.Flags().BoolP("decrypt", "d", false, "decrypt the value (store as plaintext)")
	metaCmd.Flags().Bool("readonly", false, "mark the key as read-only")
	metaCmd.Flags().Bool("writable", false, "clear the read-only flag")
	metaCmd.Flags().Bool("pin", false, "pin the key (sorts to top in list)")
	metaCmd.Flags().Bool("unpin", false, "unpin the key")
	metaCmd.Flags().Bool("force", false, "bypass read-only protection for metadata changes")
	rootCmd.AddCommand(metaCmd)
}
