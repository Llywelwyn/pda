package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var metaCmd = &cobra.Command{
	Use:   "meta KEY[@STORE]",
	Short: "View or modify metadata for a key",
	Long: `View or modify metadata (TTL, encryption) for a key without changing its value.

With no flags, displays the key's current metadata. Use flags to modify:
  --ttl DURATION   Set expiry (e.g. 30m, 2h)
  --ttl never      Remove expiry
  --encrypt        Encrypt the value at rest
  --decrypt        Decrypt the value (store as plaintext)`,
	Args:         cobra.ExactArgs(1),
	RunE:         meta,
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

	if encryptFlag && decryptFlag {
		return fmt.Errorf("cannot meta '%s': --encrypt and --decrypt are mutually exclusive", args[0])
	}

	// View mode: no flags set
	if ttlStr == "" && !encryptFlag && !decryptFlag {
		expiresStr := "never"
		if entry.ExpiresAt > 0 {
			expiresStr = formatExpiry(entry.ExpiresAt)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "  key: %s\n", spec.Full())
		fmt.Fprintf(cmd.OutOrStdout(), "  secret: %v\n", entry.Secret)
		fmt.Fprintf(cmd.OutOrStdout(), "  expires: %s\n", expiresStr)
		return nil
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

	if ttlStr != "" {
		expiresAt, err := parseTTLString(ttlStr)
		if err != nil {
			return fmt.Errorf("cannot meta '%s': %v", args[0], err)
		}
		entry.ExpiresAt = expiresAt
	}

	if encryptFlag {
		if entry.Secret {
			return fmt.Errorf("cannot meta '%s': already encrypted", args[0])
		}
		if entry.Locked {
			return fmt.Errorf("cannot meta '%s': secret is locked (identity file missing)", args[0])
		}
		entry.Secret = true
	}

	if decryptFlag {
		if !entry.Secret {
			return fmt.Errorf("cannot meta '%s': not encrypted", args[0])
		}
		if entry.Locked {
			return fmt.Errorf("cannot meta '%s': secret is locked (identity file missing)", args[0])
		}
		entry.Secret = false
	}

	if err := writeStoreFile(p, entries, recipients); err != nil {
		return fmt.Errorf("cannot meta '%s': %v", args[0], err)
	}

	return autoSync("meta " + spec.Display())
}

func init() {
	metaCmd.Flags().String("ttl", "", "set expiry (e.g. 30m, 2h) or 'never' to clear")
	metaCmd.Flags().BoolP("encrypt", "e", false, "encrypt the value at rest")
	metaCmd.Flags().BoolP("decrypt", "d", false, "decrypt the value (store as plaintext)")
	rootCmd.AddCommand(metaCmd)
}
