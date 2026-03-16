package cmd

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"unicode/utf8"

	"filippo.io/age"
	"github.com/spf13/cobra"
)

var editCmd = &cobra.Command{
	Use:   "edit KEY[@STORE]",
	Short: "Edit a key's value in $EDITOR",
	Long: `Open a key's value in $EDITOR. If the key doesn't exist, opens an
empty file — saving non-empty content creates the key.

Binary values are presented as base64 for editing and decoded back on save.

Metadata flags (--ttl, --encrypt, --decrypt) can be passed alongside the edit
to modify metadata in the same operation.`,
	Aliases:            []string{"e"},
	Args:               cobra.ExactArgs(1),
	ValidArgsFunction:  completeKeys,
	RunE:               edit,
	SilenceUsage: true,
}

func edit(cmd *cobra.Command, args []string) error {
	editor := os.Getenv("EDITOR")
	if editor == "" {
		return withHint(
			fmt.Errorf("EDITOR not set"),
			"set $EDITOR to your preferred text editor",
		)
	}

	store := &Store{}

	spec, err := store.parseKey(args[0], true)
	if err != nil {
		return fmt.Errorf("cannot edit '%s': %v", args[0], err)
	}

	ttlStr, _ := cmd.Flags().GetString("ttl")
	encryptFlag, _ := cmd.Flags().GetBool("encrypt")
	decryptFlag, _ := cmd.Flags().GetBool("decrypt")
	preserveNewline, _ := cmd.Flags().GetBool("preserve-newline")
	force, _ := cmd.Flags().GetBool("force")
	readonlyFlag, _ := cmd.Flags().GetBool("readonly")
	writableFlag, _ := cmd.Flags().GetBool("writable")
	pinFlag, _ := cmd.Flags().GetBool("pin")
	unpinFlag, _ := cmd.Flags().GetBool("unpin")

	if encryptFlag && decryptFlag {
		return fmt.Errorf("cannot edit '%s': --encrypt and --decrypt are mutually exclusive", args[0])
	}
	if readonlyFlag && writableFlag {
		return fmt.Errorf("cannot edit '%s': --readonly and --writable are mutually exclusive", args[0])
	}
	if pinFlag && unpinFlag {
		return fmt.Errorf("cannot edit '%s': --pin and --unpin are mutually exclusive", args[0])
	}

	// Load identity
	var identity *age.X25519Identity
	if encryptFlag {
		identity, err = ensureIdentity()
		if err != nil {
			return fmt.Errorf("cannot edit '%s': %v", args[0], err)
		}
	} else {
		identity, _ = loadIdentity()
	}
	recipients, err := allRecipients(identity)
	if err != nil {
		return fmt.Errorf("cannot edit '%s': %v", args[0], err)
	}

	p, err := store.storePath(spec.DB)
	if err != nil {
		return fmt.Errorf("cannot edit '%s': %v", args[0], err)
	}
	entries, err := readStoreFile(p, identity)
	if err != nil {
		return fmt.Errorf("cannot edit '%s': %v", args[0], err)
	}
	idx := findEntry(entries, spec.Key)

	creating := idx < 0
	var original []byte
	var wasBinary bool
	var entry *Entry

	if creating {
		original = nil
	} else {
		entry = &entries[idx]
		if entry.ReadOnly && !force {
			return fmt.Errorf("cannot edit '%s': key is read-only", args[0])
		}
		if entry.Locked {
			return fmt.Errorf("cannot edit '%s': secret is locked (identity file missing)", args[0])
		}
		original = entry.Value
		wasBinary = !utf8.Valid(original)
	}

	// Prepare temp file content
	var tmpContent []byte
	if wasBinary {
		tmpContent = []byte(base64.StdEncoding.EncodeToString(original))
	} else {
		tmpContent = original
	}

	// Write to temp file
	tmpFile, err := os.CreateTemp("", "pda-edit-*")
	if err != nil {
		return fmt.Errorf("cannot edit '%s': %v", args[0], err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if _, err := tmpFile.Write(tmpContent); err != nil {
		tmpFile.Close()
		return fmt.Errorf("cannot edit '%s': %v", args[0], err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("cannot edit '%s': %v", args[0], err)
	}

	// Launch editor
	c := exec.Command(editor, tmpPath)
	c.Stdin = os.Stdin
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if err := c.Run(); err != nil {
		return fmt.Errorf("cannot edit '%s': editor failed: %v", args[0], err)
	}

	// Read back
	edited, err := os.ReadFile(tmpPath)
	if err != nil {
		return fmt.Errorf("cannot edit '%s': %v", args[0], err)
	}

	// Decode base64 if original was binary; strip trailing newlines for text
	// unless --preserve-newline is set
	var newValue []byte
	if wasBinary {
		decoded, err := base64.StdEncoding.DecodeString(string(bytes.TrimSpace(edited)))
		if err != nil {
			return fmt.Errorf("cannot edit '%s': invalid base64: %v", args[0], err)
		}
		newValue = decoded
	} else if preserveNewline {
		newValue = edited
	} else {
		newValue = bytes.TrimRight(edited, "\n")
	}

	// Check for no-op
	noMetaFlags := ttlStr == "" && !encryptFlag && !decryptFlag && !readonlyFlag && !writableFlag && !pinFlag && !unpinFlag
	if bytes.Equal(original, newValue) && noMetaFlags {
		infof("no changes to '%s'", spec.Display())
		return nil
	}

	// Creating: empty save means abort
	if creating && len(newValue) == 0 && noMetaFlags {
		infof("empty value, nothing saved")
		return nil
	}

	// Build or update entry
	if creating {
		newEntry := Entry{
			Key:      spec.Key,
			Value:    newValue,
			Secret:   encryptFlag,
			ReadOnly: readonlyFlag,
			Pinned:   pinFlag,
		}
		if ttlStr != "" {
			expiresAt, err := parseTTLString(ttlStr)
			if err != nil {
				return fmt.Errorf("cannot edit '%s': %v", args[0], err)
			}
			newEntry.ExpiresAt = expiresAt
		}
		entries = append(entries, newEntry)
	} else {
		entry.Value = newValue

		if ttlStr != "" {
			expiresAt, err := parseTTLString(ttlStr)
			if err != nil {
				return fmt.Errorf("cannot edit '%s': %v", args[0], err)
			}
			entry.ExpiresAt = expiresAt
		}

		if encryptFlag {
			if entry.Secret {
				return fmt.Errorf("cannot edit '%s': already encrypted", args[0])
			}
			entry.Secret = true
		}
		if decryptFlag {
			if !entry.Secret {
				return fmt.Errorf("cannot edit '%s': not encrypted", args[0])
			}
			entry.Secret = false
		}

		if readonlyFlag {
			entry.ReadOnly = true
		}
		if writableFlag {
			entry.ReadOnly = false
		}
		if pinFlag {
			entry.Pinned = true
		}
		if unpinFlag {
			entry.Pinned = false
		}
	}

	if err := writeStoreFile(p, entries, recipients); err != nil {
		return fmt.Errorf("cannot edit '%s': %v", args[0], err)
	}

	if creating {
		okf("created '%s'", spec.Display())
	} else {
		okf("updated '%s'", spec.Display())
	}

	return autoSync("edit " + spec.Display())
}

func init() {
	editCmd.Flags().String("ttl", "", "set expiry (e.g. 30m, 2h) or 'never' to clear")
	editCmd.Flags().BoolP("encrypt", "e", false, "encrypt the value at rest")
	editCmd.Flags().BoolP("decrypt", "d", false, "decrypt the value (store as plaintext)")
	editCmd.Flags().Bool("preserve-newline", false, "keep trailing newlines added by the editor")
	editCmd.Flags().Bool("force", false, "bypass read-only protection")
	editCmd.Flags().Bool("readonly", false, "mark the key as read-only")
	editCmd.Flags().Bool("writable", false, "clear the read-only flag")
	editCmd.Flags().Bool("pin", false, "pin the key (sorts to top in list)")
	editCmd.Flags().Bool("unpin", false, "unpin the key")
	rootCmd.AddCommand(editCmd)
}
