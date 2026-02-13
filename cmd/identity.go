package cmd

import (
	"fmt"

	"filippo.io/age"
	"github.com/spf13/cobra"
)

var identityCmd = &cobra.Command{
	Use:          "identity",
	Aliases:      []string{"id"},
	Short:        "Show or create the age encryption identity",
	Args:         cobra.NoArgs,
	RunE:         identityRun,
	SilenceUsage: true,
}

func identityRun(cmd *cobra.Command, args []string) error {
	showPath, err := cmd.Flags().GetBool("path")
	if err != nil {
		return err
	}
	createNew, err := cmd.Flags().GetBool("new")
	if err != nil {
		return err
	}
	addRecipient, err := cmd.Flags().GetString("add-recipient")
	if err != nil {
		return err
	}
	removeRecipient, err := cmd.Flags().GetString("remove-recipient")
	if err != nil {
		return err
	}

	if createNew {
		existing, err := loadIdentity()
		if err != nil {
			return fmt.Errorf("cannot create identity: %v", err)
		}
		if existing != nil {
			path, _ := identityPath()
			return withHint(
				fmt.Errorf("identity already exists at %s", path),
				"delete the file manually before creating a new one",
			)
		}
		id, err := ensureIdentity()
		if err != nil {
			return fmt.Errorf("cannot create identity: %v", err)
		}
		okf("pubkey  %s", id.Recipient())
		return nil
	}

	if addRecipient != "" {
		return identityAddRecipient(addRecipient)
	}

	if removeRecipient != "" {
		return identityRemoveRecipient(removeRecipient)
	}

	if showPath {
		path, err := identityPath()
		if err != nil {
			return err
		}
		fmt.Println(path)
		return nil
	}

	// Default: show identity info
	id, err := loadIdentity()
	if err != nil {
		return fmt.Errorf("cannot load identity: %v", err)
	}
	if id == nil {
		printHint("no identity found — use 'pda identity --new' or 'pda set --encrypt' to create one")
		return nil
	}
	path, _ := identityPath()
	okf("pubkey  %s", id.Recipient())
	okf("identity  %s", path)

	extra, err := loadRecipients()
	if err != nil {
		return fmt.Errorf("cannot load recipients: %v", err)
	}
	for _, r := range extra {
		okf("recipient  %s", r)
	}

	return nil
}

func identityAddRecipient(key string) error {
	r, err := age.ParseX25519Recipient(key)
	if err != nil {
		return fmt.Errorf("cannot add recipient: %v", err)
	}

	identity, err := loadIdentity()
	if err != nil {
		return fmt.Errorf("cannot add recipient: %v", err)
	}
	if identity == nil {
		return withHint(
			fmt.Errorf("cannot add recipient: no identity found"),
			"create one first with 'pda identity --new'",
		)
	}

	if r.String() == identity.Recipient().String() {
		return fmt.Errorf("cannot add recipient: key is your own identity")
	}

	existing, err := loadRecipients()
	if err != nil {
		return fmt.Errorf("cannot add recipient: %v", err)
	}
	for _, e := range existing {
		if e.String() == r.String() {
			return fmt.Errorf("cannot add recipient: key already present")
		}
	}

	existing = append(existing, r)
	if err := saveRecipients(existing); err != nil {
		return fmt.Errorf("cannot add recipient: %v", err)
	}

	recipients, err := allRecipients(identity)
	if err != nil {
		return fmt.Errorf("cannot add recipient: %v", err)
	}

	count, err := reencryptAllStores(identity, recipients)
	if err != nil {
		return fmt.Errorf("cannot add recipient: %v", err)
	}

	okf("added recipient %s", r)
	if count > 0 {
		okf("re-encrypted %d secret(s)", count)
	}
	return autoSync("added recipient")
}

func identityRemoveRecipient(key string) error {
	r, err := age.ParseX25519Recipient(key)
	if err != nil {
		return fmt.Errorf("cannot remove recipient: %v", err)
	}

	identity, err := loadIdentity()
	if err != nil {
		return fmt.Errorf("cannot remove recipient: %v", err)
	}
	if identity == nil {
		return withHint(
			fmt.Errorf("cannot remove recipient: no identity found"),
			"create one first with 'pda identity --new'",
		)
	}

	existing, err := loadRecipients()
	if err != nil {
		return fmt.Errorf("cannot remove recipient: %v", err)
	}

	found := false
	var updated []*age.X25519Recipient
	for _, e := range existing {
		if e.String() == r.String() {
			found = true
			continue
		}
		updated = append(updated, e)
	}
	if !found {
		return fmt.Errorf("cannot remove recipient: key not found")
	}

	if err := saveRecipients(updated); err != nil {
		return fmt.Errorf("cannot remove recipient: %v", err)
	}

	recipients, err := allRecipients(identity)
	if err != nil {
		return fmt.Errorf("cannot remove recipient: %v", err)
	}

	count, err := reencryptAllStores(identity, recipients)
	if err != nil {
		return fmt.Errorf("cannot remove recipient: %v", err)
	}

	okf("removed recipient %s", r)
	if count > 0 {
		okf("re-encrypted %d secret(s)", count)
	}
	return autoSync("removed recipient")
}

func init() {
	identityCmd.Flags().Bool("new", false, "generate a new identity (errors if one already exists)")
	identityCmd.Flags().Bool("path", false, "print only the identity file path")
	identityCmd.Flags().String("add-recipient", "", "add an age public key as an additional encryption recipient")
	identityCmd.Flags().String("remove-recipient", "", "remove an age public key from the recipient list")
	identityCmd.MarkFlagsMutuallyExclusive("new", "path", "add-recipient", "remove-recipient")
	rootCmd.AddCommand(identityCmd)
}
