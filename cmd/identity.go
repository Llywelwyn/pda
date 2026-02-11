package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var identityCmd = &cobra.Command{
	Use:          "identity",
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
	return nil
}

func init() {
	identityCmd.Flags().Bool("new", false, "Generate a new identity (errors if one already exists)")
	identityCmd.Flags().Bool("path", false, "Print only the identity file path")
	identityCmd.MarkFlagsMutuallyExclusive("new", "path")
	rootCmd.AddCommand(identityCmd)
}
