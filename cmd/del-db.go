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

	"github.com/spf13/cobra"
	"os"
)

// delDbCmd represents the set command
var delDbCmd = &cobra.Command{
	Use:          "del-db DB",
	Short:        "Delete a database.",
	Aliases:      []string{"delete-db", "rm-db", "remove-db"},
	Args:         cobra.ExactArgs(1),
	RunE:         delDb,
	SilenceUsage: true,
}

func delDb(cmd *cobra.Command, args []string) error {
	store := &Store{}
	var notFound errNotFound
	path, err := store.FindStore(args[0])
	if errors.As(err, &notFound) {
		return fmt.Errorf("cannot delete-db '%s': %v", args[0], err)
	}
	if err != nil {
		return fmt.Errorf("cannot delete-db '%s': %v", args[0], err)
	}

	force, err := cmd.Flags().GetBool("force")
	if err != nil {
		return fmt.Errorf("cannot delete-db '%s': %v", args[0], err)
	}

	if force {
		return executeDeletion(path)
	}

	message := fmt.Sprintf("delete-db '%s': are you sure? (y/n)", args[0])
	fmt.Println(message)

	var confirm string
	if _, err := fmt.Scanln(&confirm); err != nil {
		return fmt.Errorf("cannot delete-db '%s': %v", args[0], err)
	}
	if strings.ToLower(confirm) == "y" {
		return executeDeletion(path)
	}
	return nil
}

func executeDeletion(path string) error {
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("cannot delete-db '%s': %v", path, err)
	}
	return nil
}

func init() {
	delDbCmd.Flags().BoolP("force", "f", false, "Force delete without confirmation")
	rootCmd.AddCommand(delDbCmd)
}
