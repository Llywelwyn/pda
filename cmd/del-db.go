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
	"os"
	"strings"

	"github.com/spf13/cobra"
)

// delStoreCmd represents the set command
var delStoreCmd = &cobra.Command{
	Use:          "remove-store STORE",
	Short:        "Delete a store",
	Aliases:      []string{"rms"},
	Args:         cobra.ExactArgs(1),
	RunE:         delStore,
	SilenceUsage: true,
}

func delStore(cmd *cobra.Command, args []string) error {
	store := &Store{}
	dbName, err := store.parseDB(args[0], false)
	if err != nil {
		return fmt.Errorf("cannot delete store '%s': %v", args[0], err)
	}
	var notFound errNotFound
	path, err := store.FindStore(dbName)
	if errors.As(err, &notFound) {
		return fmt.Errorf("cannot delete store '%s': %w", dbName, err)
	}
	if err != nil {
		return fmt.Errorf("cannot delete store '%s': %v", dbName, err)
	}

	interactive, err := cmd.Flags().GetBool("interactive")
	if err != nil {
		return fmt.Errorf("cannot delete store '%s': %v", dbName, err)
	}
	yes, err := cmd.Flags().GetBool("yes")
	if err != nil {
		return fmt.Errorf("cannot delete store '%s': %v", dbName, err)
	}

	if !yes && (interactive || config.Store.AlwaysPromptDelete) {
		promptf("delete store '%s'? (y/n)", args[0])

		var confirm string
		if err := scanln(&confirm); err != nil {
			return fmt.Errorf("cannot delete store '%s': %v", dbName, err)
		}
		if strings.ToLower(confirm) != "y" {
			return nil
		}
	}
	if err := executeDeletion(path); err != nil {
		return err
	}
	return autoSync(fmt.Sprintf("removed @%s", dbName))
}

func executeDeletion(path string) error {
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("cannot delete store '%s': %v", path, err)
	}
	return nil
}

func init() {
	delStoreCmd.Flags().BoolP("interactive", "i", false, "prompt yes/no for each deletion")
	delStoreCmd.Flags().BoolP("yes", "y", false, "skip all confirmation prompts")
	rootCmd.AddCommand(delStoreCmd)
}
