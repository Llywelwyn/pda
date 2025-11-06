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
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"os"
)

// delDbCmd represents the set command
var delDbCmd = &cobra.Command{
	Use:   "delete-db DB",
	Short: "Delete a database.",
	Args:  cobra.ExactArgs(1),
	RunE:  delDb,
}

func delDb(cmd *cobra.Command, args []string) error {
	store := &Store{}
	var notFound errNotFound
	path, err := store.FindStore(args[0])
	if errors.As(err, &notFound) {
		fmt.Fprintf(os.Stderr, "%q does not exist, %s\n", args[0], err.Error())
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "unexpected error: %s", err.Error())
		os.Exit(1)
	}

	var confirm string
	home, err := os.UserHomeDir()
	nicepath := path
	if err == nil && strings.HasPrefix(path, home) {
		nicepath = filepath.Join("~", strings.TrimPrefix(nicepath, home))
	}

	force, err := cmd.Flags().GetBool("force")
	if err != nil {
		return err
	}

	if force {
		return executeDeletion(path, nicepath)
	}

	message := fmt.Sprintf("Are you sure you want to delete '%s'? (y/n)", nicepath)
	fmt.Println(message)
	if _, err := fmt.Scanln(&confirm); err != nil {
		return err
	}
	if strings.ToLower(confirm) == "y" {
		return executeDeletion(path, nicepath)
	}
	fmt.Fprintf(os.Stderr, "Did not delete %q\n", nicepath)
	return nil
}

func executeDeletion(path, nicepath string) error {
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Deleted %q\n", nicepath)
	return nil
}

func init() {
	delDbCmd.Flags().BoolP("force", "f", false, "Force delete without confirmation")
	rootCmd.AddCommand(delDbCmd)
}
