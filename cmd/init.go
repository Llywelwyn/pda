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
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:          "init [remote-url]",
	Short:        "Initialise pda! version control",
	SilenceUsage: true,
	Args:         cobra.MaximumNArgs(1),
	RunE:         vcsInit,
}

func init() {
	initCmd.Flags().Bool("clean", false, "Remove .git from stores directory before initialising")
	rootCmd.AddCommand(initCmd)
}

func vcsInit(cmd *cobra.Command, args []string) error {
	repoDir, err := vcsRepoRoot()
	if err != nil {
		return err
	}
	store := &Store{}

	clean, err := cmd.Flags().GetBool("clean")
	if err != nil {
		return err
	}

	hasRemote := len(args) == 1

	if clean {
		gitDir := filepath.Join(repoDir, ".git")
		if _, err := os.Stat(gitDir); err == nil {
			fmt.Printf("remove .git from '%s'? (y/n)\n", repoDir)
			var confirm string
			if _, err := fmt.Scanln(&confirm); err != nil {
				return fmt.Errorf("cannot clean git dir: %w", err)
			}
			if strings.ToLower(confirm) != "y" {
				return fmt.Errorf("aborted cleaning git dir")
			}
			if err := os.RemoveAll(gitDir); err != nil {
				return fmt.Errorf("cannot clean git dir: %w", err)
			}
		}

		if hasRemote {
			dbs, err := store.AllStores()
			if err == nil && len(dbs) > 0 {
				fmt.Printf("remove all existing stores and .gitignore? (required for clone) (y/n)\n")
				var confirm string
				if _, err := fmt.Scanln(&confirm); err != nil {
					return fmt.Errorf("cannot clean stores: %w", err)
				}
				if strings.ToLower(confirm) != "y" {
					return fmt.Errorf("aborted cleaning stores")
				}
				if err := wipeAllStores(store); err != nil {
					return fmt.Errorf("cannot clean stores: %w", err)
				}
				gi := filepath.Join(repoDir, ".gitignore")
				if err := os.Remove(gi); err != nil && !os.IsNotExist(err) {
					return fmt.Errorf("cannot remove .gitignore: %w", err)
				}
			}
		}
	}

	gitDir := filepath.Join(repoDir, ".git")
	if _, err := os.Stat(gitDir); err == nil {
		fmt.Println("vcs already initialised; use --clean to reinitialise")
		return nil
	}

	if hasRemote {
		// git clone requires the target directory to be empty
		entries, err := os.ReadDir(repoDir)
		if err == nil && len(entries) > 0 {
			return fmt.Errorf("stores directory is not empty; use --clean with a remote to wipe and clone")
		}

		remote := args[0]
		fmt.Printf("running: git clone %s %s\n", remote, repoDir)
		if err := runGit("", "clone", remote, repoDir); err != nil {
			return err
		}
	} else {
		if err := os.MkdirAll(repoDir, 0o750); err != nil {
			return err
		}
		fmt.Printf("running: git init\n")
		if err := runGit(repoDir, "init"); err != nil {
			return err
		}
	}

	return writeGitignore(repoDir)
}
