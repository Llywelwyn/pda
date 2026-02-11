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

// mvStoreCmd represents the move-store command
var mvStoreCmd = &cobra.Command{
	Use:          "move-store FROM TO",
	Short:        "Rename a store",
	Aliases:      []string{"mvs"},
	Args:         cobra.ExactArgs(2),
	RunE:         mvStore,
	SilenceUsage: true,
}

func mvStore(cmd *cobra.Command, args []string) error {
	store := &Store{}

	fromName, err := store.parseDB(args[0], false)
	if err != nil {
		return fmt.Errorf("cannot rename store '%s': %v", args[0], err)
	}
	toName, err := store.parseDB(args[1], false)
	if err != nil {
		return fmt.Errorf("cannot rename store '%s': %v", args[1], err)
	}

	if fromName == toName {
		return fmt.Errorf("cannot rename store '%s': source and destination are the same", fromName)
	}

	var notFound errNotFound
	fromPath, err := store.FindStore(fromName)
	if errors.As(err, &notFound) {
		return fmt.Errorf("cannot rename store '%s': %w", fromName, err)
	}
	if err != nil {
		return fmt.Errorf("cannot rename store '%s': %v", fromName, err)
	}

	interactive, err := cmd.Flags().GetBool("interactive")
	if err != nil {
		return fmt.Errorf("cannot rename store '%s': %v", fromName, err)
	}
	safe, err := cmd.Flags().GetBool("safe")
	if err != nil {
		return fmt.Errorf("cannot rename store '%s': %v", fromName, err)
	}
	yes, err := cmd.Flags().GetBool("yes")
	if err != nil {
		return fmt.Errorf("cannot rename store '%s': %v", fromName, err)
	}
	promptOverwrite := !yes && (interactive || config.Store.AlwaysPromptOverwrite)

	toPath, err := store.storePath(toName)
	if err != nil {
		return fmt.Errorf("cannot rename store '%s': %v", fromName, err)
	}
	if _, err := os.Stat(toPath); err == nil {
		if safe {
			infof("skipped '@%s': already exists", toName)
			return nil
		}
		if promptOverwrite {
			promptf("overwrite store '%s'? (y/n)", toName)
			var confirm string
			if err := scanln(&confirm); err != nil {
				return fmt.Errorf("cannot rename store '%s': %v", fromName, err)
			}
			if strings.ToLower(confirm) != "y" {
				return nil
			}
		}
	}

	copy, _ := cmd.Flags().GetBool("copy")
	if copy {
		data, err := os.ReadFile(fromPath)
		if err != nil {
			return fmt.Errorf("cannot copy store '%s': %v", fromName, err)
		}
		if err := os.WriteFile(toPath, data, 0o640); err != nil {
			return fmt.Errorf("cannot copy store '%s': %v", fromName, err)
		}
		okf("copied @%s to @%s", fromName, toName)
	} else {
		if err := os.Rename(fromPath, toPath); err != nil {
			return fmt.Errorf("cannot rename store '%s': %v", fromName, err)
		}
		okf("renamed @%s to @%s", fromName, toName)
	}
	return autoSync()
}

func init() {
	mvStoreCmd.Flags().Bool("copy", false, "Copy instead of move (keeps source)")
	mvStoreCmd.Flags().BoolP("interactive", "i", false, "Prompt before overwriting destination")
	mvStoreCmd.Flags().BoolP("yes", "y", false, "Skip all confirmation prompts")
	mvStoreCmd.Flags().Bool("safe", false, "Do not overwrite if the destination store already exists")
	rootCmd.AddCommand(mvStoreCmd)
}
