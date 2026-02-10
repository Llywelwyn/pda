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
	"strings"

	"github.com/spf13/cobra"
)

var cpCmd = &cobra.Command{
	Use:     "copy FROM[@STORE] TO[@STORE]",
	Aliases: []string{"cp"},
	Short:   "Make a copy of a key",
	Args:    cobra.ExactArgs(2),
	RunE:    cp,
}

var mvCmd = &cobra.Command{
	Use:          "move FROM[@STORE] TO[@STORE]",
	Aliases:      []string{"mv"},
	Short:        "Move a key",
	Args:         cobra.ExactArgs(2),
	RunE:         mv,
	SilenceUsage: true,
}

func cp(cmd *cobra.Command, args []string) error {
	return mvImpl(cmd, args, true)
}

func mv(cmd *cobra.Command, args []string) error {
	keepSource, _ := cmd.Flags().GetBool("copy")
	return mvImpl(cmd, args, keepSource)
}

func mvImpl(cmd *cobra.Command, args []string, keepSource bool) error {
	store := &Store{}

	interactive, err := cmd.Flags().GetBool("interactive")
	if err != nil {
		return err
	}
	promptOverwrite := interactive || config.Key.AlwaysPromptOverwrite

	fromSpec, err := store.parseKey(args[0], true)
	if err != nil {
		return err
	}
	toSpec, err := store.parseKey(args[1], true)
	if err != nil {
		return err
	}

	// Read source
	srcPath, err := store.storePath(fromSpec.DB)
	if err != nil {
		return fmt.Errorf("cannot move '%s': %v", fromSpec.Key, err)
	}
	srcEntries, err := readStoreFile(srcPath)
	if err != nil {
		return fmt.Errorf("cannot move '%s': %v", fromSpec.Key, err)
	}
	srcIdx := findEntry(srcEntries, fromSpec.Key)
	if srcIdx < 0 {
		return fmt.Errorf("cannot move '%s': No such key", fromSpec.Key)
	}
	srcEntry := srcEntries[srcIdx]

	sameStore := fromSpec.DB == toSpec.DB

	// Check destination for overwrite prompt
	dstPath := srcPath
	dstEntries := srcEntries
	if !sameStore {
		dstPath, err = store.storePath(toSpec.DB)
		if err != nil {
			return fmt.Errorf("cannot move '%s': %v", fromSpec.Key, err)
		}
		dstEntries, err = readStoreFile(dstPath)
		if err != nil {
			return fmt.Errorf("cannot move '%s': %v", fromSpec.Key, err)
		}
	}

	dstIdx := findEntry(dstEntries, toSpec.Key)

	if promptOverwrite && dstIdx >= 0 {
		var confirm string
		fmt.Printf("overwrite '%s'? (y/n)\n", toSpec.Display())
		if _, err := fmt.Scanln(&confirm); err != nil {
			return fmt.Errorf("cannot move '%s': %v", fromSpec.Key, err)
		}
		if strings.ToLower(confirm) != "y" {
			return nil
		}
	}

	// Write destination entry
	newEntry := Entry{
		Key:       toSpec.Key,
		Value:     srcEntry.Value,
		ExpiresAt: srcEntry.ExpiresAt,
	}

	if sameStore {
		// Both source and dest in same file
		if dstIdx >= 0 {
			dstEntries[dstIdx] = newEntry
		} else {
			dstEntries = append(dstEntries, newEntry)
		}
		if !keepSource {
			// Remove source - find it again since indices may have changed
			idx := findEntry(dstEntries, fromSpec.Key)
			if idx >= 0 {
				dstEntries = append(dstEntries[:idx], dstEntries[idx+1:]...)
			}
		}
		if err := writeStoreFile(dstPath, dstEntries); err != nil {
			return err
		}
	} else {
		// Different stores
		if dstIdx >= 0 {
			dstEntries[dstIdx] = newEntry
		} else {
			dstEntries = append(dstEntries, newEntry)
		}
		if err := writeStoreFile(dstPath, dstEntries); err != nil {
			return err
		}
		if !keepSource {
			srcEntries = append(srcEntries[:srcIdx], srcEntries[srcIdx+1:]...)
			if err := writeStoreFile(srcPath, srcEntries); err != nil {
				return err
			}
		}
	}

	return autoSync()
}

func init() {
	mvCmd.Flags().Bool("copy", false, "Copy instead of move (keeps source)")
	mvCmd.Flags().BoolP("interactive", "i", false, "Prompt before overwriting destination")
	rootCmd.AddCommand(mvCmd)
	cpCmd.Flags().BoolP("interactive", "i", false, "Prompt before overwriting destination")
	rootCmd.AddCommand(cpCmd)
}
