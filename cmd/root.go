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

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:           "pda",
	Short:         "A key-value store tool",
	Long:          asciiArt,
	SilenceErrors: true, // we print errors ourselves
}

func Execute() {
	if configErr != nil {
		printError(fmt.Errorf("cannot load config: %v", configErr))
		os.Exit(1)
	}
	err := rootCmd.Execute()
	if err != nil {
		printErrorWithHints(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddGroup(&cobra.Group{ID: "keys", Title: "Key commands:"})

	setCmd.GroupID = "keys"
	getCmd.GroupID = "keys"
	runCmd.GroupID = "keys"
	mvCmd.GroupID = "keys"
	cpCmd.GroupID = "keys"
	delCmd.GroupID = "keys"
	listCmd.GroupID = "keys"
	identityCmd.GroupID = "keys"

	rootCmd.AddGroup(&cobra.Group{ID: "stores", Title: "Store commands:"})

	listStoresCmd.GroupID = "stores"
	delStoreCmd.GroupID = "stores"
	mvStoreCmd.GroupID = "stores"
	exportCmd.GroupID = "stores"
	restoreCmd.GroupID = "stores"

	rootCmd.AddGroup(&cobra.Group{ID: "git", Title: "Git commands:"})

	initCmd.GroupID = "git"
	syncCmd.GroupID = "git"
	gitCmd.GroupID = "git"
}
