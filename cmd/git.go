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
	"os"
	"os/exec"

	"github.com/spf13/cobra"
)

var gitCmd = &cobra.Command{
	Use:   "git [args...]",
	Short: "Run any arbitrary command. Use with caution.",
	Long: `Run any arbitrary command. Use with caution.

The Git repository lives directly in the data directory
("PDA_DATA"). Store files (*.ndjson) are tracked by Git as-is.

If you manually modify files without using the built-in
commands, you may desync your repository.

Generally prefer "pda sync".`,
	Args:               cobra.ArbitraryArgs,
	DisableFlagParsing: true,
	SilenceUsage:       true,
	RunE: func(cmd *cobra.Command, args []string) error {
		repoDir, err := ensureVCSInitialized()
		if err != nil {
			return err
		}

		gitCmd := exec.Command("git", args...)
		gitCmd.Dir = repoDir
		gitCmd.Stdin = os.Stdin
		gitCmd.Stdout = os.Stdout
		gitCmd.Stderr = os.Stderr
		return gitCmd.Run()
	},
}

func init() {
	rootCmd.AddCommand(gitCmd)
}
