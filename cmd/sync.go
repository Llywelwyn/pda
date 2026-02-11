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
	"time"

	"github.com/spf13/cobra"
)

var syncCmd = &cobra.Command{
	Use:          "sync",
	Short:        "Manually sync your stores with Git",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		msg, _ := cmd.Flags().GetString("message")
		return sync(true, msg)
	},
}

func init() {
	syncCmd.Flags().StringP("message", "m", "", "Custom commit message (defaults to timestamp)")
	rootCmd.AddCommand(syncCmd)
}

func sync(manual bool, customMsg string) error {
	repoDir, err := ensureVCSInitialized()
	if err != nil {
		return err
	}

	remoteInfo, err := repoRemoteInfo(repoDir)
	if err != nil {
		return err
	}

	// Commit local changes first so nothing is lost.
	if err := runGit(repoDir, "add", "-A"); err != nil {
		return err
	}
	changed, err := repoHasStagedChanges(repoDir)
	if err != nil {
		return err
	}
	if changed {
		msg := customMsg
		if msg == "" {
			msg = fmt.Sprintf("sync: %s", time.Now().UTC().Format(time.RFC3339))
			if manual {
				printHint("use -m to set a custom commit message")
			}
		}
		if err := runGit(repoDir, "commit", "-m", msg); err != nil {
			return err
		}
	} else if manual {
		okf("no changes to commit")
	}

	if remoteInfo.Ref == "" {
		if manual {
			warnf("no remote configured, skipping push")
		}
		return nil
	}

	// Fetch remote state.
	if manual || config.Git.AutoFetch {
		if err := runGit(repoDir, "fetch", "--prune"); err != nil {
			return err
		}
	}

	// Rebase local commits onto remote if behind.
	ahead, behind, err := repoAheadBehind(repoDir, remoteInfo.Ref)
	if err != nil {
		// Remote ref doesn't exist yet (first push).
		ahead = 1
	} else if behind > 0 {
		if err := pullRemote(repoDir, remoteInfo); err != nil {
			return err
		}
		ahead, _, err = repoAheadBehind(repoDir, remoteInfo.Ref)
		if err != nil {
			return err
		}
	}

	// Push if ahead.
	if manual || config.Git.AutoPush {
		if ahead > 0 {
			return pushRemote(repoDir, remoteInfo)
		}
		if manual {
			okf("nothing to push")
		}
	}

	if manual {
		okf("in sync!")
	}
	return nil
}

func autoSync() error {
	if !config.Git.AutoCommit {
		return nil
	}
	if _, err := ensureVCSInitialized(); err != nil {
		return nil
	}
	return sync(false, "")
}
