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
	"time"

	"github.com/spf13/cobra"
)

var syncCmd = &cobra.Command{
	Use:          "sync",
	Short:        "Manually sync your stores with Git",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return sync(true)
	},
}

func init() {
	rootCmd.AddCommand(syncCmd)
}

func sync(manual bool) error {
	store := &Store{}
	repoDir, err := ensureVCSInitialized()
	if err != nil {
		return err
	}

	remoteInfo, err := repoRemoteInfo(repoDir)
	if err != nil {
		return err
	}

	var ahead int
	if remoteInfo.Ref != "" {
		if manual || config.Git.AutoFetch {
			if err := runGit(repoDir, "fetch", "--prune"); err != nil {
				return err
			}
		}
		remoteAhead, behind, err := repoAheadBehind(repoDir, remoteInfo.Ref)
		if err != nil {
			ahead = 1 // ref doesn't exist yet; just push
		} else {
			ahead = remoteAhead
			if behind > 0 {
				if ahead > 0 {
					return fmt.Errorf("repo diverged from remote (ahead %d, behind %d); resolve manually", ahead, behind)
				}
				fmt.Printf("remote has %d commit(s) not present locally; discard local changes and pull? (y/n)\n", behind)
				var confirm string
				if _, err := fmt.Scanln(&confirm); err != nil {
					return fmt.Errorf("cannot continue sync: %w", err)
				}
				if strings.ToLower(confirm) != "y" {
					return fmt.Errorf("aborted sync")
				}
				dirty, err := repoHasChanges(repoDir)
				if err != nil {
					return err
				}
				if dirty {
					stashMsg := fmt.Sprintf("pda sync: %s", time.Now().UTC().Format(time.RFC3339))
					if err := runGit(repoDir, "stash", "push", "-u", "-m", stashMsg); err != nil {
						return err
					}
				}
				if err := pullRemote(repoDir, remoteInfo); err != nil {
					return err
				}
				return restoreAllSnapshots(store, repoDir)
			}
		}
	}

	if err := exportAllStores(store, repoDir); err != nil {
		return err
	}
	if err := runGit(repoDir, "add", storeDirName); err != nil {
		return err
	}
	changed, err := repoHasStagedChanges(repoDir)
	if err != nil {
		return err
	}
	madeCommit := false
	if !changed {
		if manual {
			fmt.Println("no changes to commit")
		}
	} else {
		msg := fmt.Sprintf("sync: %s", time.Now().UTC().Format(time.RFC3339))
		if err := runGit(repoDir, "commit", "-m", msg); err != nil {
			return err
		}
		madeCommit = true
	}
	if manual || config.Git.AutoPush {
		if remoteInfo.Ref == "" {
			if manual {
				fmt.Println("no remote configured; skipping push")
			}
		} else if madeCommit || ahead > 0 {
			return pushRemote(repoDir, remoteInfo)
		} else if manual {
			fmt.Println("nothing to push")
		}
	}
	return nil
}

func autoSync() error {
	if !config.Git.AutoCommit {
		return nil
	}
	return sync(false)
}
