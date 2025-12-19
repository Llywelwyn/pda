package cmd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	gap "github.com/muesli/go-app-paths"
	"github.com/spf13/cobra"
)

var vcsCmd = &cobra.Command{
	Use:   "vcs",
	Short: "Version control utilities",
}

var vcsInitCmd = &cobra.Command{
	Use:          "init [remote-url]",
	Short:        "Initialise or fetch a Git repo for version control",
	SilenceUsage: true,
	Args:         cobra.MaximumNArgs(1),
	RunE:         vcsInit,
}

var vcsSyncCmd = &cobra.Command{
	Use:          "sync",
	Short:        "export, commit, pull, restore, and push changes",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return sync(true)
	},
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
			return err
		}
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
		fmt.Println("no changes to commit")
	} else {
		msg := fmt.Sprintf("sync: %s", time.Now().UTC().Format(time.RFC3339))
		if err := runGit(repoDir, "commit", "-m", msg); err != nil {
			return err
		}
		madeCommit = true
	}
	if manual || config.Git.AutoPush {
		if remoteInfo.Ref != "" && (madeCommit || ahead > 0) {
			return pushRemote(repoDir, remoteInfo)
		}
		fmt.Println("no remote configured; skipping push")
	}
	return nil
}

const storeDirName = "stores"

func init() {
	vcsInitCmd.Flags().Bool("clean", false, "Remove existing VCS directory before initialising")
	vcsCmd.AddCommand(vcsInitCmd)
	vcsCmd.AddCommand(vcsSyncCmd)
	rootCmd.AddCommand(vcsCmd)
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
	if clean {
		entries, err := os.ReadDir(repoDir)
		if err == nil && len(entries) > 0 {
			fmt.Printf("remove existing VCS directory '%s'? (y/n)\n", repoDir)
			var confirm string
			if _, err := fmt.Scanln(&confirm); err != nil {
				return fmt.Errorf("cannot clean vcs dir: %w", err)
			}
			if strings.ToLower(confirm) != "y" {
				return fmt.Errorf("aborted cleaning vcs dir")
			}
		}
		if err := os.RemoveAll(repoDir); err != nil {
			return fmt.Errorf("cannot clean vcs dir: %w", err)
		}

		dbs, err := store.AllStores()
		if err == nil && len(dbs) > 0 {
			fmt.Printf("remove all existing stores? (y/n)\n")
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
		}
	}
	if err := os.MkdirAll(filepath.Join(repoDir), 0o750); err != nil {
		return err
	}

	gitDir := filepath.Join(repoDir, ".git")
	if _, err := os.Stat(gitDir); os.IsNotExist(err) {
		if len(args) == 1 {
			remote := args[0]
			fmt.Printf("running: git clone %s %s\n", remote, repoDir)
			if err := runGit("", "clone", remote, repoDir); err != nil {
				return err
			}
		} else {
			fmt.Printf("running: git init\n")
			if err := runGit(repoDir, "init"); err != nil {
				return err
			}
		}
	} else {
		fmt.Println("vcs already initialised; use --clean to reinitialise")
		return nil
	}

	if err := writeGitignore(repoDir); err != nil {
		return err
	}
	return restoreAllSnapshots(store, repoDir)
}

func vcsRepoRoot() (string, error) {
	scope := gap.NewVendorScope(gap.User, "pda", "vcs")
	dir, err := scope.DataPath("")
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	return dir, nil
}

func ensureVCSInitialized() (string, error) {
	repoDir, err := vcsRepoRoot()
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(filepath.Join(repoDir, ".git")); err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("vcs repository not initialised; run 'pda vcs init' first")
		}
		return "", err
	}
	return repoDir, nil
}

func writeGitignore(repoDir string) error {
	path := filepath.Join(repoDir, ".gitignore")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		content := strings.Join([]string{
			"# generated by pda",
			"*",
			"!/",
			"!.gitignore",
			"!" + storeDirName + "/",
			"!" + storeDirName + "/*",
			"",
		}, "\n")
		if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
			return err
		}

		if err := runGit(repoDir, "add", ".gitignore"); err != nil {
			return err
		}
		return runGit(repoDir, "commit", "--allow-empty", "-m", "generated gitignore")
	}
	fmt.Println("Existing .gitignore found.")
	return nil
}

func snapshotDB(store *Store, repoDir, db string) error {
	targetDir := filepath.Join(repoDir, storeDirName)
	if err := os.MkdirAll(targetDir, 0o750); err != nil {
		return err
	}
	target := filepath.Join(targetDir, fmt.Sprintf("%s.ndjson", db))
	f, err := os.Create(target)
	if err != nil {
		return err
	}
	defer f.Close()

	opts := DumpOptions{
		Encoding:      "auto",
		IncludeSecret: false,
	}
	if err := dumpDatabase(store, db, f, opts); err != nil {
		return err
	}

	return f.Sync()
}

// exportAllStores writes every Badger store to ndjson files under repoDir/stores
// and removes stale snapshot files for deleted databases.
func exportAllStores(store *Store, repoDir string) error {
	stores, err := store.AllStores()
	if err != nil {
		return err
	}

	targetDir := filepath.Join(repoDir, storeDirName)
	if err := os.MkdirAll(targetDir, 0o750); err != nil {
		return err
	}

	current := make(map[string]struct{})
	for _, db := range stores {
		current[db] = struct{}{}
		if err := snapshotDB(store, repoDir, db); err != nil {
			return fmt.Errorf("snapshot %q: %w", db, err)
		}
	}

	entries, err := os.ReadDir(targetDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".ndjson" {
			continue
		}
		dbName := strings.TrimSuffix(e.Name(), ".ndjson")
		if _, ok := current[dbName]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(targetDir, e.Name())); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}

func runGit(dir string, args ...string) error {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

type gitRemoteInfo struct {
	Ref         string
	HasUpstream bool
	Remote      string
	Branch      string
}

func repoRemoteInfo(dir string) (gitRemoteInfo, error) {
	hasUpstream, err := repoHasUpstream(dir)
	if err != nil {
		return gitRemoteInfo{}, err
	}
	if hasUpstream {
		return gitRemoteInfo{Ref: "@{u}", HasUpstream: true}, nil
	}
	hasOrigin, err := repoHasRemote(dir, "origin")
	if err != nil {
		return gitRemoteInfo{}, err
	}
	if !hasOrigin {
		return gitRemoteInfo{}, nil
	}
	branch, err := currentBranch(dir)
	if err != nil {
		return gitRemoteInfo{}, err
	}
	if branch == "" {
		branch = "main"
	}
	return gitRemoteInfo{
		Ref:    fmt.Sprintf("origin/%s", branch),
		Remote: "origin",
		Branch: branch,
	}, nil
}

func repoAheadBehind(dir, ref string) (int, int, error) {
	cmd := exec.Command("git", "rev-list", "--left-right", "--count", "HEAD..."+ref)
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		return 0, 0, err
	}
	fields := strings.Fields(string(out))
	if len(fields) != 2 {
		return 0, 0, fmt.Errorf("unexpected rev-list output: %q", strings.TrimSpace(string(out)))
	}
	ahead, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, 0, fmt.Errorf("parse ahead count: %w", err)
	}
	behind, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, 0, fmt.Errorf("parse behind count: %w", err)
	}
	return ahead, behind, nil
}

func repoHasChanges(dir string) (bool, error) {
	cmd := exec.Command("git", "status", "--porcelain")
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		return false, err
	}
	return len(bytes.TrimSpace(out)) > 0, nil
}

func repoHasStagedChanges(dir string) (bool, error) {
	cmd := exec.Command("git", "diff", "--cached", "--quiet")
	cmd.Dir = dir
	err := cmd.Run()
	if err == nil {
		return false, nil
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
		return true, nil
	}
	return false, err
}

func pullRemote(dir string, info gitRemoteInfo) error {
	if info.HasUpstream {
		return runGit(dir, "pull", "--ff-only")
	}
	if info.Remote != "" && info.Branch != "" {
		fmt.Printf("running: git pull --ff-only %s %s\n", info.Remote, info.Branch)
		return runGit(dir, "pull", "--ff-only", info.Remote, info.Branch)
	}
	fmt.Println("no remote configured; skipping pull")
	return nil
}

func pushRemote(dir string, info gitRemoteInfo) error {
	if info.HasUpstream {
		return runGit(dir, "push")
	}
	if info.Remote != "" && info.Branch != "" {
		fmt.Printf("running: git push -u %s %s\n", info.Remote, info.Branch)
		return runGit(dir, "push", "-u", info.Remote, info.Branch)
	}
	fmt.Println("no remote configured; skipping push")
	return nil
}

func repoHasUpstream(dir string) (bool, error) {
	cmd := exec.Command("git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}")
	cmd.Dir = dir
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	err := cmd.Run()
	if err == nil {
		return true, nil
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 0 {
		return false, nil
	}
	return false, err
}

func repoHasRemote(dir, name string) (bool, error) {
	cmd := exec.Command("git", "remote", "get-url", name)
	cmd.Dir = dir
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	err := cmd.Run()
	if err == nil {
		return true, nil
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 0 {
		return false, nil
	}
	return false, err
}

func currentBranch(dir string) (string, error) {
	cmd := exec.Command("git", "rev-parse", "--abbrev-ref", "HEAD")
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	branch := strings.TrimSpace(string(out))
	if branch == "HEAD" {
		return "", nil
	}
	return branch, nil
}

func restoreAllSnapshots(store *Store, repoDir string) error {
	targetDir := filepath.Join(repoDir, storeDirName)
	entries, err := os.ReadDir(targetDir)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("no repo directory found")
		}
		return err
	}
	snapshotDBs := make(map[string]struct{})

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) != ".ndjson" {
			continue
		}
		dbName := strings.TrimSuffix(e.Name(), ".ndjson")
		snapshotDBs[dbName] = struct{}{}

		dbPath, err := store.FindStore(dbName)
		if err == nil {
			_ = os.RemoveAll(dbPath)
		}

		if err := restoreSnapshot(store, filepath.Join(targetDir, e.Name()), dbName); err != nil {
			return fmt.Errorf("restore %q: %w", dbName, err)
		}
	}

	localDBs, err := store.AllStores()
	if err != nil {
		return err
	}
	for _, db := range localDBs {
		if _, ok := snapshotDBs[db]; ok {
			continue
		}
		dbPath, err := store.FindStore(db)
		if err != nil {
			return err
		}
		if err := os.RemoveAll(dbPath); err != nil {
			return fmt.Errorf("remove db '%s': %w", db, err)
		}
	}

	return nil
}

func wipeAllStores(store *Store) error {
	dbs, err := store.AllStores()
	if err != nil {
		return err
	}
	for _, db := range dbs {
		path, err := store.FindStore(db)
		if err != nil {
			return err
		}
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("remove db '%s': %w", db, err)
		}
	}
	return nil
}

func restoreSnapshot(store *Store, path string, dbName string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	db, err := store.open(dbName)
	if err != nil {
		return err
	}
	defer db.Close()

	decoder := json.NewDecoder(bufio.NewReader(f))
	wb := db.NewWriteBatch()
	defer wb.Cancel()

	entryNo := 0
	for {
		var entry dumpEntry
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("entry %d: %w", entryNo+1, err)
		}
		entryNo++
		if entry.Key == "" {
			return fmt.Errorf("entry %d: missing key", entryNo)
		}

		value, err := decodeEntryValue(entry)
		if err != nil {
			return fmt.Errorf("entry %d: %w", entryNo, err)
		}

		entryMeta := byte(0x0)
		if entry.Secret {
			entryMeta = metaSecret
		}

		writeEntry := badger.NewEntry([]byte(entry.Key), value).WithMeta(entryMeta)
		if entry.ExpiresAt != nil {
			if *entry.ExpiresAt < 0 {
				return fmt.Errorf("entry %d: expires_at must be >= 0", entryNo)
			}
			writeEntry.ExpiresAt = uint64(*entry.ExpiresAt)
		}

		if err := wb.SetEntry(writeEntry); err != nil {
			return fmt.Errorf("entry %d: %w", entryNo, err)
		}
	}

	if err := wb.Flush(); err != nil {
		return err
	}
	return nil
}

// hasMergeConflicts returns true if there are files with unresolved merge
// conflicts in the working tree.
func hasMergeConflicts(dir string) (bool, error) {
	cmd := exec.Command("git", "diff", "--name-only", "--diff-filter=U")
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		return false, err
	}
	return len(bytes.TrimSpace(out)) > 0, nil
}

func autoSync() error {
	if !config.Git.AutoCommit {
		return nil
	}
	return sync(false)
}
