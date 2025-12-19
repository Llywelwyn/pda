package cmd

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

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

var vcsGitignoreCmd = &cobra.Command{
	Use:          "gitignore",
	Short:        "generates a suitable .gitignore file",
	SilenceUsage: true,
	Args:         cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		repoDir, err := ensureVCSInitialized()
		if err != nil {
			return err
		}

		return writeGitignore(repoDir, rewrite)
	},
}

var vcsSnapshotCmd = &cobra.Command{
	Use:          "snapshot",
	Short:        "commit a snapshot into the vcs",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		repoDir, err := ensureVCSInitialized()
		if err != nil {
			return err
		}

		store := &Store{}
		stores, err := store.AllStores()
		if err != nil {
			return err
		}

		for _, db := range stores {
			if err := snapshotDB(store, repoDir, db); err != nil {
				return fmt.Errorf("snapshot %q: %w", db, err)
			}
		}

		if err := runGit(repoDir, "add", "snapshots"); err != nil {
			return err
		}

		message := fmt.Sprintf("snapshot: %s", time.Now().UTC().Format(time.RFC3339))
		if err := runGit(repoDir, "commit", "-m", message); err != nil {
			fmt.Println(err.Error())
			return nil
		}

		return nil
	},
}

var vcsLogCmd = &cobra.Command{
	Use:          "log",
	Short:        "show git log for pda snapshots",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {

		repoDir, err := ensureVCSInitialized()
		if err != nil {
			return err
		}
		return runGit(repoDir, "log", "--oneline", "--graph", "--decorate")
	},
}

var vcsPullCmd = &cobra.Command{
	Use:          "pull",
	Short:        "pull snapshots from remote and restore into local store",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		repoDir, err := ensureVCSInitialized()
		if err != nil {
			return err
		}

		clean, err := cmd.Flags().GetBool("clean")
		if err != nil {
			return err
		}

		hasUpstream, err := repoHasUpstream(repoDir)
		if err != nil {
			return err
		}
		if hasUpstream {
			if err := runGit(repoDir, "pull"); err != nil {
				return err
			}
		}

		store := &Store{}
		if clean {
			fmt.Printf("this will remove all existing stores before restoring from version control. continue? (y/n)\n")
			var confirm string
			if _, err := fmt.Scanln(&confirm); err != nil {
				return fmt.Errorf("cannot clean stores: %w", err)
			}
			if strings.ToLower(confirm) != "y" {
				return fmt.Errorf("pull aborted; stores not removed")
			}
			if err := wipeAllStores(store); err != nil {
				return err
			}
		}
		if err := restoreAllSnapshots(store, repoDir); err != nil {
			return err
		}
		return nil
	},
}

var vcsPushCmd = &cobra.Command{
	Use:          "push",
	Short:        "push local snapshots to remote",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		repoDir, err := ensureVCSInitialized()
		if err != nil {
			return err
		}
		hasUpstream, err := repoHasUpstream(repoDir)
		if err != nil {
			return err
		}
		if !hasUpstream {
			hasOrigin, err := repoHasRemote(repoDir, "origin")
			if err != nil {
				return err
			}
			if !hasOrigin {
				return fmt.Errorf("no upstream configured; set a remote before pushing")
			}
			branch, err := currentBranch(repoDir)
			if err != nil {
				return err
			}
			if branch == "" {
				branch = "main"
			}
			fmt.Printf("running: git push -u origin %s\n", branch)
			return runGit(repoDir, "push", "-u", "origin", branch)
		}
		return runGit(repoDir, "push")
	},
}

var (
	rewrite bool = false
)

func init() {
	vcsInitCmd.Flags().Bool("clean", false, "Remove existing VCS directory before initialising")
	vcsCmd.AddCommand(vcsInitCmd)
	vcsGitignoreCmd.Flags().BoolVarP(&rewrite, "rewrite", "r", false, "Rewrite existing .gitignore, if present")
	vcsCmd.AddCommand(vcsGitignoreCmd)
	vcsCmd.AddCommand(vcsSnapshotCmd)
	vcsCmd.AddCommand(vcsLogCmd)
	vcsPullCmd.Flags().Bool("clean", false, "Remove all existing stores before restoring snapshots")
	vcsCmd.AddCommand(vcsPullCmd)
	vcsCmd.AddCommand(vcsPushCmd)
	rootCmd.AddCommand(vcsCmd)
}

func vcsInit(cmd *cobra.Command, args []string) error {
	repoDir, err := vcsRepoRoot()
	if err != nil {
		return err
	}

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

	return vcsGitignoreCmd.RunE(cmd, args)
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

func writeGitignore(repoDir string, rewrite bool) error {
	path := filepath.Join(repoDir, ".gitignore")
	if _, err := os.Stat(path); os.IsNotExist(err) || rewrite {
		content := strings.Join([]string{
			"# generated by pda",
			"*",
			"!/",
			"!.gitignore",
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
	if err := os.MkdirAll(repoDir, 0o750); err != nil {
		return err
	}
	target := filepath.Join(repoDir, fmt.Sprintf("%s.ndjson", db))
	f, err := os.Create(target)
	if err != nil {
		return err
	}
	defer f.Close()

	trans := TransactionArgs{
		key:      "@" + db,
		readonly: true,
		sync:     true,
		transact: func(tx *badger.Txn, k []byte) error {
			it := tx.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				key := item.KeyCopy(nil)
				meta := item.UserMeta()
				isSecret := meta&metaSecret != 0
				expiresAt := item.ExpiresAt()
				if err := item.Value(func(v []byte) error {
					entry := dumpEntry{
						Key:    string(key),
						Secret: isSecret,
					}
					if expiresAt > 0 {
						ts := int64(expiresAt)
						entry.ExpiresAt = &ts
					}
					if utf8.Valid(v) {
						entry.Encoding = "text"
						entry.Value = string(v)
					} else {
						encodeBase64(&entry, v)
					}
					payload, err := json.Marshal(entry)
					if err != nil {
						return err
					}
					_, err = fmt.Fprintln(f, string(payload))
					return err
				}); err != nil {
					return err
				}
			}
			return nil
		},
	}

	if err := store.Transaction(trans); err != nil {
		return err
	}

	return f.Sync()
}

func runGit(dir string, args ...string) error {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
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
	entries, err := os.ReadDir(repoDir)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("no repo directory found")
		}
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) != ".ndjson" {
			continue
		}
		dbName := strings.TrimSuffix(e.Name(), ".ndjson")
		if err := restoreSnapshot(store, filepath.Join(repoDir, e.Name()), dbName); err != nil {
			return fmt.Errorf("restore %q: %w", dbName, err)
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

func autoCommit(store *Store, dbs []string, message string) error {
	if !config.Git.AutoCommit {
		return nil
	}

	repoDir, err := ensureVCSInitialized()
	if err != nil {
		return err
	}

	unique := make(map[string]struct{})
	for _, db := range dbs {
		if db == "" {
			db = config.Store.DefaultStoreName
		}
		unique[db] = struct{}{}
	}

	for db := range unique {
		if err := snapshotOrRemoveDB(store, repoDir, db); err != nil {
			return err
		}
	}

	if err := runGit(repoDir, "add", strings.Join(dbs, " ")); err != nil {
		return err
	}

	return runGit(repoDir, "commit", "--allow-empty", "-m", message)
}

func snapshotOrRemoveDB(store *Store, repoDir, db string) error {
	_, err := store.FindStore(db)
	var nf errNotFound
	if errors.As(err, &nf) {
		snapPath := filepath.Join(repoDir, "snapshots", fmt.Sprintf("%s.ndjson", db))
		if rmErr := os.Remove(snapPath); rmErr != nil && !os.IsNotExist(rmErr) {
			return rmErr
		}
		return nil
	}
	if err != nil {
		return err
	}
	return snapshotDB(store, repoDir, db)
}
