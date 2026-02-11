package cmd

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var doctorCmd = &cobra.Command{
	Use:          "doctor",
	Short:        "Check environment health",
	RunE:         doctor,
	SilenceUsage: true,
}

func init() {
	rootCmd.AddCommand(doctorCmd)
}

func doctor(cmd *cobra.Command, args []string) error {
	if runDoctor(os.Stdout) {
		os.Exit(1)
	}
	return nil
}

func runDoctor(w io.Writer) bool {
	tty := false
	if f, ok := w.(*os.File); ok {
		tty = term.IsTerminal(int(f.Fd()))
	}
	hasError := false

	emit := func(level, msg string) {
		var code string
		switch level {
		case "ok":
			code = "32"
		case "WARN":
			code = "33"
		case "FAIL":
			code = "31"
			hasError = true
		}
		fmt.Fprintf(w, "%s  %s\n", keyword(code, level, tty), msg)
	}

	tree := func(items []string) {
		for i, item := range items {
			connector := "├── "
			if i == len(items)-1 {
				connector = "└── "
			}
			fmt.Fprintf(w, "      %s%s\n", connector, item)
		}
	}

	// 1. Version + platform
	emit("ok", fmt.Sprintf("%s (%s/%s)", version, runtime.GOOS, runtime.GOARCH))

	// 2. OS detail
	switch runtime.GOOS {
	case "linux":
		if out, err := exec.Command("uname", "-r").Output(); err == nil {
			emit("ok", fmt.Sprintf("OS: Linux %s", strings.TrimSpace(string(out))))
		}
	case "darwin":
		if out, err := exec.Command("sw_vers", "-productVersion").Output(); err == nil {
			emit("ok", fmt.Sprintf("OS: macOS %s", strings.TrimSpace(string(out))))
		}
	}

	// 3. Go version
	emit("ok", fmt.Sprintf("Go: %s", runtime.Version()))

	// 4. Git dependency
	gitAvailable := false
	if out, err := exec.Command("git", "--version").Output(); err == nil {
		gitVer := strings.TrimSpace(string(out))
		if after, ok := strings.CutPrefix(gitVer, "git version "); ok {
			gitVer = after
		}
		emit("ok", fmt.Sprintf("Git: %s", gitVer))
		gitAvailable = true
	} else {
		emit("WARN", "git not found on PATH")
	}

	// 5. Shell
	if shell := os.Getenv("SHELL"); shell != "" {
		emit("ok", fmt.Sprintf("Shell: %s", shell))
	} else {
		emit("WARN", "SHELL not set")
	}

	// 6. Config directory and file
	cfgPath, err := configPath()
	if err != nil {
		emit("FAIL", fmt.Sprintf("Cannot determine config path: %v", err))
	} else {
		cfgDir := filepath.Dir(cfgPath)
		envSuffix := ""
		if os.Getenv("PDA_CONFIG") != "" {
			envSuffix = " (PDA_CONFIG)"
		}

		var issues []string
		if _, statErr := os.Stat(cfgPath); statErr != nil && !os.IsNotExist(statErr) {
			issues = append(issues, fmt.Sprintf("Config file unreadable: %s", cfgPath))
		}
		if unexpectedFiles(cfgDir, map[string]bool{
			"config.toml":  true,
			"identity.txt": true,
		}) {
			issues = append(issues, "Unexpected file(s) in directory")
		}

		if len(issues) > 0 {
			emit("FAIL", fmt.Sprintf("Config: %s%s", cfgDir, envSuffix))
			tree(issues)
		} else {
			emit("ok", fmt.Sprintf("Config: %s%s", cfgDir, envSuffix))
		}

		if _, statErr := os.Stat(cfgPath); os.IsNotExist(statErr) {
			emit("ok", "Using default configuration")
		}
	}

	// 7. Non-default config values
	if diffs := configDiffs(); len(diffs) > 0 {
		emit("ok", "Non-default config:")
		tree(diffs)
	}

	// 8. Data directory
	store := &Store{}
	dataDir, err := store.path()
	if err != nil {
		emit("FAIL", fmt.Sprintf("Data directory inaccessible: %v", err))
	} else {
		envSuffix := ""
		if os.Getenv("PDA_DATA") != "" {
			envSuffix = " (PDA_DATA)"
		}

		if unexpectedDataFiles(dataDir) {
			emit("FAIL", fmt.Sprintf("Data: %s%s", dataDir, envSuffix))
			tree([]string{"Unexpected file(s) in directory"})
		} else {
			emit("ok", fmt.Sprintf("Data: %s%s", dataDir, envSuffix))
		}
	}

	// 9. Identity file
	idPath, err := identityPath()
	if err != nil {
		emit("FAIL", fmt.Sprintf("Cannot determine identity path: %v", err))
	} else if _, err := os.Stat(idPath); os.IsNotExist(err) {
		emit("WARN", "No identity file found")
	} else if err != nil {
		emit("FAIL", fmt.Sprintf("Cannot access identity file: %v", err))
	} else {
		info, _ := os.Stat(idPath)
		emit("ok", fmt.Sprintf("Identity: %s", idPath))
		if perm := info.Mode().Perm(); perm != 0o600 {
			emit("WARN", fmt.Sprintf("Identity file permissions %04o (should be 0600)", perm))
		}
		if _, loadErr := loadIdentity(); loadErr != nil {
			emit("WARN", fmt.Sprintf("Identity file invalid: %v", loadErr))
		}
	}

	// 10. Git initialised
	gitInitialised := false
	if dataDir != "" {
		gitDir := filepath.Join(dataDir, ".git")
		if _, err := os.Stat(gitDir); os.IsNotExist(err) {
			emit("WARN", "Git not initialised")
		} else if err != nil {
			emit("FAIL", fmt.Sprintf("Cannot check git status: %v", err))
		} else {
			gitInitialised = true
			branch, _ := currentBranch(dataDir)
			if branch == "" {
				branch = "unknown"
			}
			emit("ok", fmt.Sprintf("Git initialised on %s", branch))
		}
	}

	// 11. Git uncommitted changes (only if git initialised)
	if gitInitialised && gitAvailable {
		ucCmd := exec.Command("git", "status", "--porcelain")
		ucCmd.Dir = dataDir
		if out, err := ucCmd.Output(); err == nil {
			if trimmed := strings.TrimSpace(string(out)); trimmed != "" {
				count := len(strings.Split(trimmed, "\n"))
				emit("WARN", fmt.Sprintf("Git %d file(s) with uncommitted changes", count))
			}
		}
	}

	// 12. Git remote (only if git initialised)
	hasOrigin := false
	if gitInitialised {
		var err error
		hasOrigin, err = repoHasRemote(dataDir, "origin")
		if err != nil {
			emit("FAIL", fmt.Sprintf("Cannot check git remote: %v", err))
		} else if hasOrigin {
			emit("ok", "Git remote configured")
		} else {
			emit("WARN", "No git remote configured")
		}
	}

	// 13. Git sync (only if git initialised AND remote exists)
	if gitInitialised && hasOrigin && gitAvailable {
		info, err := repoRemoteInfo(dataDir)
		if err != nil || info.Ref == "" {
			emit("WARN", "Git sync status unknown")
		} else {
			ahead, behind, err := repoAheadBehind(dataDir, info.Ref)
			if err != nil {
				emit("WARN", "Git sync status unknown")
			} else if ahead == 0 && behind == 0 {
				emit("ok", "Git in sync with remote")
			} else {
				var parts []string
				if ahead > 0 {
					parts = append(parts, fmt.Sprintf("%d ahead", ahead))
				}
				if behind > 0 {
					parts = append(parts, fmt.Sprintf("%d behind", behind))
				}
				emit("WARN", fmt.Sprintf("Git %s remote", strings.Join(parts, ", ")))
			}
		}
	}

	// 14. Stores summary
	stores, err := store.AllStores()
	if err != nil {
		emit("FAIL", fmt.Sprintf("Cannot list stores: %v", err))
	} else if len(stores) == 0 {
		emit("WARN", "No stores found")
	} else {
		var totalKeys, totalSecrets, parseErrors int
		var totalSize int64
		for _, name := range stores {
			p, pErr := store.storePath(name)
			if pErr != nil {
				parseErrors++
				continue
			}
			if fi, sErr := os.Stat(p); sErr == nil {
				totalSize += fi.Size()
			}
			entries, rErr := readStoreFile(p, nil)
			if rErr != nil {
				parseErrors++
				continue
			}
			totalKeys += len(entries)
			for _, e := range entries {
				if e.Secret {
					totalSecrets++
				}
			}
		}
		if parseErrors > 0 {
			emit("FAIL", fmt.Sprintf("%d store(s), %d with parse errors", len(stores), parseErrors))
		} else {
			emit("ok", fmt.Sprintf("%d store(s), %d key(s), %d secret(s), %s total",
				len(stores), totalKeys, totalSecrets, formatSize(int(totalSize))))
		}
	}

	if hasError {
		emit("FAIL", "1 or more issues found")
	} else {
		emit("ok", "No issues found")
	}

	return hasError
}

func configDiffs() []string {
	def := defaultConfig()
	var diffs []string
	if config.DisplayAsciiArt != def.DisplayAsciiArt {
		diffs = append(diffs, fmt.Sprintf("display_ascii_art: %v", config.DisplayAsciiArt))
	}
	if config.Key.AlwaysPromptDelete != def.Key.AlwaysPromptDelete {
		diffs = append(diffs, fmt.Sprintf("key.always_prompt_delete: %v", config.Key.AlwaysPromptDelete))
	}
	if config.Key.AlwaysPromptOverwrite != def.Key.AlwaysPromptOverwrite {
		diffs = append(diffs, fmt.Sprintf("key.always_prompt_overwrite: %v", config.Key.AlwaysPromptOverwrite))
	}
	if config.Store.DefaultStoreName != def.Store.DefaultStoreName {
		diffs = append(diffs, fmt.Sprintf("store.default_store_name: %s", config.Store.DefaultStoreName))
	}
	if config.Store.AlwaysPromptDelete != def.Store.AlwaysPromptDelete {
		diffs = append(diffs, fmt.Sprintf("store.always_prompt_delete: %v", config.Store.AlwaysPromptDelete))
	}
	if config.Store.AlwaysPromptOverwrite != def.Store.AlwaysPromptOverwrite {
		diffs = append(diffs, fmt.Sprintf("store.always_prompt_overwrite: %v", config.Store.AlwaysPromptOverwrite))
	}
	if config.Git.AutoFetch != def.Git.AutoFetch {
		diffs = append(diffs, fmt.Sprintf("git.auto_fetch: %v", config.Git.AutoFetch))
	}
	if config.Git.AutoCommit != def.Git.AutoCommit {
		diffs = append(diffs, fmt.Sprintf("git.auto_commit: %v", config.Git.AutoCommit))
	}
	if config.Git.AutoPush != def.Git.AutoPush {
		diffs = append(diffs, fmt.Sprintf("git.auto_push: %v", config.Git.AutoPush))
	}
	return diffs
}

func unexpectedFiles(dir string, allowed map[string]bool) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !allowed[e.Name()] {
			return true
		}
	}
	return false
}

func unexpectedDataFiles(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() && name == ".git" {
			continue
		}
		if !e.IsDir() && (name == ".gitignore" || filepath.Ext(name) == ".ndjson") {
			continue
		}
		return true
	}
	return false
}
