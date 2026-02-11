package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
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
	tty := stdoutIsTerminal()
	hasError := false

	emit := func(level, msg string) {
		var code string
		switch level {
		case "ok":
			code = "32"
		case "info":
			code = "34"
		case "FAIL":
			code = "31"
			hasError = true
		}
		fmt.Fprintf(os.Stdout, "%s  %s\n", keyword(code, level, tty), msg)
	}

	// Config
	cfgPath, err := configPath()
	if err != nil {
		emit("FAIL", fmt.Sprintf("Cannot determine config path: %v", err))
	} else if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
		emit("info", "Using default configuration")
	} else if err != nil {
		emit("FAIL", fmt.Sprintf("Cannot access config: %v", err))
	} else {
		emit("ok", "Configuration loaded")
	}

	// Data directory
	store := &Store{}
	dataDir, err := store.path()
	if err != nil {
		emit("FAIL", fmt.Sprintf("Data directory inaccessible: %v", err))
	} else {
		emit("ok", "Data directory accessible")
	}

	// Identity
	idPath, err := identityPath()
	if err != nil {
		emit("FAIL", fmt.Sprintf("Cannot determine identity path: %v", err))
	} else if _, err := os.Stat(idPath); os.IsNotExist(err) {
		emit("info", "No identity file. One will be created on first encrypt.")
	} else if err != nil {
		emit("FAIL", fmt.Sprintf("Cannot access identity file: %v", err))
	} else {
		emit("ok", "Identity file present")
	}

	// Git
	gitInitialised := false
	if dataDir != "" {
		gitDir := filepath.Join(dataDir, ".git")
		if _, err := os.Stat(gitDir); os.IsNotExist(err) {
			emit("info", "Git not initialised")
		} else if err != nil {
			emit("FAIL", fmt.Sprintf("Cannot check git status: %v", err))
		} else {
			gitInitialised = true
			emit("ok", "Git initialised")
		}
	}

	// Git remote (only when git is initialised)
	if gitInitialised {
		hasOrigin, err := repoHasRemote(dataDir, "origin")
		if err != nil {
			emit("FAIL", fmt.Sprintf("Cannot check git remote: %v", err))
		} else if hasOrigin {
			emit("ok", "Git remote configured")
		} else {
			emit("info", "No git remote configured")
		}
	}

	// Stores
	stores, err := store.AllStores()
	if err != nil {
		emit("FAIL", fmt.Sprintf("Cannot list stores: %v", err))
	} else if len(stores) == 0 {
		emit("info", "No stores")
	} else {
		var parseErrors int
		for _, name := range stores {
			p, pErr := store.storePath(name)
			if pErr != nil {
				parseErrors++
				continue
			}
			if _, rErr := readStoreFile(p, nil); rErr != nil {
				parseErrors++
			}
		}
		if parseErrors > 0 {
			emit("FAIL", fmt.Sprintf("%d store(s), %d with errors", len(stores), parseErrors))
		} else {
			emit("ok", fmt.Sprintf("%d store(s)", len(stores)))
		}
	}

	if hasError {
		os.Exit(1)
	}
	return nil
}
