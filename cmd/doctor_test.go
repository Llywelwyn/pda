package cmd

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestDoctorCleanEnv(t *testing.T) {
	dataDir := t.TempDir()
	configDir := t.TempDir()
	t.Setenv("PDA_DATA", dataDir)
	t.Setenv("PDA_CONFIG", configDir)
	saved := configErr
	configErr = nil
	t.Cleanup(func() { configErr = saved })

	var buf bytes.Buffer
	hasError := runDoctor(&buf)
	out := buf.String()

	if hasError {
		t.Errorf("expected no errors, got hasError=true\noutput:\n%s", out)
	}
	for _, want := range []string{
		version,
		"Using default configuration",
		"No identity file found",
		"Git not initialised",
		"No stores found",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in output, got:\n%s", want, out)
		}
	}
}

func TestDoctorWithStores(t *testing.T) {
	dataDir := t.TempDir()
	configDir := t.TempDir()
	t.Setenv("PDA_DATA", dataDir)
	t.Setenv("PDA_CONFIG", configDir)
	saved := configErr
	configErr = nil
	t.Cleanup(func() { configErr = saved })

	content := "{\"key\":\"foo\",\"value\":\"bar\",\"encoding\":\"text\"}\n" +
		"{\"key\":\"baz\",\"value\":\"qux\",\"encoding\":\"text\"}\n"
	if err := os.WriteFile(filepath.Join(dataDir, "test.ndjson"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	hasError := runDoctor(&buf)
	out := buf.String()

	if hasError {
		t.Errorf("expected no errors, got hasError=true\noutput:\n%s", out)
	}
	if !strings.Contains(out, "1 store(s), 2 key(s), 0 secret(s)") {
		t.Errorf("expected store summary in output, got:\n%s", out)
	}
}

func TestDoctorIdentityPermissions(t *testing.T) {
	dataDir := t.TempDir()
	configDir := t.TempDir()
	t.Setenv("PDA_DATA", dataDir)
	t.Setenv("PDA_CONFIG", configDir)

	idPath := filepath.Join(dataDir, "identity.txt")
	if err := os.WriteFile(idPath, []byte("placeholder\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	runDoctor(&buf)
	out := buf.String()

	if !strings.Contains(out, "Identity:") {
		t.Errorf("expected 'Identity:' in output, got:\n%s", out)
	}
	if !strings.Contains(out, "should be 0600") {
		t.Errorf("expected permissions warning in output, got:\n%s", out)
	}
}

func TestDoctorUndecodedKeys(t *testing.T) {
	dataDir := t.TempDir()
	configDir := t.TempDir()
	t.Setenv("PDA_DATA", dataDir)
	t.Setenv("PDA_CONFIG", configDir)

	// Write a config with an unknown key.
	cfgContent := "[store]\nno_such_key = true\n"
	if err := os.WriteFile(filepath.Join(configDir, "config.toml"), []byte(cfgContent), 0o644); err != nil {
		t.Fatal(err)
	}

	savedCfg, savedUndecoded, savedErr := config, configUndecodedKeys, configErr
	config, configUndecodedKeys, _, configErr = loadConfig()
	t.Cleanup(func() {
		config, configUndecodedKeys, configErr = savedCfg, savedUndecoded, savedErr
	})

	var buf bytes.Buffer
	runDoctor(&buf)
	out := buf.String()

	if !strings.Contains(out, "Unrecognised config key") {
		t.Errorf("expected undecoded key warning, got:\n%s", out)
	}
	if !strings.Contains(out, "store.no_such_key") {
		t.Errorf("expected 'store.no_such_key' in output, got:\n%s", out)
	}
}

func TestDoctorGitInitialised(t *testing.T) {
	dataDir := t.TempDir()
	configDir := t.TempDir()
	t.Setenv("PDA_DATA", dataDir)
	t.Setenv("PDA_CONFIG", configDir)

	cmd := exec.Command("git", "init")
	cmd.Dir = dataDir
	if err := cmd.Run(); err != nil {
		t.Skipf("git not available: %v", err)
	}
	cmd = exec.Command("git", "commit", "--allow-empty", "-m", "init")
	cmd.Dir = dataDir
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=test",
		"GIT_AUTHOR_EMAIL=test@test",
		"GIT_COMMITTER_NAME=test",
		"GIT_COMMITTER_EMAIL=test@test",
	)
	if err := cmd.Run(); err != nil {
		t.Fatalf("git commit: %v", err)
	}

	var buf bytes.Buffer
	runDoctor(&buf)
	out := buf.String()

	if !strings.Contains(out, "Git initialised on") {
		t.Errorf("expected 'Git initialised on' in output, got:\n%s", out)
	}
	if !strings.Contains(out, "No git remote configured") {
		t.Errorf("expected 'No git remote configured' in output, got:\n%s", out)
	}
}
