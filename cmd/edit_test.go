package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"filippo.io/age"
)

func setupEditTest(t *testing.T) (*age.X25519Identity, string) {
	t.Helper()
	dataDir := t.TempDir()
	configDir := t.TempDir()
	t.Setenv("PDA_DATA", dataDir)
	t.Setenv("PDA_CONFIG", configDir)

	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "identity.txt"), []byte(id.String()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Reset global config to defaults with test env vars active
	config, _, _, _ = loadConfig()

	return id, dataDir
}

func TestEditCreatesNewKey(t *testing.T) {
	id, _ := setupEditTest(t)

	// Create editor script that writes "hello"
	script := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho hello > \"$1\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EDITOR", script)

	// Run edit for a new key
	rootCmd.SetArgs([]string{"edit", "newkey@testedit"})
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("edit failed: %v", err)
	}

	// Verify key was created
	store := &Store{}
	p, _ := store.storePath("testedit")
	entries, _ := readStoreFile(p, id)
	idx := findEntry(entries, "newkey")
	if idx < 0 {
		t.Fatal("key was not created")
	}
	if string(entries[idx].Value) != "hello" {
		t.Fatalf("unexpected value: %q", entries[idx].Value)
	}
}

func TestEditModifiesExistingKey(t *testing.T) {
	id, _ := setupEditTest(t)

	// Create an existing key
	store := &Store{}
	p, _ := store.storePath("testedit2")
	entries := []Entry{{Key: "existing", Value: []byte("original")}}
	if err := writeStoreFile(p, entries, nil); err != nil {
		t.Fatal(err)
	}

	// Editor that replaces content
	script := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho modified > \"$1\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EDITOR", script)

	rootCmd.SetArgs([]string{"edit", "existing@testedit2"})
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("edit failed: %v", err)
	}

	// Verify
	entries, _ = readStoreFile(p, id)
	idx := findEntry(entries, "existing")
	if idx < 0 {
		t.Fatal("key disappeared")
	}
	if string(entries[idx].Value) != "modified" {
		t.Fatalf("unexpected value: %q", entries[idx].Value)
	}
}

func TestEditNoChangeSkipsWrite(t *testing.T) {
	setupEditTest(t)

	store := &Store{}
	p, _ := store.storePath("testedit3")
	entries := []Entry{{Key: "unchanged", Value: []byte("same")}}
	if err := writeStoreFile(p, entries, nil); err != nil {
		t.Fatal(err)
	}

	// "true" command does nothing — file stays the same
	t.Setenv("EDITOR", "true")

	rootCmd.SetArgs([]string{"edit", "unchanged@testedit3"})
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("edit failed: %v", err)
	}
	// Should print "no changes" — we just verify it didn't error
}
