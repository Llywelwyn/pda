package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"filippo.io/age"
)

func TestEncryptDecryptRoundtrip(t *testing.T) {
	id, err := generateTestIdentity(t)
	if err != nil {
		t.Fatal(err)
	}
	recipient := id.Recipient()

	tests := []struct {
		name      string
		plaintext []byte
	}{
		{"simple text", []byte("hello world")},
		{"empty", []byte("")},
		{"binary", []byte{0x00, 0xff, 0xfe, 0xfd}},
		{"large", make([]byte, 64*1024)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ciphertext, err := encrypt(tt.plaintext, recipient)
			if err != nil {
				t.Fatalf("encrypt: %v", err)
			}
			if len(ciphertext) == 0 && len(tt.plaintext) > 0 {
				t.Fatal("ciphertext is empty for non-empty plaintext")
			}
			got, err := decrypt(ciphertext, id)
			if err != nil {
				t.Fatalf("decrypt: %v", err)
			}
			if string(got) != string(tt.plaintext) {
				t.Errorf("roundtrip mismatch: got %q, want %q", got, tt.plaintext)
			}
		})
	}
}

func TestLoadIdentityMissing(t *testing.T) {
	t.Setenv("PDA_CONFIG", t.TempDir())
	id, err := loadIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if id != nil {
		t.Fatal("expected nil identity for missing file")
	}
}

func TestEnsureIdentityCreatesFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PDA_CONFIG", dir)

	id, err := ensureIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if id == nil {
		t.Fatal("expected non-nil identity")
	}

	path := filepath.Join(dir, "identity.txt")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("identity file not created: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("identity file permissions = %o, want 0600", perm)
	}

	// Second call should return same identity
	id2, err := ensureIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if id2.Recipient().String() != id.Recipient().String() {
		t.Error("second ensureIdentity returned different identity")
	}
}

func TestEnsureIdentityIdempotent(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PDA_CONFIG", dir)

	id1, err := ensureIdentity()
	if err != nil {
		t.Fatal(err)
	}
	id2, err := ensureIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if id1.String() != id2.String() {
		t.Error("ensureIdentity is not idempotent")
	}
}

func TestSecretEntryRoundtrip(t *testing.T) {
	id, err := generateTestIdentity(t)
	if err != nil {
		t.Fatal(err)
	}
	recipient := id.Recipient()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.ndjson")

	entries := []Entry{
		{Key: "plain", Value: []byte("hello")},
		{Key: "encrypted", Value: []byte("secret-value"), Secret: true},
	}

	if err := writeStoreFile(path, entries, recipient); err != nil {
		t.Fatal(err)
	}

	// Read with identity — should decrypt
	got, err := readStoreFile(path, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}

	plain := got[findEntry(got, "plain")]
	if string(plain.Value) != "hello" || plain.Secret || plain.Locked {
		t.Errorf("plain entry unexpected: %+v", plain)
	}

	secret := got[findEntry(got, "encrypted")]
	if string(secret.Value) != "secret-value" {
		t.Errorf("secret value = %q, want %q", secret.Value, "secret-value")
	}
	if !secret.Secret {
		t.Error("secret entry should have Secret=true")
	}
	if secret.Locked {
		t.Error("secret entry should not be locked when identity available")
	}
}

func TestSecretEntryLockedWithoutIdentity(t *testing.T) {
	id, err := generateTestIdentity(t)
	if err != nil {
		t.Fatal(err)
	}
	recipient := id.Recipient()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.ndjson")

	entries := []Entry{
		{Key: "encrypted", Value: []byte("secret-value"), Secret: true},
	}
	if err := writeStoreFile(path, entries, recipient); err != nil {
		t.Fatal(err)
	}

	// Read without identity — should be locked
	got, err := readStoreFile(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1", len(got))
	}
	if !got[0].Secret || !got[0].Locked {
		t.Errorf("expected Secret=true, Locked=true, got Secret=%v, Locked=%v", got[0].Secret, got[0].Locked)
	}
	if string(got[0].Value) == "secret-value" {
		t.Error("locked entry should not contain plaintext")
	}
}

func TestLockedPassthrough(t *testing.T) {
	id, err := generateTestIdentity(t)
	if err != nil {
		t.Fatal(err)
	}
	recipient := id.Recipient()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.ndjson")

	// Write with encryption
	entries := []Entry{
		{Key: "encrypted", Value: []byte("secret-value"), Secret: true},
	}
	if err := writeStoreFile(path, entries, recipient); err != nil {
		t.Fatal(err)
	}

	// Read without identity (locked)
	locked, err := readStoreFile(path, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Write back without identity (passthrough)
	if err := writeStoreFile(path, locked, nil); err != nil {
		t.Fatal(err)
	}

	// Read with identity — should still decrypt
	got, err := readStoreFile(path, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1", len(got))
	}
	if string(got[0].Value) != "secret-value" {
		t.Errorf("after passthrough: value = %q, want %q", got[0].Value, "secret-value")
	}
	if !got[0].Secret || got[0].Locked {
		t.Error("entry should be Secret=true, Locked=false after decryption")
	}
}

func generateTestIdentity(t *testing.T) (*age.X25519Identity, error) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("PDA_CONFIG", dir)
	return ensureIdentity()
}
