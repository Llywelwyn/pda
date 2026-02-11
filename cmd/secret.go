package cmd

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"filippo.io/age"
	gap "github.com/muesli/go-app-paths"
)

// identityPath returns the path to the age identity file,
// respecting PDA_CONFIG the same way configPath() does.
func identityPath() (string, error) {
	if override := os.Getenv("PDA_CONFIG"); override != "" {
		return filepath.Join(override, "identity.txt"), nil
	}
	scope := gap.NewScope(gap.User, "pda")
	dir, err := scope.ConfigPath("")
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "identity.txt"), nil
}

// loadIdentity loads the age identity from disk.
// Returns (nil, nil) if the identity file does not exist.
func loadIdentity() (*age.X25519Identity, error) {
	path, err := identityPath()
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	identity, err := age.ParseX25519Identity(string(bytes.TrimSpace(data)))
	if err != nil {
		return nil, fmt.Errorf("parse identity %s: %w", path, err)
	}
	return identity, nil
}

// ensureIdentity loads an existing identity or generates a new one.
// On first creation prints an ok message with the file path.
func ensureIdentity() (*age.X25519Identity, error) {
	id, err := loadIdentity()
	if err != nil {
		return nil, err
	}
	if id != nil {
		return id, nil
	}

	id, err = age.GenerateX25519Identity()
	if err != nil {
		return nil, fmt.Errorf("generate identity: %w", err)
	}

	path, err := identityPath()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, []byte(id.String()+"\n"), 0o600); err != nil {
		return nil, err
	}

	okf("created identity at %s", path)
	return id, nil
}

// encrypt encrypts plaintext for the given recipient using age.
func encrypt(plaintext []byte, recipient *age.X25519Recipient) ([]byte, error) {
	var buf bytes.Buffer
	w, err := age.Encrypt(&buf, recipient)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(plaintext); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decrypt decrypts age ciphertext with the given identity.
func decrypt(ciphertext []byte, identity *age.X25519Identity) ([]byte, error) {
	r, err := age.Decrypt(bytes.NewReader(ciphertext), identity)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
}
