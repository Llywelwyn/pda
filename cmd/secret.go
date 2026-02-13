package cmd

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"filippo.io/age"
	gap "github.com/muesli/go-app-paths"
)

// identityPath returns the path to the age identity file,
// respecting PDA_DATA the same way Store.path() does.
func identityPath() (string, error) {
	if override := os.Getenv("PDA_DATA"); override != "" {
		return filepath.Join(override, "identity.txt"), nil
	}
	scope := gap.NewScope(gap.User, "pda")
	dir, err := scope.DataPath("")
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

// recipientsPath returns the path to the additional recipients file,
// respecting PDA_DATA the same way identityPath does.
func recipientsPath() (string, error) {
	if override := os.Getenv("PDA_DATA"); override != "" {
		return filepath.Join(override, "recipients.txt"), nil
	}
	scope := gap.NewScope(gap.User, "pda")
	dir, err := scope.DataPath("")
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "recipients.txt"), nil
}

// loadRecipients loads additional age recipients from disk.
// Returns (nil, nil) if the recipients file does not exist.
func loadRecipients() ([]*age.X25519Recipient, error) {
	path, err := recipientsPath()
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var recipients []*age.X25519Recipient
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		r, err := age.ParseX25519Recipient(line)
		if err != nil {
			return nil, fmt.Errorf("parse recipient %q: %w", line, err)
		}
		recipients = append(recipients, r)
	}
	return recipients, scanner.Err()
}

// saveRecipients writes the recipients file. If the list is empty,
// the file is deleted.
func saveRecipients(recipients []*age.X25519Recipient) error {
	path, err := recipientsPath()
	if err != nil {
		return err
	}
	if len(recipients) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	var buf bytes.Buffer
	for _, r := range recipients {
		fmt.Fprintln(&buf, r.String())
	}
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

// allRecipients combines the identity's own recipient with any additional
// recipients from the recipients file into a single []age.Recipient slice.
// Returns nil if identity is nil and no recipients file exists.
func allRecipients(identity *age.X25519Identity) ([]age.Recipient, error) {
	extra, err := loadRecipients()
	if err != nil {
		return nil, err
	}
	if identity == nil && len(extra) == 0 {
		return nil, nil
	}
	var recipients []age.Recipient
	if identity != nil {
		recipients = append(recipients, identity.Recipient())
	}
	for _, r := range extra {
		recipients = append(recipients, r)
	}
	return recipients, nil
}

// encrypt encrypts plaintext for the given recipients using age.
func encrypt(plaintext []byte, recipients ...age.Recipient) ([]byte, error) {
	var buf bytes.Buffer
	w, err := age.Encrypt(&buf, recipients...)
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

// reencryptAllStores decrypts all secrets across all stores with the
// given identity, then re-encrypts them for the new recipient list.
// Returns the count of re-encrypted secrets.
func reencryptAllStores(identity *age.X25519Identity, recipients []age.Recipient) (int, error) {
	store := &Store{}
	storeNames, err := store.AllStores()
	if err != nil {
		return 0, err
	}

	count := 0
	for _, name := range storeNames {
		p, err := store.storePath(name)
		if err != nil {
			return 0, err
		}
		entries, err := readStoreFile(p, identity)
		if err != nil {
			return 0, err
		}
		hasSecrets := false
		for _, e := range entries {
			if e.Secret {
				if e.Locked {
					return 0, fmt.Errorf("cannot re-encrypt: secret '%s@%s' is locked (identity cannot decrypt it)", e.Key, name)
				}
				hasSecrets = true
			}
		}
		if !hasSecrets {
			continue
		}
		if err := writeStoreFile(p, entries, recipients); err != nil {
			return 0, err
		}
		for _, e := range entries {
			if e.Secret {
				spec := KeySpec{Key: e.Key, DB: name}
				okf("re-encrypted %s", spec.Display())
				count++
			}
		}
	}
	return count, nil
}

// decrypt decrypts age ciphertext with the given identity.
func decrypt(ciphertext []byte, identity *age.X25519Identity) ([]byte, error) {
	r, err := age.Decrypt(bytes.NewReader(ciphertext), identity)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
}
