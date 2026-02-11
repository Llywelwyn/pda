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
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"filippo.io/age"
)

// Entry is the in-memory representation of a stored key-value pair.
type Entry struct {
	Key       string
	Value     []byte
	ExpiresAt uint64 // Unix timestamp; 0 = never expires
	Secret    bool   // encrypted on disk
	Locked    bool   // secret but no identity available to decrypt
}

// jsonEntry is the NDJSON on-disk format.
type jsonEntry struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Encoding  string `json:"encoding,omitempty"`
	ExpiresAt *int64 `json:"expires_at,omitempty"`
}

// readStoreFile reads all non-expired entries from an NDJSON file.
// Returns empty slice (not error) if file does not exist.
// If identity is nil, secret entries are returned as locked.
func readStoreFile(path string, identity *age.X25519Identity) ([]Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	now := uint64(time.Now().Unix())
	var entries []Entry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var je jsonEntry
		if err := json.Unmarshal(line, &je); err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNo, err)
		}
		entry, err := decodeJsonEntry(je, identity)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNo, err)
		}
		// Skip expired entries
		if entry.ExpiresAt > 0 && entry.ExpiresAt <= now {
			continue
		}
		entries = append(entries, entry)
	}
	return entries, scanner.Err()
}

// writeStoreFile atomically writes entries to an NDJSON file, sorted by key.
// Expired entries are excluded. Empty entry list writes an empty file.
// If recipient is nil, secret entries are written as-is (locked passthrough).
func writeStoreFile(path string, entries []Entry, recipient *age.X25519Recipient) error {
	// Sort by key for deterministic output
	slices.SortFunc(entries, func(a, b Entry) int {
		return strings.Compare(a.Key, b.Key)
	})

	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer func() {
		f.Close()
		os.Remove(tmp) // clean up on failure; no-op after successful rename
	}()

	w := bufio.NewWriter(f)
	now := uint64(time.Now().Unix())
	for _, e := range entries {
		if e.ExpiresAt > 0 && e.ExpiresAt <= now {
			continue
		}
		je, err := encodeJsonEntry(e, recipient)
		if err != nil {
			return fmt.Errorf("key '%s': %w", e.Key, err)
		}
		data, err := json.Marshal(je)
		if err != nil {
			return fmt.Errorf("key '%s': %w", e.Key, err)
		}
		w.Write(data)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func decodeJsonEntry(je jsonEntry, identity *age.X25519Identity) (Entry, error) {
	var expiresAt uint64
	if je.ExpiresAt != nil {
		expiresAt = uint64(*je.ExpiresAt)
	}

	if je.Encoding == "secret" {
		ciphertext, err := base64.StdEncoding.DecodeString(je.Value)
		if err != nil {
			return Entry{}, fmt.Errorf("decode secret for '%s': %w", je.Key, err)
		}
		if identity == nil {
			return Entry{Key: je.Key, Value: ciphertext, ExpiresAt: expiresAt, Secret: true, Locked: true}, nil
		}
		plaintext, err := decrypt(ciphertext, identity)
		if err != nil {
			warnf("cannot decrypt '%s': %v", je.Key, err)
			return Entry{Key: je.Key, Value: ciphertext, ExpiresAt: expiresAt, Secret: true, Locked: true}, nil
		}
		return Entry{Key: je.Key, Value: plaintext, ExpiresAt: expiresAt, Secret: true}, nil
	}

	var value []byte
	switch je.Encoding {
	case "", "text":
		value = []byte(je.Value)
	case "base64":
		var err error
		value, err = base64.StdEncoding.DecodeString(je.Value)
		if err != nil {
			return Entry{}, fmt.Errorf("decode base64 for '%s': %w", je.Key, err)
		}
	default:
		return Entry{}, fmt.Errorf("unsupported encoding '%s' for '%s'", je.Encoding, je.Key)
	}
	return Entry{Key: je.Key, Value: value, ExpiresAt: expiresAt}, nil
}

func encodeJsonEntry(e Entry, recipient *age.X25519Recipient) (jsonEntry, error) {
	je := jsonEntry{Key: e.Key}
	if e.ExpiresAt > 0 {
		ts := int64(e.ExpiresAt)
		je.ExpiresAt = &ts
	}

	if e.Secret && e.Locked {
		// Passthrough: Value holds raw ciphertext, re-encode as-is
		je.Value = base64.StdEncoding.EncodeToString(e.Value)
		je.Encoding = "secret"
		return je, nil
	}
	if e.Secret {
		if recipient == nil {
			return je, fmt.Errorf("no recipient available to encrypt")
		}
		ciphertext, err := encrypt(e.Value, recipient)
		if err != nil {
			return je, fmt.Errorf("encrypt: %w", err)
		}
		je.Value = base64.StdEncoding.EncodeToString(ciphertext)
		je.Encoding = "secret"
		return je, nil
	}

	if utf8.Valid(e.Value) {
		je.Value = string(e.Value)
		je.Encoding = "text"
	} else {
		je.Value = base64.StdEncoding.EncodeToString(e.Value)
		je.Encoding = "base64"
	}
	return je, nil
}

// findEntry returns the index of the entry with the given key, or -1.
func findEntry(entries []Entry, key string) int {
	for i, e := range entries {
		if e.Key == key {
			return i
		}
	}
	return -1
}
