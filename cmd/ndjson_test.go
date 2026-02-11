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
	"path/filepath"
	"testing"
	"time"
)

func TestReadWriteRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.ndjson")

	entries := []Entry{
		{Key: "alpha", Value: []byte("hello")},
		{Key: "beta", Value: []byte("world"), ExpiresAt: uint64(time.Now().Add(time.Hour).Unix())},
		{Key: "gamma", Value: []byte{0xff, 0xfe}}, // binary
	}

	if err := writeStoreFile(path, entries); err != nil {
		t.Fatal(err)
	}

	got, err := readStoreFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != len(entries) {
		t.Fatalf("got %d entries, want %d", len(got), len(entries))
	}
	for i := range entries {
		if got[i].Key != entries[i].Key {
			t.Errorf("entry %d: key = %q, want %q", i, got[i].Key, entries[i].Key)
		}
		if string(got[i].Value) != string(entries[i].Value) {
			t.Errorf("entry %d: value mismatch", i)
		}
	}
}

func TestReadStoreFileSkipsExpired(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.ndjson")

	entries := []Entry{
		{Key: "alive", Value: []byte("yes")},
		{Key: "dead", Value: []byte("no"), ExpiresAt: 1}, // expired long ago
	}

	if err := writeStoreFile(path, entries); err != nil {
		t.Fatal(err)
	}

	got, err := readStoreFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 1 || got[0].Key != "alive" {
		t.Fatalf("expected only 'alive', got %v", got)
	}
}

func TestReadStoreFileNotExist(t *testing.T) {
	got, err := readStoreFile("/nonexistent/path.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty, got %d entries", len(got))
	}
}

func TestWriteStoreFileSortsKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.ndjson")

	entries := []Entry{
		{Key: "charlie", Value: []byte("3")},
		{Key: "alpha", Value: []byte("1")},
		{Key: "bravo", Value: []byte("2")},
	}

	if err := writeStoreFile(path, entries); err != nil {
		t.Fatal(err)
	}

	got, err := readStoreFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if got[0].Key != "alpha" || got[1].Key != "bravo" || got[2].Key != "charlie" {
		t.Fatalf("entries not sorted: %v", got)
	}
}

func TestWriteStoreFileAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.ndjson")

	// Write initial data
	if err := writeStoreFile(path, []Entry{{Key: "a", Value: []byte("1")}}); err != nil {
		t.Fatal(err)
	}

	// Overwrite — should not leave .tmp files
	if err := writeStoreFile(path, []Entry{{Key: "b", Value: []byte("2")}}); err != nil {
		t.Fatal(err)
	}

	// Check no .tmp file remains
	matches, _ := filepath.Glob(filepath.Join(dir, "*.tmp"))
	if len(matches) > 0 {
		t.Fatalf("leftover tmp files: %v", matches)
	}
}
