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

package main

import (
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"filippo.io/age"
	cmdtest "github.com/google/go-cmdtest"
)

var update = flag.Bool("update", false, "update test files with results")

func TestMain(t *testing.T) {
	ts, err := cmdtest.Read("testdata")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	bin := filepath.Join(t.TempDir(), "pda")
	if err := exec.Command("go", "build", "-o", bin, ".").Run(); err != nil {
		t.Fatal(err)
	}
	ts.Commands["pda"] = cmdtest.Program(bin)

	// Each .ct file gets its own isolated data and config directories
	// inside its ROOTDIR, so tests cannot leak state to each other.
	ts.Setup = func(rootDir string) error {
		dataDir := filepath.Join(rootDir, "data")
		configDir := filepath.Join(rootDir, "config")
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			return err
		}
		if err := os.MkdirAll(configDir, 0o755); err != nil {
			return err
		}
		os.Setenv("PDA_DATA", dataDir)
		os.Setenv("PDA_CONFIG", configDir)

		// Pre-create an age identity so encryption tests don't print
		// a creation message with a non-deterministic path.
		id, err := age.GenerateX25519Identity()
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dataDir, "identity.txt"), []byte(id.String()+"\n"), 0o600)
	}

	ts.Run(t, *update)
}
