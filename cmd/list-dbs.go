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
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// listStoresCmd represents the list-stores command
var listStoresCmd = &cobra.Command{
	Use:          "list-stores",
	Short:        "List all stores",
	Aliases:      []string{"lss"},
	Args:         cobra.NoArgs,
	RunE:         listStores,
	SilenceUsage: true,
}

func listStores(cmd *cobra.Command, args []string) error {
	store := &Store{}
	dbs, err := store.AllStores()
	if err != nil {
		return fmt.Errorf("cannot list stores: %v", err)
	}

	short, err := cmd.Flags().GetBool("short")
	if err != nil {
		return fmt.Errorf("cannot list stores: %v", err)
	}

	if short {
		for _, db := range dbs {
			fmt.Println("@" + db)
		}
		return nil
	}

	type storeInfo struct {
		name  string
		keys  int
		size  string
	}

	rows := make([]storeInfo, 0, len(dbs))
	nameW, keysW, sizeW := len("Store"), len("Keys"), len("Size")

	for _, db := range dbs {
		p, err := store.storePath(db)
		if err != nil {
			return fmt.Errorf("cannot list stores: %v", err)
		}
		fi, err := os.Stat(p)
		if err != nil {
			return fmt.Errorf("cannot list stores: %v", err)
		}
		entries, err := readStoreFile(p, nil)
		if err != nil {
			return fmt.Errorf("cannot list stores: %v", err)
		}
		name := "@" + db
		keysStr := fmt.Sprintf("%d", len(entries))
		sizeStr := formatSize(int(fi.Size()))
		if len(name) > nameW {
			nameW = len(name)
		}
		if len(keysStr) > keysW {
			keysW = len(keysStr)
		}
		if len(sizeStr) > sizeW {
			sizeW = len(sizeStr)
		}
		rows = append(rows, storeInfo{name: name, keys: len(entries), size: sizeStr})
	}

	underline := func(s string) string {
		if stdoutIsTerminal() {
			return "\033[4m" + s + "\033[0m"
		}
		return s
	}
	noHeader, _ := cmd.Flags().GetBool("no-header")
	if !noHeader {
		fmt.Printf("%*s%s %*s%s %s\n",
			keysW-len("Keys"), "", underline("Keys"),
			sizeW-len("Size"), "", underline("Size"),
			underline("Store"))
	}
	for _, r := range rows {
		fmt.Printf("%*d %*s %s\n", keysW, r.keys, sizeW, r.size, r.name)
	}
	return nil
}

func init() {
	listStoresCmd.Flags().Bool("short", false, "only print store names")
	listStoresCmd.Flags().Bool("no-header", false, "suppress the header row")
	rootCmd.AddCommand(listStoresCmd)
}
