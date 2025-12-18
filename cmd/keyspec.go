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
	"strings"
)

// KeySpec is a parsed key reference.
type KeySpec struct {
	Raw    string // Whole, unmodified user input
	RawKey string // Key segment
	RawDB  string // DB segment
	Key    string // Normalised Key
	DB     string // Normalised DB
}

// ParseKey parses "KEY[@DB]" into a normalized KeySpec.
// When defaults is true, a missing DB defaults to the configured default.
func ParseKey(raw string, defaults bool) (KeySpec, error) {
	parts := strings.Split(raw, "@")
	if len(parts) > 2 {
		return KeySpec{}, fmt.Errorf("bad key format, use KEY@DB")
	}

	rawKey := parts[0]
	rawDB := ""
	if len(parts) == 2 {
		rawDB = parts[1]
		if strings.TrimSpace(rawDB) == "" {
			return KeySpec{}, fmt.Errorf("bad key format, use KEY@DB")
		}
		if err := validateDBName(rawDB); err != nil {
			return KeySpec{}, err
		}
	}

	key := strings.ToLower(rawKey)
	db := strings.ToLower(rawDB)
	if db == "" && defaults {
		db = config.Store.DefaultStoreName
	}

	return KeySpec{
		Raw:    raw,
		RawKey: rawKey,
		RawDB:  rawDB,
		Key:    key,
		DB:     db,
	}, nil
}

// Full returns the whole normalized key reference.
func (k KeySpec) Full() string {
	if k.DB == "" {
		return k.Key
	}
	return fmt.Sprintf("%s@%s", k.Key, k.DB)
}

// Display returns the normalized key reference
// but omits the default database if none was set manually
func (k KeySpec) Display() string {
	if k.DB == "" || k.DB == config.Store.DefaultStoreName {
		return k.Key
	}
	return fmt.Sprintf("%s@%s", k.Key, k.DB)
}
