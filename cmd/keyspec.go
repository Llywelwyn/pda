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
// When defaults is true, a missing DB defaults to "default".
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
	}

	key := strings.ToLower(rawKey)
	db := strings.ToLower(rawDB)
	if db == "" && defaults {
		db = "default"
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
	if k.DB == "" || k.DB == "default" {
		return k.Key
	}
	return fmt.Sprintf("%s@%s", k.Key, k.DB)
}
