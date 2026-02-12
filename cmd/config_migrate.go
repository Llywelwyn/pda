package cmd

import "strings"

type deprecation struct {
	Old string // e.g. "list.list_all_stores"
	New string // e.g. "list.always_show_all_stores"
}

type migration struct {
	Old      string // key that was removed
	New      string // key that holds the value
	Conflict bool   // both old and new were present; new key wins
}

var deprecations = []deprecation{
	{"list.list_all_stores", "list.always_show_all_stores"},
}

func migrateRawConfig(raw map[string]any) []migration {
	var migrations []migration
	for _, dep := range deprecations {
		oldParts := strings.Split(dep.Old, ".")
		newParts := strings.Split(dep.New, ".")

		_, ok := nestedGet(raw, oldParts)
		if !ok {
			continue
		}
		m := migration{Old: dep.Old, New: dep.New}
		if _, exists := nestedGet(raw, newParts); exists {
			m.Conflict = true
		} else {
			nestedSet(raw, newParts, nestedMustGet(raw, oldParts))
		}
		nestedDelete(raw, oldParts)
		migrations = append(migrations, m)
	}
	return migrations
}

func nestedMustGet(m map[string]any, parts []string) any {
	v, _ := nestedGet(m, parts)
	return v
}

func nestedGet(m map[string]any, parts []string) (any, bool) {
	for i, p := range parts {
		v, ok := m[p]
		if !ok {
			return nil, false
		}
		if i == len(parts)-1 {
			return v, true
		}
		sub, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		m = sub
	}
	return nil, false
}

func nestedSet(m map[string]any, parts []string, val any) {
	for i, p := range parts {
		if i == len(parts)-1 {
			m[p] = val
			return
		}
		sub, ok := m[p].(map[string]any)
		if !ok {
			sub = make(map[string]any)
			m[p] = sub
		}
		m = sub
	}
}

func nestedDelete(m map[string]any, parts []string) {
	for i, p := range parts {
		if i == len(parts)-1 {
			delete(m, p)
			return
		}
		sub, ok := m[p].(map[string]any)
		if !ok {
			return
		}
		m = sub
	}
}
