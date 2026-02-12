package cmd

import (
	"strings"
	"testing"
)

func TestRenderCommitMessage(t *testing.T) {
	t.Run("summary and time", func(t *testing.T) {
		msg := renderCommitMessage("{{ summary }} {{ time }}", "set foo")
		if !strings.HasPrefix(msg, "set foo ") {
			t.Errorf("expected prefix 'set foo ', got %q", msg)
		}
		parts := strings.SplitN(msg, " ", 3)
		if len(parts) < 3 || !strings.Contains(parts[2], "T") {
			t.Errorf("expected RFC3339 time, got %q", msg)
		}
	})

	t.Run("empty summary", func(t *testing.T) {
		msg := renderCommitMessage("{{ summary }} {{ time }}", "")
		if !strings.HasPrefix(msg, " ") {
			t.Errorf("expected leading space (empty summary), got %q", msg)
		}
	})

	t.Run("default function", func(t *testing.T) {
		msg := renderCommitMessage(`{{ default "sync" (summary) }}`, "")
		if msg != "sync" {
			t.Errorf("expected 'sync', got %q", msg)
		}
		msg = renderCommitMessage(`{{ default "sync" (summary) }}`, "set foo")
		if msg != "set foo" {
			t.Errorf("expected 'set foo', got %q", msg)
		}
	})

	t.Run("env function", func(t *testing.T) {
		t.Setenv("PDA_TEST_USER", "alice")
		msg := renderCommitMessage(`{{ env "PDA_TEST_USER" }}: {{ summary }}`, "set foo")
		if msg != "alice: set foo" {
			t.Errorf("expected 'alice: set foo', got %q", msg)
		}
	})

	t.Run("bad template returns raw", func(t *testing.T) {
		raw := "{{ bad template"
		msg := renderCommitMessage(raw, "test")
		if msg != raw {
			t.Errorf("expected raw %q, got %q", raw, msg)
		}
	})
}
