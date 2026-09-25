package cli_test

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/garm-ai/sink/internal/cli"
)

func runnable(use string) *cobra.Command {
	return &cobra.Command{Use: use, RunE: func(*cobra.Command, []string) error { return nil }}
}

func TestRequireSubcommandRefusesAndNamesTheOptions(t *testing.T) {
	// Real subcommands have a RunE; cobra's IsAvailableCommand treats a
	// command with nothing to run as unavailable, so the stubs need one.
	parent := &cobra.Command{Use: "drain"}
	parent.AddCommand(runnable("ledger"), runnable("audit"))
	cli.RequireSubcommand(parent)

	if parent.RunE == nil {
		t.Fatal("no RunE: the parent would print help and exit 0")
	}
	err := parent.RunE(parent, nil)
	if err == nil {
		t.Fatal("a parent with no subcommand succeeded")
	}
	for _, want := range []string{"audit", "ledger"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not offer %q: %v", want, err)
		}
	}
	if err := parent.RunE(parent, []string{"ledge"}); err == nil {
		t.Fatal("a misspelled subcommand succeeded")
	} else if !strings.Contains(err.Error(), "ledge") {
		t.Errorf("the error does not quote what was typed: %v", err)
	}
}

// A hidden command is not an answer to "which subcommand did you mean".
func TestRequireSubcommandDoesNotAdvertiseHiddenCommands(t *testing.T) {
	parent := &cobra.Command{Use: "garm-sink"}
	parent.AddCommand(runnable("drain"))
	hidden := runnable("secret")
	hidden.Hidden = true
	parent.AddCommand(hidden)
	cli.RequireSubcommand(parent)

	err := parent.RunE(parent, nil)
	if err == nil {
		t.Fatal("a parent with no subcommand succeeded")
	}
	if !strings.Contains(err.Error(), "drain") {
		t.Errorf("the error does not offer the real subcommand: %v", err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Errorf("the error advertises a hidden command: %v", err)
	}
}
