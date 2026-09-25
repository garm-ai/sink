// Package cli holds the cobra conventions the garm binaries share.
//
// Only the parent-command guard came across from the monorepo. The argv
// pre-scan that lives beside it there exists to soften a migration from
// standard-library flags, which this binary never had.
package cli

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

// RequireSubcommand makes a parent command FAIL rather than print help and
// exit 0 when it is invoked without a subcommand, or with one it does not
// have.
//
// Cobra's default is to print help and exit 0. That turns a container
// entrypoint templated as `garm-sink drain $STREAM` into a pod that starts,
// reports healthy, and forwards nothing the day $STREAM is unset — a job that
// looks alive while the lake stops filling.
func RequireSubcommand(cmd *cobra.Command) *cobra.Command {
	cmd.RunE = func(c *cobra.Command, args []string) error {
		var names []string
		for _, sub := range c.Commands() {
			// IsAvailableCommand drops hidden entries and cobra's own help
			// and completion commands, which are not answers to "which
			// subcommand did you mean".
			if sub.IsAvailableCommand() {
				names = append(names, sub.Name())
			}
		}
		sort.Strings(names)
		if len(args) > 0 {
			return fmt.Errorf("unknown %s subcommand %q; expected one of: %s",
				c.Name(), args[0], strings.Join(names, ", "))
		}
		return fmt.Errorf("%s needs a subcommand: %s",
			c.CommandPath(), strings.Join(names, ", "))
	}
	return cmd
}
