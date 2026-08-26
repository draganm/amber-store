package main

import (
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/urfave/cli/v2"
	"golang.org/x/term"
)

func browseCommand() *cli.Command {
	var socket string
	var maxView int64
	return &cli.Command{
		Name:      "browse",
		Usage:     "interactively browse a tree: navigate dirs, view files (text/hex/json), export; accepts KEY[/PATH] or ref:NAME[@PATH], or no argument to pick from a searchable reference list",
		ArgsUsage: "[KEY[/PATH] | ref:NAME[@PATH]]",
		Flags: []cli.Flag{
			socketFlag(&socket),
			&cli.Int64Flag{
				Name:        "max-view-bytes",
				Usage:       "cap on bytes fetched into the file viewer",
				Value:       10 << 20,
				Destination: &maxView,
			},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() > 1 {
				return fmt.Errorf("browse takes at most one KEY[/PATH] argument, got %d", c.NArg())
			}
			if !term.IsTerminal(int(os.Stdout.Fd())) {
				return fmt.Errorf("browse requires a terminal")
			}
			cl, err := daemonClient(socket)
			if err != nil {
				return err
			}
			cwd, err := os.Getwd()
			if err != nil {
				return err
			}

			var m browseModel
			if c.NArg() == 0 {
				// No SPEC: open the searchable reference picker.
				m = newRefPickerModel(c.Context, cl, cwd, maxView)
			} else {
				k, path, err := resolveSpec(c.Context, cl, c.Args().First())
				if err != nil {
					return err
				}
				m = newBrowseModel(c.Context, cl, cwd, maxView, k, c.Args().First())
				if path != "" {
					// resolveSpec returns the reference/key root plus a sub-path;
					// navigation starts at the root, so a deep initial PATH is
					// only noted, not auto-opened (out of scope per the design).
					m.status = fmt.Sprintf("note: initial subpath %q not auto-opened; navigate from the root", path)
				}
			}
			p := tea.NewProgram(m, tea.WithAltScreen())
			_, err = p.Run()
			return err
		},
	}
}
