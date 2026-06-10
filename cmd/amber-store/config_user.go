package main

import (
	"fmt"

	"github.com/draganm/amber-store/internal/userconfig"
	"github.com/urfave/cli/v2"
)

func configUserCommand() *cli.Command {
	return &cli.Command{
		Name:      "config-user",
		Usage:     "record the user name written into references created by this machine",
		ArgsUsage: "NAME",
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("config-user requires exactly one NAME argument, got %d", c.NArg())
			}
			if err := userconfig.Save(userconfig.Config{User: c.Args().First()}); err != nil {
				return err
			}
			p, err := userconfig.Path()
			if err != nil {
				return err
			}
			fmt.Fprintf(c.App.Writer, "user config written to %s\n", p)
			return nil
		},
	}
}
