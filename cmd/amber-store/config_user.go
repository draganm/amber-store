package main

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/draganm/amber-store/sshsign"
	"github.com/draganm/amber-store/userconfig"
	"github.com/draganm/amber-store/reference"
	"github.com/urfave/cli/v2"
)

func configUserCommand() *cli.Command {
	var signingKey string
	return &cli.Command{
		Name:      "config-user",
		Usage:     "record the user name written into references created by this machine",
		ArgsUsage: "NAME",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "signing-key",
				Usage:       "SSH key that signs created references: a private-key file, or a .pub whose key the ssh-agent holds; pass an empty value to clear",
				Destination: &signingKey,
			},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("config-user requires exactly one NAME argument, got %d", c.NArg())
			}
			name := c.Args().First()
			if err := reference.ValidateUser(name); err != nil {
				return fmt.Errorf("invalid user name: %w", err)
			}
			// Start from the existing config so an omitted --signing-key
			// preserves the stored key; a missing config is a fresh start.
			cfg, err := userconfig.Load()
			if err != nil && !errors.Is(err, userconfig.ErrNotConfigured) {
				return err
			}
			cfg.User = name
			if c.IsSet("signing-key") {
				if signingKey == "" {
					cfg.SigningKey = ""
				} else {
					abs, err := filepath.Abs(signingKey)
					if err != nil {
						return err
					}
					if err := sshsign.CheckKeyFile(abs); err != nil {
						return err
					}
					cfg.SigningKey = abs
				}
			}
			if err := userconfig.Save(cfg); err != nil {
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
