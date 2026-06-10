package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/internal/socketpath"
	"github.com/draganm/amber-store/internal/sshsign"
	"github.com/draganm/amber-store/internal/userconfig"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"github.com/urfave/cli/v2"
)

// socketFlag is the shared --socket flag; dst receives the value.
func socketFlag(dst *string) cli.Flag {
	return &cli.StringFlag{
		Name:        "socket",
		Usage:       "daemon unix socket (default: $AMBER_STORE_SOCKET or a per-user path)",
		Destination: dst,
	}
}

func refCommand() *cli.Command {
	return &cli.Command{
		Name:  "ref",
		Usage: "manage references (named pointers to keys)",
		Subcommands: []*cli.Command{
			refCreateCommand(),
			refLsCommand(),
			refShowCommand(),
			refRmCommand(),
		},
	}
}

func refCreateCommand() *cli.Command {
	var socket string
	return &cli.Command{
		Name:      "create",
		Usage:     "point NAME at the existing key KEY (creates or overwrites)",
		ArgsUsage: "NAME KEY",
		Flags:     []cli.Flag{socketFlag(&socket)},
		Action: func(c *cli.Context) error {
			if c.NArg() != 2 {
				return fmt.Errorf("ref create requires NAME KEY arguments, got %d", c.NArg())
			}
			name := c.Args().Get(0)
			if err := reference.ValidateName(name); err != nil {
				return err
			}
			// Load the config before parsing the key so a missing config is
			// reported with its config-user hint regardless of the KEY value.
			ucfg, err := userconfig.Load()
			if err != nil {
				return err
			}
			k, err := parseHexKey(c.Args().Get(1))
			if err != nil {
				return err
			}
			rec := reference.Reference{
				Name:      name,
				Key:       k[:],
				User:      ucfg.User,
				CreatedAt: time.Now().UnixNano(),
			}
			if ucfg.SigningKey != "" {
				payload, err := rec.SignaturePayload()
				if err != nil {
					return fmt.Errorf("encoding reference for signing: %w", err)
				}
				// Fail closed: a configured key never silently yields an
				// unsigned reference.
				sig, err := sshsign.Sign(ucfg.SigningKey, payload, sshsign.TTYPrompt)
				if err != nil {
					return fmt.Errorf("signing reference %q: %w", name, err)
				}
				rec.Signature = sig
			}
			return client.New(socketpath.Resolve(socket)).PutRef(c.Context, rec)
		},
	}
}

func refLsCommand() *cli.Command {
	var socket string
	return &cli.Command{
		Name:  "ls",
		Usage: "list all references: name, key, user, creation date",
		Flags: []cli.Flag{socketFlag(&socket)},
		Action: func(c *cli.Context) error {
			if c.NArg() != 0 {
				return fmt.Errorf("ref ls takes no arguments, got %d", c.NArg())
			}
			infos, err := client.New(socketpath.Resolve(socket)).ListRefs(c.Context)
			if err != nil {
				return err
			}
			for _, info := range infos {
				// Tab-separated: names and users may contain spaces, but control
				// characters (incl. tab) are rejected by validation, so tabs are
				// unambiguous column separators.
				if _, err := fmt.Fprintf(c.App.Writer, "%s\t%s\t%s\t%s\n",
					info.Name, info.Key, info.User, info.CreatedAt); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

// refShowOutput is the JSON document `ref show` prints.
type refShowOutput struct {
	Name      string `json:"name"`
	Key       string `json:"key"`
	User      string `json:"user"`
	CreatedAt string `json:"created_at"`
	Signature string `json:"signature,omitempty"` // hex
}

func refShowCommand() *cli.Command {
	var socket string
	return &cli.Command{
		Name:      "show",
		Usage:     "print one reference's full record as JSON",
		ArgsUsage: "NAME",
		Flags:     []cli.Flag{socketFlag(&socket)},
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("ref show requires exactly one NAME argument, got %d", c.NArg())
			}
			rec, err := client.New(socketpath.Resolve(socket)).GetRef(c.Context, c.Args().First())
			if err != nil {
				return err
			}
			k, err := key.Parse(rec.Key)
			if err != nil {
				return err
			}
			out := refShowOutput{
				Name:      rec.Name,
				Key:       k.String(),
				User:      rec.User,
				CreatedAt: time.Unix(0, rec.CreatedAt).UTC().Format(time.RFC3339Nano),
				Signature: hex.EncodeToString(rec.Signature),
			}
			enc := json.NewEncoder(c.App.Writer)
			enc.SetIndent("", "  ")
			return enc.Encode(out)
		},
	}
}

func refRmCommand() *cli.Command {
	var socket string
	return &cli.Command{
		Name:      "rm",
		Usage:     "delete a reference (the pointed-to objects stay in the store)",
		ArgsUsage: "NAME",
		Flags:     []cli.Flag{socketFlag(&socket)},
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("ref rm requires exactly one NAME argument, got %d", c.NArg())
			}
			return client.New(socketpath.Resolve(socket)).DeleteRef(c.Context, c.Args().First())
		},
	}
}
