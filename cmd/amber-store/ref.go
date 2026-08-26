package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/socketpath"
	"github.com/draganm/amber-store/sshsign"
	"github.com/draganm/amber-store/userconfig"
	"github.com/urfave/cli/v2"
	"golang.org/x/crypto/ssh"
)

// socketFlag is the shared --socket flag; dst receives the value.
func socketFlag(dst *string) cli.Flag {
	return &cli.StringFlag{
		Name:        "socket",
		Usage:       "daemon unix socket (default: $AMBER_STORE_SOCKET or a per-user path)",
		Destination: dst,
	}
}

// daemonClient resolves the socket (flag, env, or the verified per-user
// default) and returns a client for it.
func daemonClient(socket string) (*client.Client, error) {
	sock, err := socketpath.Resolve(socket)
	if err != nil {
		return nil, err
	}
	return client.New(sock), nil
}

func refCommand() *cli.Command {
	return &cli.Command{
		Name:  "ref",
		Usage: "manage references (named pointers to keys)",
		Subcommands: []*cli.Command{
			refCreateCommand(),
			refLsCommand(),
			refShowCommand(),
			refVerifySignatureCommand(),
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
				// Fail closed: a configured key never silently yields an
				// unsigned reference.
				if err := signReference(&rec, ucfg.SigningKey); err != nil {
					return fmt.Errorf("signing reference %q: %w", name, err)
				}
			}
			cl, err := daemonClient(socket)
			if err != nil {
				return err
			}
			return cl.PutRef(c.Context, rec)
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
			cl, err := daemonClient(socket)
			if err != nil {
				return err
			}
			infos, err := cl.ListRefs(c.Context)
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
	Signature string `json:"signature,omitempty"`  // hex
	PublicKey string `json:"public_key,omitempty"` // authorized_keys form, hex if unparseable
}

// formatPublicKey renders a stored SSH wire-format public key in
// authorized_keys form, falling back to hex for unparseable bytes.
func formatPublicKey(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	pub, err := ssh.ParsePublicKey(b)
	if err != nil {
		return hex.EncodeToString(b)
	}
	return strings.TrimSpace(string(ssh.MarshalAuthorizedKey(pub)))
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
			cl, err := daemonClient(socket)
			if err != nil {
				return err
			}
			rec, err := cl.GetRef(c.Context, c.Args().First())
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
				PublicKey: formatPublicKey(rec.PublicKey),
			}
			enc := json.NewEncoder(c.App.Writer)
			enc.SetIndent("", "  ")
			return enc.Encode(out)
		},
	}
}

func refVerifySignatureCommand() *cli.Command {
	var socket string
	return &cli.Command{
		Name:      "verify-signature",
		Usage:     "check a reference's stored signature: a valid SSHSIG over the record by its recorded public key (integrity only, no trust model)",
		ArgsUsage: "NAME",
		Flags:     []cli.Flag{socketFlag(&socket)},
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("ref verify-signature requires exactly one NAME argument, got %d", c.NArg())
			}
			name := c.Args().First()
			cl, err := daemonClient(socket)
			if err != nil {
				return err
			}
			rec, err := cl.GetRef(c.Context, name)
			if err != nil {
				return err
			}
			if len(rec.Signature) == 0 {
				return fmt.Errorf("reference %q is not signed", name)
			}
			if len(rec.PublicKey) == 0 {
				return fmt.Errorf("reference %q carries no public key", name)
			}
			payload, err := rec.SignaturePayload()
			if err != nil {
				return err
			}
			pub, err := sshsign.Verify(payload, rec.Signature, rec.PublicKey)
			if err != nil {
				return fmt.Errorf("reference %q: invalid signature: %w", name, err)
			}
			fmt.Fprintf(c.App.Writer, "good signature on %q by %q with %s key %s\n",
				name, rec.User, pub.Type(), ssh.FingerprintSHA256(pub))
			return nil
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
			cl, err := daemonClient(socket)
			if err != nil {
				return err
			}
			return cl.DeleteRef(c.Context, c.Args().First())
		},
	}
}
