package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/internal/socketpath"
	"github.com/urfave/cli/v2"
)

func remoteCommand() *cli.Command {
	return &cli.Command{
		Name:  "remote",
		Usage: "manage remote servers and push/pull objects and references",
		Subcommands: []*cli.Command{
			remoteAddCommand(),
			remoteRmCommand(),
			remoteLsCommand(),
			remotePushPullCommand("push"),
			remotePushPullCommand("pull"),
			remoteLsRefsCommand(),
		},
	}
}

// remoteAndName parses the [REMOTE] NAME argument pair; one argument means
// "the sole registered remote".
func remoteAndName(c *cli.Context, cmd string) (remote, name string, err error) {
	switch c.NArg() {
	case 1:
		return "", c.Args().Get(0), nil
	case 2:
		return c.Args().Get(0), c.Args().Get(1), nil
	default:
		return "", "", fmt.Errorf("%s requires [REMOTE] NAME arguments, got %d", cmd, c.NArg())
	}
}

func remoteAddCommand() *cli.Command {
	var socket string
	var yes bool
	return &cli.Command{
		Name:      "add",
		Usage:     "register a remote: fetch its key, confirm the fingerprint, persist",
		ArgsUsage: "NAME URL",
		Flags: []cli.Flag{
			socketFlag(&socket),
			&cli.BoolFlag{
				Name:        "yes",
				Usage:       "trust the server key without prompting (scripting)",
				Destination: &yes,
			},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() != 2 {
				return fmt.Errorf("remote add requires NAME URL arguments, got %d", c.NArg())
			}
			name, url := c.Args().Get(0), c.Args().Get(1)
			cl := client.New(socketpath.Resolve(socket))
			info, err := cl.RemotePreflight(c.Context, url)
			if err != nil {
				return err
			}
			fmt.Fprintf(c.App.Writer, "%s key fingerprint: %s (%s)\n", url, info.Fingerprint, info.KeyType)
			if !yes {
				fmt.Fprint(c.App.Writer, "Trust this key and register the remote? [y/N] ")
				reader := c.App.Reader
				if reader == nil {
					reader = os.Stdin
				}
				answer, err := bufio.NewReader(reader).ReadString('\n')
				if err != nil && err != io.EOF {
					return fmt.Errorf("reading confirmation: %w", err)
				}
				if a := strings.ToLower(strings.TrimSpace(answer)); a != "y" && a != "yes" {
					return fmt.Errorf("remote add aborted: server key not confirmed")
				}
			}
			if err := cl.RemoteAdd(c.Context, name, url, info.PublicKey); err != nil {
				return err
			}
			fmt.Fprintf(c.App.Writer, "remote %s added (%s)\n", name, url)
			return nil
		},
	}
}

func remoteRmCommand() *cli.Command {
	var socket string
	return &cli.Command{
		Name:      "rm",
		Usage:     "unregister a remote",
		ArgsUsage: "NAME",
		Flags:     []cli.Flag{socketFlag(&socket)},
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("remote rm requires exactly one NAME argument, got %d", c.NArg())
			}
			return client.New(socketpath.Resolve(socket)).RemoteRemove(c.Context, c.Args().First())
		},
	}
}

func remoteLsCommand() *cli.Command {
	var socket string
	return &cli.Command{
		Name:  "ls",
		Usage: "list registered remotes",
		Flags: []cli.Flag{socketFlag(&socket)},
		Action: func(c *cli.Context) error {
			infos, err := client.New(socketpath.Resolve(socket)).RemoteList(c.Context)
			if err != nil {
				return err
			}
			for _, info := range infos {
				fmt.Fprintf(c.App.Writer, "%s\t%s\t%s\n", info.Name, info.URL, info.Fingerprint)
			}
			return nil
		},
	}
}

// remotePushPullCommand builds the push / pull commands (same flags and arg
// shape; each performs the whole objects+reference transfer in one operation).
func remotePushPullCommand(name string) *cli.Command {
	var socket string
	var jobs int
	var batchBytes uint64
	usage := "push the local reference NAME and all its objects to the remote"
	if name == "pull" {
		usage = "pull the reference NAME and all its objects from the remote"
	}
	return &cli.Command{
		Name:      name,
		Usage:     usage,
		ArgsUsage: "[REMOTE] NAME",
		Flags: []cli.Flag{
			socketFlag(&socket),
			&cli.IntFlag{
				Name:        "jobs",
				Aliases:     []string{"j"},
				Value:       4,
				Usage:       "parallel transfer workers",
				Destination: &jobs,
			},
			&cli.Uint64Flag{
				Name:        "batch-bytes",
				Value:       60 << 20,
				Usage:       "per-batch payload target in bytes (kept under the server's 64 MiB body cap)",
				Destination: &batchBytes,
			},
		},
		Action: func(c *cli.Context) error {
			remote, refName, err := remoteAndName(c, "remote "+name)
			if err != nil {
				return err
			}
			cl := client.New(socketpath.Resolve(socket))
			progress := func(done, total int) {
				if total > 0 {
					fmt.Fprintf(os.Stderr, "\r%s: %d/%d objects", name, done, total)
				} else {
					fmt.Fprintf(os.Stderr, "\r%s: %d objects", name, done)
				}
			}
			defer fmt.Fprintln(os.Stderr)
			if name == "push" {
				stats, err := cl.RemotePush(c.Context, remote, refName, jobs, batchBytes, progress)
				if err != nil {
					return err
				}
				fmt.Fprintf(c.App.Writer, "pushed %s: %d objects (%d bytes), %d already present\n",
					refName, stats.ObjectsPushed, stats.BytesPushed, stats.ObjectsTotal-stats.ObjectsPushed)
				return nil
			}
			stats, rootKey, err := cl.RemotePull(c.Context, remote, refName, jobs, batchBytes, progress)
			if err != nil {
				return err
			}
			fmt.Fprintf(c.App.Writer, "pulled %s: %d objects (%d bytes), root %s\n",
				refName, stats.ObjectsFetched, stats.BytesFetched, rootKey)
			return nil
		},
	}
}

func remoteLsRefsCommand() *cli.Command {
	var socket string
	return &cli.Command{
		Name:      "ls-refs",
		Usage:     "list references on the remote",
		ArgsUsage: "[REMOTE]",
		Flags:     []cli.Flag{socketFlag(&socket)},
		Action: func(c *cli.Context) error {
			if c.NArg() > 1 {
				return fmt.Errorf("remote ls-refs takes at most one REMOTE argument, got %d", c.NArg())
			}
			infos, err := client.New(socketpath.Resolve(socket)).RemoteLsRefs(c.Context, c.Args().First())
			if err != nil {
				return err
			}
			for _, info := range infos {
				fmt.Fprintf(c.App.Writer, "%s\t%s\t%s\t%s\n", info.Name, info.Key, info.User, info.CreatedAt)
			}
			return nil
		},
	}
}
