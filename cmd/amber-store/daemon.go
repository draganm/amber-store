package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/draganm/amber-store/daemon"
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/internal/socketpath"
	"github.com/urfave/cli/v2"
)

type daemonConfig struct {
	store           string
	socket          string
	inlineThreshold int
	sync            bool
}

func daemonCommand() *cli.Command {
	cfg := &daemonConfig{}
	return &cli.Command{
		Name:  "daemon",
		Usage: "run the store-owning daemon, serving clients over a unix socket",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "store",
				Aliases:     []string{"s"},
				Usage:       "diskstore directory (created if missing)",
				Required:    true,
				Destination: &cfg.store,
			},
			&cli.StringFlag{
				Name:        "socket",
				Usage:       "unix socket path (default: $AMBER_STORE_SOCKET or a per-user path)",
				Destination: &cfg.socket,
			},
			&cli.IntFlag{
				Name:        "inline-threshold",
				Value:       diskstore.DefaultInlineThreshold,
				Usage:       "objects larger than this many bytes are stored as external blob files",
				Destination: &cfg.inlineThreshold,
			},
			&cli.BoolFlag{
				Name:        "sync",
				Value:       true,
				Usage:       "fsync writes for crash durability",
				Destination: &cfg.sync,
			},
		},
		Action: func(c *cli.Context) error { return runDaemon(c, cfg) },
	}
}

func runDaemon(c *cli.Context, cfg *daemonConfig) error {
	store, err := diskstore.Open(cfg.store,
		diskstore.WithInlineThreshold(cfg.inlineThreshold),
		diskstore.WithSync(cfg.sync),
	)
	if err != nil {
		return err
	}
	defer store.Close()

	sock := socketpath.Resolve(cfg.socket)
	// Remove a stale socket from a previous run before binding.
	if err := os.Remove(sock); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing stale socket %s: %w", sock, err)
	}
	ln, err := net.Listen("unix", sock)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", sock, err)
	}
	defer os.Remove(sock)

	srv := &http.Server{Handler: daemon.New(store)}

	// Shut down gracefully on context cancellation (tests) or SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(c.Context, os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	fmt.Fprintf(os.Stderr, "amber-store daemon listening on %s\n", sock)
	err = srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
