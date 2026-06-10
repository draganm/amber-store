package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/draganm/amber-store/daemon"
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/internal/socketpath"
	"github.com/draganm/amber-store/refstore"
	"github.com/urfave/cli/v2"
)

type daemonConfig struct {
	store           string
	socket          string
	inlineThreshold int
	sync            bool
	logLevel        string
	logFormat       string
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
			&cli.StringFlag{
				Name:        "log-level",
				Value:       "info",
				Usage:       "log level: debug, info, warn or error",
				Destination: &cfg.logLevel,
			},
			&cli.StringFlag{
				Name:        "log-format",
				Value:       "text",
				Usage:       "log format: text or json",
				Destination: &cfg.logFormat,
			},
		},
		Action: func(c *cli.Context) error { return runDaemon(c, cfg) },
	}
}

// buildLogger constructs the daemon's slog logger on stderr from the
// --log-level and --log-format flags.
func buildLogger(level, format string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid --log-level %q: %w", level, err)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	switch format {
	case "text":
		h = slog.NewTextHandler(os.Stderr, opts)
	case "json":
		h = slog.NewJSONHandler(os.Stderr, opts)
	default:
		return nil, fmt.Errorf("invalid --log-format %q: want text or json", format)
	}
	return slog.New(h), nil
}

func runDaemon(c *cli.Context, cfg *daemonConfig) error {
	logger, err := buildLogger(cfg.logLevel, cfg.logFormat)
	if err != nil {
		return err
	}

	store, err := diskstore.Open(cfg.store,
		diskstore.WithInlineThreshold(cfg.inlineThreshold),
		diskstore.WithSync(cfg.sync),
	)
	if err != nil {
		return err
	}
	defer store.Close()

	refs, err := refstore.Open(filepath.Join(cfg.store, "refs"), cfg.sync)
	if err != nil {
		return err
	}
	defer refs.Close()

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

	srv := &http.Server{
		Handler: daemon.New(store, refs, logger),
		// Route the http.Server's own diagnostics (handler panics, accept
		// errors) through the structured logger.
		ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	// Shut down gracefully on context cancellation (tests) or SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(c.Context, os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		// No timeout: let in-flight ingests/tar streams finish so a shutdown never
		// truncates a client mid-operation.
		_ = srv.Shutdown(context.Background())
	}()

	logger.Info("daemon listening", "socket", sock, "store", cfg.store)
	err = srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
