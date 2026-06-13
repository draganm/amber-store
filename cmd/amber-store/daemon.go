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
	"strings"
	"syscall"

	"github.com/draganm/amber-store/daemon"
	"github.com/draganm/amber-store/internal/identity"
	"github.com/draganm/amber-store/internal/remotes"
	"github.com/draganm/amber-store/internal/socketpath"
	"github.com/draganm/amber-store/packstore"
	"github.com/draganm/amber-store/refstore"
	"github.com/urfave/cli/v2"
	"golang.org/x/crypto/ssh"
)

type daemonConfig struct {
	store       string
	socket      string
	segmentSize int64
	sync        bool
	logLevel    string
	logFormat   string
	remoteKeys  cli.StringSlice
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
				Usage:       "packstore directory (created if missing)",
				Required:    true,
				Destination: &cfg.store,
			},
			&cli.StringFlag{
				Name:        "socket",
				Usage:       "unix socket path (default: $AMBER_STORE_SOCKET or a per-user path)",
				Destination: &cfg.socket,
			},
			&cli.Int64Flag{
				Name:        "segment-size",
				Value:       packstore.DefaultSegmentSize,
				Usage:       "seal the active segment once it reaches this many bytes",
				Destination: &cfg.segmentSize,
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
			&cli.StringSliceFlag{
				Name: "remote-key",
				Usage: "SSH identity for remote sync: PATH (default for all remotes) " +
					"or NAME=PATH (per-remote override); repeatable. Passphrase-protected " +
					"keys must be used via the ssh-agent (.pub path). Default: an " +
					"auto-generated identity stored in the store directory.",
				Destination: &cfg.remoteKeys,
			},
		},
		Action: func(c *cli.Context) error { return runDaemon(c, cfg) },
	}
}

// defaultRemoteSigner returns the daemon's default remote-sync signer: the
// configured one when --remote-key gave a bare PATH, otherwise the store's
// auto-generated identity.
func defaultRemoteSigner(configured ssh.Signer, storeDir string) (ssh.Signer, error) {
	if configured != nil {
		return configured, nil
	}
	return identity.LoadOrCreate(storeDir)
}

// parseRemoteKeys resolves --remote-key flags into signers held for the
// daemon's lifetime. A bare PATH is the default identity (at most one);
// NAME=PATH overrides per remote. Closers are tied to the process — agent
// connections stay open as long as the daemon runs.
func parseRemoteKeys(entries []string) (ssh.Signer, map[string]ssh.Signer, error) {
	var def ssh.Signer
	overrides := map[string]ssh.Signer{}
	for _, e := range entries {
		name, path, isOverride := strings.Cut(e, "=")
		if !isOverride {
			path, name = e, ""
		}
		signer, _, err := remoteIdentitySigner(path)
		if err != nil {
			return nil, nil, fmt.Errorf("--remote-key %s: %w", e, err)
		}
		if name == "" {
			if def != nil {
				return nil, nil, errors.New("--remote-key: only one default (NAME-less) key is allowed")
			}
			def = signer
			continue
		}
		if _, dup := overrides[name]; dup {
			return nil, nil, fmt.Errorf("--remote-key: duplicate override for remote %q", name)
		}
		overrides[name] = signer
	}
	return def, overrides, nil
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

	store, err := packstore.Open(filepath.Join(cfg.store, "packstore"),
		packstore.WithSegmentSize(cfg.segmentSize),
		packstore.WithSync(cfg.sync),
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

	defSigner, overrides, err := parseRemoteKeys(cfg.remoteKeys.Value())
	if err != nil {
		return err
	}
	defSigner, err = defaultRemoteSigner(defSigner, cfg.store)
	if err != nil {
		return err
	}
	registry, err := remotes.Open(filepath.Join(cfg.store, "remotes"))
	if err != nil {
		return err
	}

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
		Handler: daemon.NewWithRemotes(store, refs, logger, &daemon.RemoteConfig{
			Registry:      registry,
			DefaultSigner: defSigner,
			Signers:       overrides,
		}),
		// Route the http.Server's own diagnostics (handler panics, accept
		// errors) through the structured logger.
		ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	// Shut down gracefully on context cancellation (tests) or SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(c.Context, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// shutdownDone is closed after srv.Shutdown returns, meaning all in-flight
	// requests have finished and it is safe to close stores.
	shutdownDone := make(chan struct{})
	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		// No timeout: let in-flight ingests/tar streams finish so a shutdown never
		// truncates a client mid-operation.
		_ = srv.Shutdown(context.Background())
		close(shutdownDone)
	}()

	logger.Info("daemon listening", "socket", sock, "store", cfg.store,
		"identity", ssh.FingerprintSHA256(defSigner.PublicKey()))
	err = srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		// Serve returned because Shutdown closed the listener.  Wait until
		// Shutdown has drained all in-flight requests before the deferred
		// store/refs Close calls run.
		<-shutdownDone
		return nil
	}
	// Real listener error — shutdown was never triggered, so shutdownDone will
	// never be closed; return immediately without waiting.
	return err
}
