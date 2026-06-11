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
	"time"

	"github.com/draganm/amber-store/admin"
	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/internal/allowstore"
	"github.com/draganm/amber-store/internal/identity"
	"github.com/draganm/amber-store/internal/sshsign"
	"github.com/draganm/amber-store/refstore"
	"github.com/draganm/amber-store/server"
	"github.com/urfave/cli/v2"
	"golang.org/x/crypto/ssh"
)

type serveConfig struct {
	store         string
	listen        string
	identity      string
	tlsCert       string
	tlsKey        string
	authWindow    time.Duration
	sync          bool
	adminPassword string
	logLevel      string
	logFormat     string
}

func serveCommand() *cli.Command {
	cfg := &serveConfig{}
	return &cli.Command{
		Name:  "serve",
		Usage: "run the remote server other amber daemons push to and pull from",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "store",
				Aliases:     []string{"s"},
				Usage:       "diskstore directory (created if missing)",
				Required:    true,
				Destination: &cfg.store,
			},
			&cli.StringFlag{
				Name:        "listen",
				Value:       ":8590",
				Usage:       "TCP listen address",
				Destination: &cfg.listen,
			},
			&cli.StringFlag{
				Name:        "identity",
				Usage:       "server SSH identity: a private-key file, or a .pub resolved through the ssh-agent (default: auto-generated in the store directory)",
				Destination: &cfg.identity,
			},
			&cli.StringFlag{
				Name:        "tls-cert",
				Usage:       "TLS certificate file (omit both tls flags to serve plain HTTP behind a TLS-terminating proxy)",
				Destination: &cfg.tlsCert,
			},
			&cli.StringFlag{
				Name:        "tls-key",
				Usage:       "TLS private-key file",
				Destination: &cfg.tlsKey,
			},
			&cli.DurationFlag{
				Name:        "auth-window",
				Value:       5 * time.Minute,
				Usage:       "request-timestamp validity window (each side of now)",
				Destination: &cfg.authWindow,
			},
			&cli.BoolFlag{
				Name:        "sync",
				Value:       true,
				Usage:       "fsync writes for crash durability",
				Destination: &cfg.sync,
			},
			&cli.StringFlag{
				Name:        "admin-password",
				Usage:       "enable the /admin/ web UI for managing allowed keys; set via the environment so the password stays out of process listings",
				EnvVars:     []string{"AMBER_ADMIN_PASSWORD"},
				Destination: &cfg.adminPassword,
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
		Action: func(c *cli.Context) error { return runServe(c, cfg) },
	}
}

// remoteIdentitySigner loads an SSH identity under the remote-protocol key
// rule: unencrypted private-key files load directly, .pub files resolve via
// the ssh-agent, and passphrase-protected files are rejected — only agent
// signing is allowed for protected keys, because nothing may block at
// request time.
func remoteIdentitySigner(path string) (ssh.Signer, func(), error) {
	return sshsign.Signer(path, func(p string) ([]byte, error) {
		return nil, fmt.Errorf("key %s is passphrase-protected; load it into the ssh-agent and configure the .pub path instead", p)
	})
}

// resolveIdentity loads the explicitly configured identity, or the store's
// auto-generated one when no path was given.
func resolveIdentity(identityPath, storeDir string) (ssh.Signer, func(), error) {
	if identityPath == "" {
		signer, err := identity.LoadOrCreate(storeDir)
		return signer, func() {}, err
	}
	return remoteIdentitySigner(identityPath)
}

func runServe(c *cli.Context, cfg *serveConfig) error {
	if (cfg.tlsCert == "") != (cfg.tlsKey == "") {
		return errors.New("--tls-cert and --tls-key must be set together")
	}
	logger, err := buildLogger(cfg.logLevel, cfg.logFormat)
	if err != nil {
		return err
	}
	signer, closeIdentity, err := resolveIdentity(cfg.identity, cfg.store)
	if err != nil {
		return err
	}
	defer closeIdentity()

	store, err := diskstore.Open(cfg.store, diskstore.WithSync(cfg.sync))
	if err != nil {
		return err
	}
	defer store.Close()
	refs, err := refstore.Open(filepath.Join(cfg.store, "refs"), cfg.sync)
	if err != nil {
		return err
	}
	defer refs.Close()

	keys, err := allowstore.Open(filepath.Join(cfg.store, "allowed-keys"), cfg.sync)
	if err != nil {
		return err
	}
	defer keys.Close()
	if len(keys.List()) == 0 {
		if cfg.adminPassword == "" {
			logger.Warn("the allowlist is empty and the admin UI is disabled; this server cannot authorize anyone (set AMBER_ADMIN_PASSWORD to manage keys)")
		} else {
			logger.Warn("the allowlist is empty; add keys via the admin UI at /admin/")
		}
	}

	ln, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", cfg.listen, err)
	}

	handler := server.New(server.Config{
		Store:    store,
		Refs:     refs,
		Allow:    keys.Current,
		Identity: signer,
		Log:      logger,
		Window:   cfg.authWindow,
	})
	if cfg.adminPassword != "" {
		ui, err := adminUI()
		if err != nil {
			return fmt.Errorf("loading the embedded admin UI: %w", err)
		}
		adminHandler, err := admin.New(admin.Config{
			Password: cfg.adminPassword,
			Keys:     keys,
			UI:       ui,
			Log:      logger,
			Secure:   cfg.tlsCert != "",
		})
		if err != nil {
			return err
		}
		root := http.NewServeMux()
		root.Handle("/admin/", adminHandler)
		root.Handle("/admin", adminHandler)
		root.Handle("/", handler)
		handler = root
		logger.Info("admin UI enabled", "path", "/admin/")
	}

	srv := &http.Server{
		Handler: handler,
		// Route the http.Server's own diagnostics (handler panics, accept
		// errors) through the structured logger, like the daemon does.
		ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	ctx, stop := signal.NotifyContext(c.Context, os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownDone := make(chan struct{})
	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		_ = srv.Shutdown(context.Background())
		close(shutdownDone)
	}()

	logger.Info("serve listening",
		"addr", ln.Addr().String(),
		"store", cfg.store,
		"tls", cfg.tlsCert != "",
		"identity", ssh.FingerprintSHA256(signer.PublicKey()),
	)
	if cfg.tlsCert != "" {
		err = srv.ServeTLS(ln, cfg.tlsCert, cfg.tlsKey)
	} else {
		err = srv.Serve(ln)
	}
	if errors.Is(err, http.ErrServerClosed) {
		<-shutdownDone
		return nil
	}
	return err
}
