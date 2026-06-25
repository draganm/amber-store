package main

import (
	"bytes"
	"testing"

	"github.com/urfave/cli/v2"
)

func TestBrowseCommand_RequiresTerminal(t *testing.T) {
	app := &cli.App{Commands: []*cli.Command{browseCommand()}}
	app.Writer = &bytes.Buffer{}
	app.ErrWriter = &bytes.Buffer{}
	// Stdout in `go test` is not a TTY, so browse must refuse before any daemon I/O.
	err := app.Run([]string{"amber-store", "browse", "ref:does-not-matter"})
	if err == nil {
		t.Fatal("expected an error when not attached to a terminal")
	}
}

func TestBrowseCommand_ArgCount(t *testing.T) {
	app := &cli.App{Commands: []*cli.Command{browseCommand()}}
	app.Writer = &bytes.Buffer{}
	app.ErrWriter = &bytes.Buffer{}
	err := app.Run([]string{"amber-store", "browse"})
	if err == nil {
		t.Fatal("expected an error with no SPEC argument")
	}
}
