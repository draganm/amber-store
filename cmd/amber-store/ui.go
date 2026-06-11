package main

import (
	"embed"
	"io/fs"
)

// The admin SPA lives in ui/ (solid-js + vite). `go generate` builds it
// into ui/dist, which is embedded here so the binary serves the UI with
// no files on disk. The built dist is committed, so plain `go build`
// works without node; re-run `go generate ./cmd/amber-store` after
// changing the UI.

//go:generate npm --prefix ui ci
//go:generate npm --prefix ui run build

//go:embed all:ui/dist
var uiDist embed.FS

// adminUI returns the SPA root, with index.html at its top level.
func adminUI() (fs.FS, error) { return fs.Sub(uiDist, "ui/dist") }
