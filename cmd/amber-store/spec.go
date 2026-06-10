package main

import (
	"context"
	"strings"

	"github.com/draganm/amber-store/client"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
)

// resolveSpec parses a content spec: either KEY[/PATH] (lowercase-hex key,
// slash-separated subpath) or ref:NAME[@PATH] (reference name, '@'-separated
// subpath — '@' is banned in names, so the first '@' is unambiguous).
// Reference names resolve through the daemon.
func resolveSpec(ctx context.Context, cl *client.Client, s string) (key.Key, string, error) {
	rest, isRef := strings.CutPrefix(s, "ref:")
	if !isRef {
		return parseKeyPath(s)
	}
	name, path, _ := strings.Cut(rest, "@")
	if err := reference.ValidateName(name); err != nil {
		return key.Key{}, "", err
	}
	rec, err := cl.GetRef(ctx, name)
	if err != nil {
		return key.Key{}, "", err
	}
	k, err := key.Parse(rec.Key)
	if err != nil {
		return key.Key{}, "", err
	}
	return k, path, nil
}
