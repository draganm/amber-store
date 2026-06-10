package main

import (
	"context"
	"fmt"
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
		return key.Key{}, "", fmt.Errorf("invalid reference spec %q: %w", s, err)
	}
	rec, err := cl.GetRef(ctx, name)
	if err != nil {
		return key.Key{}, "", err
	}
	// GetRef already validated the record, so this parse cannot fail in
	// practice; the wrap keeps any future gap attributable to the reference.
	k, err := key.Parse(rec.Key)
	if err != nil {
		return key.Key{}, "", fmt.Errorf("reference %q: stored key: %w", name, err)
	}
	return k, path, nil
}
