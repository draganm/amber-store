package server_test

import (
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/draganm/amber-store/allowlist"
	"github.com/draganm/amber-store/grant"
	"github.com/draganm/amber-store/refstore"
)

func TestWipeClearsObjectsAndRefsAndKeepsServing(t *testing.T) {
	op := capsSigner(t)
	h := newCapsHarness(t, "read,push-objects,write-refs,wipe "+keyLine(op))

	root := storedBlob(t, h, "content")
	rec := signedRecord(t, op, "thing:1", root)
	if resp := signedDo(t, h, op, nil, http.MethodPut, "/v1/refs?name=thing%3A1", rec); resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("put ref: got %d (%s)", resp.StatusCode, b)
	}

	resp := signedDo(t, h, op, nil, http.MethodPost, "/v1/wipe", nil)
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("wipe: got %d (%s), want 200", resp.StatusCode, b)
	}

	// Refs and objects are gone.
	if _, err := h.refs.Get("thing:1"); err != refstore.ErrNotFound {
		t.Fatalf("ref survived wipe: %v", err)
	}
	if has, err := h.store.Has(root); err != nil || has {
		t.Fatalf("object survived wipe: has=%v err=%v", has, err)
	}

	// The allowlist survives: the same key keeps serving, and writes work again.
	if resp := signedDo(t, h, op, nil, http.MethodGet, "/v1/refs", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("list after wipe: got %d, want 200", resp.StatusCode)
	}
	root2 := storedBlob(t, h, "content-after")
	rec2 := signedRecord(t, op, "thing:2", root2)
	if resp := signedDo(t, h, op, nil, http.MethodPut, "/v1/refs?name=thing%3A2", rec2); resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("put ref after wipe: got %d (%s)", resp.StatusCode, b)
	}
}

func TestWipeRequiresWipeCapability(t *testing.T) {
	legacy := capsSigner(t)  // option-less: full non-admin access
	powered := capsSigner(t) // everything short of wipe/admin
	h := newCapsHarness(t,
		keyLine(legacy),
		"read,push-objects,write-refs,delegate "+keyLine(powered),
	)
	if resp := signedDo(t, h, legacy, nil, http.MethodPost, "/v1/wipe", nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("legacy key wipe: got %d, want 403", resp.StatusCode)
	}
	if resp := signedDo(t, h, powered, nil, http.MethodPost, "/v1/wipe", nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("delegate key wipe: got %d, want 403", resp.StatusCode)
	}
}

func TestAdminImpliesWipe(t *testing.T) {
	admin := capsSigner(t)
	h := newCapsHarness(t, "admin "+keyLine(admin))
	if resp := signedDo(t, h, admin, nil, http.MethodPost, "/v1/wipe", nil); resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("admin wipe: got %d (%s), want 200", resp.StatusCode, b)
	}
}

func TestWipeUnreachableViaGrant(t *testing.T) {
	engine := capsSigner(t) // allowlisted delegate — but NOT wipe
	runner := capsSigner(t) // unlisted, holds a read+push grant
	h := newCapsHarness(t, "read,push-objects,write-refs,delegate "+keyLine(engine))
	now := time.Now()
	g, err := grant.Sign(grant.Grant{
		Subject:   runner.PublicKey().Marshal(),
		Caps:      []string{allowlist.CapRead, allowlist.CapPushObjects},
		IssuedAt:  now.UnixNano(),
		ExpiresAt: now.Add(15 * time.Minute).UnixNano(),
	}, engine)
	if err != nil {
		t.Fatal(err)
	}
	if resp := signedDo(t, h, runner, g, http.MethodPost, "/v1/wipe", nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("granted runner wipe: got %d, want 403", resp.StatusCode)
	}
}
