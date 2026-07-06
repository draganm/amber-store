package allowlist_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"

	"github.com/draganm/amber-store/allowlist"
	"golang.org/x/crypto/ssh"
)

func testPub(t *testing.T) ssh.PublicKey {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sp, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return sp
}

func testKeyLine(t *testing.T) string {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	s, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(ssh.MarshalAuthorizedKey(s.PublicKey())))
}

func TestParseLookupAndAdmin(t *testing.T) {
	plain, admin, absent := testPub(t), testPub(t), testPub(t)
	content := "# fleet keys\n\n" +
		string(ssh.MarshalAuthorizedKey(plain)) +
		"admin " + string(ssh.MarshalAuthorizedKey(admin))
	l, err := allowlist.Parse([]byte(content))
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := l.Lookup(plain.Marshal()); !ok || e.Admin {
		t.Fatalf("plain key: ok=%v admin=%v, want ok, non-admin", ok, e.Admin)
	}
	if e, ok := l.Lookup(admin.Marshal()); !ok || !e.Admin {
		t.Fatalf("admin key: ok=%v admin=%v, want ok, admin", ok, e.Admin)
	}
	if _, ok := l.Lookup(absent.Marshal()); ok {
		t.Fatal("absent key found")
	}
}

func TestParseRejectsGarbageLine(t *testing.T) {
	if _, err := allowlist.Parse([]byte("not a key\n")); err == nil {
		t.Fatal("want parse error")
	}
}

func TestDuplicateKeyLastWins(t *testing.T) {
	pub := testPub(t)
	content := "admin " + string(ssh.MarshalAuthorizedKey(pub)) +
		string(ssh.MarshalAuthorizedKey(pub))
	l, err := allowlist.Parse([]byte(content))
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := l.Lookup(pub.Marshal()); !ok || e.Admin {
		t.Fatalf("duplicate key: ok=%v admin=%v, want ok and last (non-admin) entry to win", ok, e.Admin)
	}
}

func TestNewCopiesEntries(t *testing.T) {
	pub := testPub(t)
	entries := map[string]allowlist.Entry{
		string(pub.Marshal()): {Admin: true},
	}
	l := allowlist.New(entries)
	// mutating the caller's map must not affect the built list
	delete(entries, string(pub.Marshal()))
	if e, ok := l.Lookup(pub.Marshal()); !ok || !e.Admin {
		t.Fatalf("key: ok=%v admin=%v, want ok, admin", ok, e.Admin)
	}
}

func TestParseEmpty(t *testing.T) {
	l, err := allowlist.Parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := l.Lookup([]byte("anything")); ok {
		t.Fatal("empty list found a key")
	}
}

func TestParseCaps(t *testing.T) {
	for _, tc := range []struct {
		name    string
		caps    []string
		want    allowlist.Entry
		wantErr bool
	}{
		{name: "read only", caps: []string{"read"}, want: allowlist.Entry{Read: true}},
		{name: "runner set", caps: []string{"read", "push-objects"}, want: allowlist.Entry{Read: true, PushObjects: true}},
		{name: "delegate", caps: []string{"delegate"}, want: allowlist.Entry{Delegate: true}},
		{name: "admin", caps: []string{"admin"}, want: allowlist.Entry{Admin: true}},
		{name: "wipe", caps: []string{"wipe"}, want: allowlist.Entry{Wipe: true}},
		{name: "unknown", caps: []string{"reed"}, wantErr: true},
		{name: "empty", caps: nil, want: allowlist.Entry{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := allowlist.ParseCaps(tc.caps)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestEntryAllows(t *testing.T) {
	full := allowlist.FullAccess()
	if !full.Allows(allowlist.CapRead) || !full.Allows(allowlist.CapPushObjects) || !full.Allows(allowlist.CapWriteRefs) {
		t.Fatal("FullAccess must allow read, push-objects and write-refs")
	}
	if full.Allows(allowlist.CapDelegate) || full.Allows(allowlist.CapAdmin) {
		t.Fatal("FullAccess must not allow delegate or admin")
	}
	admin := allowlist.Entry{Admin: true}
	for _, c := range []string{allowlist.CapRead, allowlist.CapPushObjects, allowlist.CapWriteRefs, allowlist.CapDelegate, allowlist.CapAdmin} {
		if !admin.Allows(c) {
			t.Fatalf("admin must imply %s", c)
		}
	}
	pushOnly := allowlist.Entry{PushObjects: true}
	if pushOnly.Allows(allowlist.CapWriteRefs) {
		t.Fatal("push-objects must not imply write-refs")
	}
	if (allowlist.Entry{}).Allows("bogus") {
		t.Fatal("unknown capability must never be allowed")
	}
}

func TestWipeCapability(t *testing.T) {
	if allowlist.FullAccess().Allows(allowlist.CapWipe) {
		t.Fatal("legacy FullAccess must not allow wipe")
	}
	wipe := allowlist.Entry{Wipe: true}
	if !wipe.Allows(allowlist.CapWipe) {
		t.Fatal("wipe entry must allow wipe")
	}
	if wipe.Allows(allowlist.CapRead) || wipe.Allows(allowlist.CapWriteRefs) {
		t.Fatal("wipe must not imply read or write-refs")
	}
	if !(allowlist.Entry{Admin: true}).Allows(allowlist.CapWipe) {
		t.Fatal("admin must imply wipe")
	}
	got := allowlist.Entry{Read: true, Wipe: true}.Caps()
	want := []string{allowlist.CapRead, allowlist.CapWipe}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("Caps() = %v, want %v", got, want)
	}
}

func TestParseCapOptions(t *testing.T) {
	lineFor := func(t *testing.T, opts string) (*allowlist.List, []byte) {
		t.Helper()
		s := testKeyLine(t)
		if opts != "" {
			s = opts + " " + s
		}
		l, err := allowlist.Parse([]byte(s))
		if err != nil {
			t.Fatal(err)
		}
		pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(s))
		if err != nil {
			t.Fatal(err)
		}
		return l, pub.Marshal()
	}

	t.Run("no options is legacy full access", func(t *testing.T) {
		l, wire := lineFor(t, "")
		e, ok := l.Lookup(wire)
		if !ok || e != allowlist.FullAccess() {
			t.Fatalf("got %+v ok=%v, want FullAccess", e, ok)
		}
	})
	t.Run("cap options are exact", func(t *testing.T) {
		l, wire := lineFor(t, "read,push-objects")
		e, ok := l.Lookup(wire)
		if !ok || e != (allowlist.Entry{Read: true, PushObjects: true}) {
			t.Fatalf("got %+v ok=%v", e, ok)
		}
	})
	t.Run("delegate option", func(t *testing.T) {
		l, wire := lineFor(t, "read,push-objects,write-refs,delegate")
		e, ok := l.Lookup(wire)
		if !ok || !e.Allows(allowlist.CapDelegate) || !e.Allows(allowlist.CapWriteRefs) {
			t.Fatalf("got %+v ok=%v", e, ok)
		}
	})
	t.Run("unknown option errors", func(t *testing.T) {
		s := "reed " + testKeyLine(t)
		if _, err := allowlist.Parse([]byte(s)); err == nil {
			t.Fatal("expected an error for an unknown option")
		}
	})
}
