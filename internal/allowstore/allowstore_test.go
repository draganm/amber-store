package allowstore_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"path/filepath"
	"strings"
	"testing"

	"github.com/draganm/amber-store/internal/allowstore"
	"golang.org/x/crypto/ssh"
)

func testKey(t *testing.T) (ssh.PublicKey, string) {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sp, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return sp, strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sp)))
}

func open(t *testing.T, dir string) *allowstore.Store {
	t.Helper()
	s, err := allowstore.Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return s
}

// findKey returns the listed key with the given fingerprint, or fails.
func findKey(t *testing.T, keys []allowstore.Key, fingerprint string) allowstore.Key {
	t.Helper()
	for _, k := range keys {
		if k.Fingerprint == fingerprint {
			return k
		}
	}
	t.Fatalf("no key with fingerprint %s in %+v", fingerprint, keys)
	return allowstore.Key{}
}

func TestOpenEmpty(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "allowed-keys"))
	if keys := s.List(); len(keys) != 0 {
		t.Fatalf("fresh store lists %d keys, want 0", len(keys))
	}
	pub, _ := testKey(t)
	if _, ok := s.Current().Lookup(pub.Marshal()); ok {
		t.Fatal("fresh store allows a key")
	}
}

func TestAddListsAndSwapsList(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "allowed-keys"))
	plain, plainLine := testKey(t)
	admin, adminLine := testKey(t)
	if err := s.Add(plainLine+" laptop", false); err != nil {
		t.Fatal(err)
	}
	if err := s.Add("admin "+adminLine+" ops", false); err != nil {
		t.Fatal(err)
	}
	keys := s.List()
	if len(keys) != 2 {
		t.Fatalf("listed %d keys, want 2", len(keys))
	}
	p := findKey(t, keys, ssh.FingerprintSHA256(plain))
	if p.Admin || p.Comment != "laptop" || p.Type != "ssh-ed25519" || p.Line != plainLine+" laptop" {
		t.Fatalf("plain key listed wrong: %+v", p)
	}
	a := findKey(t, keys, ssh.FingerprintSHA256(admin))
	if !a.Admin || a.Comment != "ops" || a.Line != "admin "+adminLine+" ops" {
		t.Fatalf("admin key listed wrong: %+v", a)
	}
	if e, ok := s.Current().Lookup(plain.Marshal()); !ok || e.Admin {
		t.Fatalf("plain key live lookup: ok=%v admin=%v, want allowed non-admin", ok, e.Admin)
	}
	if e, ok := s.Current().Lookup(admin.Marshal()); !ok || !e.Admin {
		t.Fatalf("admin key live lookup: ok=%v admin=%v, want allowed admin", ok, e.Admin)
	}
}

func TestAddAdminFlagSetsOption(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "allowed-keys"))
	added, line := testKey(t)
	if err := s.Add(line, true); err != nil {
		t.Fatal(err)
	}
	if e, ok := s.Current().Lookup(added.Marshal()); !ok || !e.Admin {
		t.Fatalf("added key: ok=%v admin=%v, want allowed admin", ok, e.Admin)
	}
	k := findKey(t, s.List(), ssh.FingerprintSHA256(added))
	if !k.Admin || !strings.HasPrefix(k.Line, "admin ") {
		t.Fatalf("listed key not admin: %+v", k)
	}
}

func TestAddRejectsBadLines(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "allowed-keys"))
	_, existing := testKey(t)
	if err := s.Add(existing, false); err != nil {
		t.Fatal(err)
	}
	_, fresh := testKey(t)
	for name, line := range map[string]string{
		"garbage":            "not a key",
		"duplicate":          existing,
		"trailing content":   fresh + " comment\n" + fresh,
		"unsupported option": "no-pty " + fresh,
	} {
		if err := s.Add(line, false); err == nil {
			t.Fatalf("Add(%s) succeeded, want error", name)
		}
	}
}

func TestRemove(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "allowed-keys"))
	gone, goneLine := testKey(t)
	stay, stayLine := testKey(t)
	if err := s.Add(goneLine, false); err != nil {
		t.Fatal(err)
	}
	if err := s.Add("admin "+stayLine, false); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(ssh.FingerprintSHA256(gone)); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Current().Lookup(gone.Marshal()); ok {
		t.Fatal("removed key still in live list")
	}
	if _, ok := s.Current().Lookup(stay.Marshal()); !ok {
		t.Fatal("unrelated key lost")
	}
	if keys := s.List(); len(keys) != 1 {
		t.Fatalf("list after remove has %d keys, want 1", len(keys))
	}
}

func TestRemoveUnknownFingerprint(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "allowed-keys"))
	if err := s.Remove("SHA256:doesnotexist"); err == nil {
		t.Fatal("want error for unknown fingerprint")
	}
}

func TestPersistsAcrossReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "allowed-keys")
	s, err := allowstore.Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	plain, plainLine := testKey(t)
	admin, adminLine := testKey(t)
	if err := s.Add(plainLine+" laptop", false); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(adminLine+" ops", true); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := open(t, dir)
	keys := s2.List()
	if len(keys) != 2 {
		t.Fatalf("reopened store lists %d keys, want 2", len(keys))
	}
	p := findKey(t, keys, ssh.FingerprintSHA256(plain))
	if p.Admin || p.Comment != "laptop" {
		t.Fatalf("plain key survived wrong: %+v", p)
	}
	if e, ok := s2.Current().Lookup(admin.Marshal()); !ok || !e.Admin {
		t.Fatalf("admin key after reopen: ok=%v admin=%v, want allowed admin", ok, e.Admin)
	}
}
