package allowlist_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/draganm/amber-store/internal/allowlist"
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
