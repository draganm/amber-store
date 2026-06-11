package allowfile_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/draganm/amber-store/internal/allowfile"
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

func writeFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "allowed")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestOpenListsKeys(t *testing.T) {
	plain, plainLine := testKey(t)
	admin, adminLine := testKey(t)
	f, err := allowfile.Open(writeFile(t, "# fleet\n\n"+plainLine+" laptop\nadmin "+adminLine+" ops\n"))
	if err != nil {
		t.Fatal(err)
	}
	keys, err := f.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("listed %d keys, want 2", len(keys))
	}
	if keys[0].Fingerprint != ssh.FingerprintSHA256(plain) || keys[0].Admin || keys[0].Comment != "laptop" || keys[0].Type != "ssh-ed25519" {
		t.Fatalf("plain key listed wrong: %+v", keys[0])
	}
	if keys[1].Fingerprint != ssh.FingerprintSHA256(admin) || !keys[1].Admin || keys[1].Comment != "ops" {
		t.Fatalf("admin key listed wrong: %+v", keys[1])
	}
}

func TestOpenMissingFile(t *testing.T) {
	if _, err := allowfile.Open(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("want error for missing file")
	}
}

func TestAddAppendsAndSwapsList(t *testing.T) {
	_, existing := testKey(t)
	path := writeFile(t, "# comment to keep\n"+existing+"\n")
	f, err := allowfile.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	added, addedLine := testKey(t)
	if err := f.Add(addedLine+" new-host", false); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.Current().Lookup(added.Marshal()); !ok {
		t.Fatal("added key not in live list")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "# comment to keep") {
		t.Fatal("comment line lost on rewrite")
	}
	if !strings.Contains(string(b), addedLine+" new-host") {
		t.Fatalf("added line not persisted:\n%s", b)
	}
	// reopen: persisted state must parse and contain both keys
	f2, err := allowfile.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := f2.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("reopened file has %d keys, want 2", len(keys))
	}
}

func TestAddAdminSetsOption(t *testing.T) {
	_, existing := testKey(t)
	path := writeFile(t, existing+"\n")
	f, err := allowfile.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	added, addedLine := testKey(t)
	if err := f.Add(addedLine, true); err != nil {
		t.Fatal(err)
	}
	if e, ok := f.Current().Lookup(added.Marshal()); !ok || !e.Admin {
		t.Fatalf("added key: ok=%v admin=%v, want allowed admin", ok, e.Admin)
	}
	keys, err := f.List()
	if err != nil {
		t.Fatal(err)
	}
	if !keys[1].Admin {
		t.Fatalf("listed key not admin: %+v", keys[1])
	}
}

func TestAddRejectsGarbageAndDuplicate(t *testing.T) {
	_, existing := testKey(t)
	f, err := allowfile.Open(writeFile(t, existing+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Add("not a key", false); err == nil {
		t.Fatal("want error for garbage line")
	}
	if err := f.Add(existing, false); err == nil {
		t.Fatal("want error for duplicate key")
	}
}

func TestRemove(t *testing.T) {
	gone, goneLine := testKey(t)
	stay, stayLine := testKey(t)
	path := writeFile(t, "# keep me\n"+goneLine+"\nadmin "+stayLine+"\n")
	f, err := allowfile.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Remove(ssh.FingerprintSHA256(gone)); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.Current().Lookup(gone.Marshal()); ok {
		t.Fatal("removed key still in live list")
	}
	if _, ok := f.Current().Lookup(stay.Marshal()); !ok {
		t.Fatal("unrelated key lost")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "# keep me") {
		t.Fatal("comment lost on remove")
	}
	if strings.Contains(string(b), goneLine) {
		t.Fatal("removed line still in file")
	}
}

func TestRemoveUnknownFingerprint(t *testing.T) {
	_, line := testKey(t)
	f, err := allowfile.Open(writeFile(t, line+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Remove("SHA256:doesnotexist"); err == nil {
		t.Fatal("want error for unknown fingerprint")
	}
}

func TestReloadPicksUpExternalEdit(t *testing.T) {
	_, line := testKey(t)
	path := writeFile(t, line+"\n")
	f, err := allowfile.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	extra, extraLine := testKey(t)
	if err := os.WriteFile(path, []byte(line+"\n"+extraLine+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.Current().Lookup(extra.Marshal()); ok {
		t.Fatal("external edit visible before reload")
	}
	if err := f.Reload(); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.Current().Lookup(extra.Marshal()); !ok {
		t.Fatal("external edit not visible after reload")
	}
}
