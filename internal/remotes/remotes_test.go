package remotes_test

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/internal/remotes"
)

func open(t *testing.T, path string) *remotes.Registry {
	t.Helper()
	r, err := remotes.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestAddGetPersistRemove(t *testing.T) {
	p := filepath.Join(t.TempDir(), "remotes")
	r := open(t, p)
	rem := remotes.Remote{URL: "https://amber.example.com", ServerKey: []byte("wire-key-bytes")}
	if err := r.Add("origin", rem); err != nil {
		t.Fatal(err)
	}
	if err := r.Add("origin", rem); !errors.Is(err, remotes.ErrExists) {
		t.Fatalf("second add = %v, want ErrExists", err)
	}
	// a fresh Registry sees the persisted state
	r2 := open(t, p)
	got, ok := r2.Get("origin")
	if !ok || got.URL != rem.URL || string(got.ServerKey) != string(rem.ServerKey) {
		t.Fatalf("reloaded remote = %+v, ok=%v", got, ok)
	}
	if err := r2.Remove("origin"); err != nil {
		t.Fatal(err)
	}
	if err := r2.Remove("origin"); !errors.Is(err, remotes.ErrNotFound) {
		t.Fatalf("second remove = %v, want ErrNotFound", err)
	}
	if _, ok := open(t, p).Get("origin"); ok {
		t.Fatal("removed remote survived reload")
	}
}

func TestResolve(t *testing.T) {
	r := open(t, filepath.Join(t.TempDir(), "remotes"))
	if _, _, err := r.Resolve(""); err == nil {
		t.Fatal("resolve with no remotes should fail")
	}
	if err := r.Add("a", remotes.Remote{URL: "http://a", ServerKey: []byte("k")}); err != nil {
		t.Fatal(err)
	}
	name, rem, err := r.Resolve("")
	if err != nil || name != "a" || rem.URL != "http://a" {
		t.Fatalf("sole resolve = %q, %+v, %v", name, rem, err)
	}
	if err := r.Add("b", remotes.Remote{URL: "http://b", ServerKey: []byte("k")}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Resolve(""); err == nil {
		t.Fatal("ambiguous resolve should fail")
	}
	if _, _, err := r.Resolve("missing"); err == nil {
		t.Fatal("unknown name should fail")
	}
	if name, _, err := r.Resolve("b"); err != nil || name != "b" {
		t.Fatalf("named resolve = %q, %v", name, err)
	}
}

func TestAddValidates(t *testing.T) {
	r := open(t, filepath.Join(t.TempDir(), "remotes"))
	bad := []struct {
		name string
		rem  remotes.Remote
	}{
		{"", remotes.Remote{URL: "http://x", ServerKey: []byte("k")}},
		{"has/slash", remotes.Remote{URL: "http://x", ServerKey: []byte("k")}},
		{"ok", remotes.Remote{URL: "ftp://x", ServerKey: []byte("k")}},
		{"ok", remotes.Remote{URL: "http://x", ServerKey: nil}},
	}
	for _, tc := range bad {
		if err := r.Add(tc.name, tc.rem); err == nil {
			t.Fatalf("Add(%q, %+v) succeeded, want error", tc.name, tc.rem)
		}
	}
}

func TestAllSorted(t *testing.T) {
	r := open(t, filepath.Join(t.TempDir(), "remotes"))
	for _, n := range []string{"b", "a", "c"} {
		if err := r.Add(n, remotes.Remote{URL: "http://" + n, ServerKey: []byte("k")}); err != nil {
			t.Fatal(err)
		}
	}
	all := r.All()
	if len(all) != 3 || all[0].Name != "a" || all[1].Name != "b" || all[2].Name != "c" {
		t.Fatalf("All() = %+v, want name order", all)
	}
}
