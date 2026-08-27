package nixcache_test

import (
	"encoding/base64"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/nixcache"
)

// TestStockNixSubstitutes runs real nix against the node: copy a path from
// the loopback substituter into a fresh local store, trusting only the
// upstream signing key.
func TestStockNixSubstitutes(t *testing.T) {
	nix, err := exec.LookPath("nix")
	if err != nil {
		t.Skip("nix not on PATH")
	}
	u := newUpstream(t, "zstd", nil)
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "/nix/store/"+hashPart(7)+"-upstream-1.0\n")
	}))
	defer catalog.Close()
	node, srv, _ := newNode(t, u, func(c *nixcache.NodeConfig) {
		c.CatalogURLs = []string{catalog.URL + "/store-paths"}
	})
	node.SyncCatalog(t.Context())

	dest := filepath.Join(t.TempDir(), "root")
	t.Cleanup(func() { // nix marks store contents read-only
		filepath.WalkDir(dest, func(p string, d fs.DirEntry, err error) error {
			if err == nil && d.IsDir() {
				os.Chmod(p, 0o755)
			}
			return nil
		})
	})
	storePath := "/nix/store/" + hashPart(7) + "-upstream-1.0"
	cmd := exec.Command(nix,
		"--extra-experimental-features", "nix-command",
		"copy",
		"--from", srv.URL,
		"--to", "local?root="+dest,
		"--option", "trusted-public-keys", "test-1:"+base64.StdEncoding.EncodeToString(u.pub),
		"--option", "narinfo-cache-negative-ttl", "0",
		storePath,
	)
	cmd.Env = append(os.Environ(), "XDG_CACHE_HOME="+t.TempDir())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("nix copy: %v\n%s", err, out)
	}
	got, err := os.ReadFile(filepath.Join(dest, storePath, "f"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "upstream content" {
		t.Fatalf("substituted content %q", got)
	}
}
