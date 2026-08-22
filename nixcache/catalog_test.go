package nixcache_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/draganm/amber-store/nixcache"
)

func storePathList(is ...int) string {
	var b strings.Builder
	for _, i := range is {
		fmt.Fprintf(&b, "/nix/store/%s-pkg-%d\n", hashPart(i), i)
	}
	return b.String()
}

func TestCatalogAddContains(t *testing.T) {
	c := &nixcache.Catalog{}
	added, err := c.AddList(strings.NewReader(storePathList(3, 1, 2, 1)))
	if err != nil {
		t.Fatal(err)
	}
	if added != 3 || c.Len() != 3 {
		t.Fatalf("added %d len %d", added, c.Len())
	}
	for i := 1; i <= 3; i++ {
		if !c.Contains(hashPart(i)) {
			t.Fatalf("missing %d", i)
		}
	}
	if c.Contains(hashPart(4)) || c.Contains("short") {
		t.Fatal("false positive")
	}
}

func TestCatalogMerge(t *testing.T) {
	c := &nixcache.Catalog{}
	if _, err := c.AddList(strings.NewReader(storePathList(1, 2))); err != nil {
		t.Fatal(err)
	}
	added, err := c.AddList(strings.NewReader(storePathList(2, 3)))
	if err != nil {
		t.Fatal(err)
	}
	if added != 1 || c.Len() != 3 {
		t.Fatalf("added %d len %d", added, c.Len())
	}
}

func TestCatalogBareHashparts(t *testing.T) {
	c := &nixcache.Catalog{}
	if _, err := c.AddList(strings.NewReader(hashPart(1) + "\n\n" + hashPart(2) + "\n")); err != nil {
		t.Fatal(err)
	}
	if c.Len() != 2 {
		t.Fatalf("len %d", c.Len())
	}
}

func TestCatalogRejectsMalformed(t *testing.T) {
	for _, line := range []string{"garbage", "/nix/store/tooshort-x", strings.Repeat("e", 32)} {
		c := &nixcache.Catalog{}
		if _, err := c.AddList(strings.NewReader(line + "\n")); err == nil {
			t.Fatalf("accepted %q", line)
		}
	}
}

func TestCatalogPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog")
	c, err := nixcache.LoadCatalog(path)
	if err != nil || c.Len() != 0 {
		t.Fatalf("missing file: %v len %d", err, c.Len())
	}
	if _, err := c.AddList(strings.NewReader(storePathList(1, 2, 3))); err != nil {
		t.Fatal(err)
	}
	if err := c.Save(path); err != nil {
		t.Fatal(err)
	}
	c2, err := nixcache.LoadCatalog(path)
	if err != nil {
		t.Fatal(err)
	}
	if c2.Len() != 3 || !c2.Contains(hashPart(2)) {
		t.Fatalf("reloaded len %d", c2.Len())
	}
}
