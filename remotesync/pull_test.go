package remotesync_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/remotesync"
)

func TestPullFetchesWholeTree(t *testing.T) {
	h := newHarness(t)
	root := buildTree(t, h.store) // tree lives on the SERVER
	local := newLocalStore(t)

	stats, err := remotesync.Pull(context.Background(), local, h.rc(t), root, remotesync.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsFetched != 4 {
		t.Fatalf("fetched %d objects, want 4", stats.ObjectsFetched)
	}
	// the local store can now serve the full tree
	keys, err := fstree.ReachableKeys(root, local.Get)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 4 {
		t.Fatalf("local reachable = %d, want 4", len(keys))
	}
	// payloads survived intact
	for _, k := range keys {
		want, err := h.store.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		got, err := local.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(want, got) {
			t.Fatalf("payload of %s differs", k)
		}
	}
}

func TestPullCompletesPartialLocalTree(t *testing.T) {
	h := newHarness(t)
	root := buildTree(t, h.store)
	local := newLocalStore(t)

	// pre-seed the local store with the root object only: present-but-
	// incomplete roots must still be descended (spec §3).
	rootData, err := h.store.Get(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := local.Put(root, rootData); err != nil {
		t.Fatal(err)
	}
	stats, err := remotesync.Pull(context.Background(), local, h.rc(t), root, remotesync.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsFetched != 3 {
		t.Fatalf("fetched %d objects, want 3 (root was local)", stats.ObjectsFetched)
	}
	if keys, err := fstree.ReachableKeys(root, local.Get); err != nil || len(keys) != 4 {
		t.Fatalf("local tree incomplete: %d keys, err %v", len(keys), err)
	}
}

func TestPullIsMinimalOnRerun(t *testing.T) {
	h := newHarness(t)
	root := buildTree(t, h.store)
	local := newLocalStore(t)
	ctx := context.Background()
	rc := h.rc(t)
	if _, err := remotesync.Pull(ctx, local, rc, root, remotesync.Opts{}); err != nil {
		t.Fatal(err)
	}
	stats, err := remotesync.Pull(ctx, local, rc, root, remotesync.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsFetched != 0 {
		t.Fatalf("re-pull fetched %d objects, want 0", stats.ObjectsFetched)
	}
}

func TestPullAbsentRootFails(t *testing.T) {
	h := newHarness(t)
	local := newLocalStore(t)
	absent, err := fstree.EncodeBlob([]byte("never on server"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := remotesync.Pull(context.Background(), local, h.rc(t), absent.Key, remotesync.Opts{}); err == nil {
		t.Fatal("pull of an absent root succeeded")
	}
}
