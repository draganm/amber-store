package remotesync_test

import (
	"context"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/remotesync"
)

func TestPushTransfersAllReachableObjects(t *testing.T) {
	h := newHarness(t)
	local := newLocalStore(t)
	root := buildTree(t, local)

	stats, err := remotesync.Push(context.Background(), local, h.rc(t), root, remotesync.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsTotal != 4 || stats.ObjectsPushed != 4 {
		t.Fatalf("stats = %+v, want 4/4", stats)
	}
	// every reachable object is now on the server
	keys, err := fstree.ReachableKeys(root, h.store.Get)
	if err != nil {
		t.Fatalf("server-side walk failed: %v", err)
	}
	if len(keys) != 4 {
		t.Fatalf("server has %d reachable objects, want 4", len(keys))
	}
}

func TestPushIsMinimalOnRerun(t *testing.T) {
	h := newHarness(t)
	local := newLocalStore(t)
	root := buildTree(t, local)
	ctx := context.Background()
	rc := h.rc(t)
	if _, err := remotesync.Push(ctx, local, rc, root, remotesync.Opts{}); err != nil {
		t.Fatal(err)
	}
	stats, err := remotesync.Push(ctx, local, rc, root, remotesync.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectsPushed != 0 || stats.BytesPushed != 0 {
		t.Fatalf("re-push transferred %+v, want nothing", stats)
	}
}

func TestPushReportsProgress(t *testing.T) {
	h := newHarness(t)
	local := newLocalStore(t)
	root := buildTree(t, local)
	var last, total int
	_, err := remotesync.Push(context.Background(), local, h.rc(t), root, remotesync.Opts{
		Progress: func(done, t int) { last, total = done, t },
	})
	if err != nil {
		t.Fatal(err)
	}
	if last != 4 || total != 4 {
		t.Fatalf("final progress = %d/%d, want 4/4", last, total)
	}
}
