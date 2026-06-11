package remotesync_test

import (
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/diskstore"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/remotesync"
)

func TestBatchesBalanceByBytes(t *testing.T) {
	// three 100-byte blobs
	var keys []key.Key
	for _, c := range []string{"a", "b", "c"} {
		o, err := fstree.EncodeBlob([]byte(c + string(make([]byte, 99))))
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, o.Key)
	}
	// 150-byte target: a second 100-byte blob would exceed it → one per batch
	batches := remotesync.Batches(keys, 150, remotesync.PullSizer())
	if len(batches) != 3 {
		t.Fatalf("got %d batches, want 3", len(batches))
	}
	// 250-byte target: two fit, the third spills
	batches = remotesync.Batches(keys, 250, remotesync.PullSizer())
	if len(batches) != 2 || len(batches[0]) != 2 || len(batches[1]) != 1 {
		t.Fatalf("got %v-shaped batches, want [2 1]", batches)
	}
}

func TestBatchesOversizedSingleItemGetsOwnBatch(t *testing.T) {
	big, err := fstree.EncodeBlob(make([]byte, 1000))
	if err != nil {
		t.Fatal(err)
	}
	small, err := fstree.EncodeBlob([]byte("small"))
	if err != nil {
		t.Fatal(err)
	}
	batches := remotesync.Batches([]key.Key{big.Key, small.Key}, 100, remotesync.PullSizer())
	if len(batches) != 2 || len(batches[0]) != 1 || len(batches[1]) != 1 {
		t.Fatalf("batches = %v, want one key each", batches)
	}
}

func TestPushSizerUsesActualSizeForNodes(t *testing.T) {
	store, err := diskstore.Open(filepath.Join(t.TempDir(), "s"), diskstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	blob, err := fstree.EncodeBlob(make([]byte, 4096))
	if err != nil {
		t.Fatal(err)
	}
	fn, err := fstree.EncodeFileNode([]key.Key{blob.Key})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(fn.Key, fn.Bytes); err != nil {
		t.Fatal(err)
	}
	size := remotesync.PushSizer(store)
	if got := size(blob.Key); got != 4096 {
		t.Fatalf("blob size = %d, want 4096 (from the key)", got)
	}
	// FileNode keys encode the logical file length (4096), not the node's
	// encoded size; the push sizer must use the actual stored size.
	if got := size(fn.Key); got != uint64(len(fn.Bytes)) {
		t.Fatalf("node size = %d, want %d (actual encoded size)", got, len(fn.Bytes))
	}
}
