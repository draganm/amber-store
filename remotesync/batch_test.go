package remotesync_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/draganm/amber-store/amberpack"
	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/packstore"
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

func TestPushSizerUsesStoredCompressedSize(t *testing.T) {
	store, err := packstore.Open(filepath.Join(t.TempDir(), "s"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// A highly compressible blob: its stored (zstd) record is far smaller than
	// its raw payload. The push sizer balances batches by bytes on the wire, so
	// it must report the stored compressed size, not the logical key length.
	raw := bytes.Repeat([]byte("amber"), 4096) // 20480 bytes, very compressible
	blob, err := fstree.EncodeBlob(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(blob.Key, blob.Bytes); err != nil {
		t.Fatal(err)
	}
	rec, err := amberpack.EncodeRecord(blob.Key, blob.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	wantStored := uint64(len(rec) - amberpack.RecHeaderSize)

	size := remotesync.PushSizer(store)
	if got := size(blob.Key); got != wantStored {
		t.Fatalf("blob size = %d, want %d (stored compressed size)", got, wantStored)
	}
	if got := size(blob.Key); got >= blob.Key.Length() {
		t.Fatalf("blob size %d not below logical length %d; compression not reflected", got, blob.Key.Length())
	}
}

func TestPushSizerUsesActualSizeForNodes(t *testing.T) {
	store, err := packstore.Open(filepath.Join(t.TempDir(), "s"), packstore.WithSync(false))
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
	// FileNode keys encode the logical file length, not the node's stored size;
	// the push sizer must use the actual stored (post-compression) size.
	rec, err := amberpack.EncodeRecord(fn.Key, fn.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	wantStored := uint64(len(rec) - amberpack.RecHeaderSize)
	size := remotesync.PushSizer(store)
	if got := size(fn.Key); got != wantStored {
		t.Fatalf("node size = %d, want %d (stored size)", got, wantStored)
	}
}
