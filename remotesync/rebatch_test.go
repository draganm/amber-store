package remotesync

import (
	"context"
	"errors"
	"testing"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/key"
)

// testKeys returns n distinct blob keys whose payloads are size bytes long
// (size must be >= 2; the first two bytes make the contents distinct).
func testKeys(t *testing.T, n, size int) []key.Key {
	t.Helper()
	out := make([]key.Key, 0, n)
	for i := range n {
		payload := make([]byte, size)
		payload[0], payload[1] = byte(i), byte(i>>8)
		o, err := fstree.EncodeBlob(payload)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, o.Key)
	}
	return out
}

func blobLength(k key.Key) uint64 { return k.Length() }

// runRebatch feeds inputs through rebatch and collects the emitted batches.
func runRebatch(t *testing.T, inputs [][]key.Key, target uint64) [][]key.Key {
	t.Helper()
	in := make(chan []key.Key)
	out := make(chan []key.Key)
	errc := make(chan error, 1)
	go func() {
		errc <- rebatch(context.Background(), in, out, target, blobLength)
		close(out)
	}()
	go func() {
		for _, batch := range inputs {
			in <- batch
		}
		close(in)
	}()
	var got [][]key.Key
	for b := range out {
		got = append(got, b)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	return got
}

func batchShapes(batches [][]key.Key) []int {
	shapes := make([]int, 0, len(batches))
	for _, b := range batches {
		shapes = append(shapes, len(b))
	}
	return shapes
}

func TestRebatchCoalescesSmallInputsAndFlushesTail(t *testing.T) {
	// ten single-key arrivals of 100-byte blobs, 400-byte target:
	// coalesce into [4 4] and flush the final partial [2]
	keys := testKeys(t, 10, 100)
	var inputs [][]key.Key
	for _, k := range keys {
		inputs = append(inputs, []key.Key{k})
	}
	got := batchShapes(runRebatch(t, inputs, 400))
	want := []int{4, 4, 2}
	if len(got) != len(want) {
		t.Fatalf("batch shapes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("batch shapes = %v, want %v", got, want)
		}
	}
}

func TestRebatchOversizedKeyGetsOwnBatch(t *testing.T) {
	big, err := fstree.EncodeBlob(make([]byte, 1000))
	if err != nil {
		t.Fatal(err)
	}
	small, err := fstree.EncodeBlob([]byte("small"))
	if err != nil {
		t.Fatal(err)
	}
	got := runRebatch(t, [][]key.Key{{big.Key, small.Key}}, 100)
	if len(got) != 2 || len(got[0]) != 1 || len(got[1]) != 1 {
		t.Fatalf("batch shapes = %v, want one key each", batchShapes(got))
	}
}

func TestRebatchHonorsMaxBatchKeys(t *testing.T) {
	// 9000 tiny keys under a huge byte target must still split at 8192
	keys := testKeys(t, 9000, 2)
	got := batchShapes(runRebatch(t, [][]key.Key{keys}, 1<<30))
	if len(got) != 2 || got[0] != maxBatchKeys || got[1] != 9000-maxBatchKeys {
		t.Fatalf("batch shapes = %v, want [%d %d]", got, maxBatchKeys, 9000-maxBatchKeys)
	}
}

func TestRebatchStopsOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	in := make(chan []key.Key)  // never closed
	out := make(chan []key.Key) // never drained
	if err := rebatch(ctx, in, out, 100, blobLength); !errors.Is(err, context.Canceled) {
		t.Fatalf("rebatch returned %v on canceled context, want context.Canceled", err)
	}
}

func TestRebatchUnblocksSendOnCancel(t *testing.T) {
	// two 100-byte keys against a 100-byte target force an emit; nobody
	// receives on out, so only cancellation can unblock the send. The
	// unbuffered rendezvous on in guarantees rebatch is past the receive
	// and blocked in send before cancel fires.
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan []key.Key)
	out := make(chan []key.Key)
	done := make(chan error, 1)
	go func() { done <- rebatch(ctx, in, out, 100, blobLength) }()
	in <- testKeys(t, 2, 100)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("rebatch returned %v after cancel while blocked on send, want context.Canceled", err)
	}
}
