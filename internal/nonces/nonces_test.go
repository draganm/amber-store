package nonces_test

import (
	"testing"
	"time"

	"github.com/draganm/amber-store/internal/nonces"
)

func TestReplayDetection(t *testing.T) {
	c := nonces.New(time.Minute)
	now := time.Now()
	if c.SeenBefore("fp1", []byte("n1"), now) {
		t.Fatal("fresh nonce reported as seen")
	}
	if !c.SeenBefore("fp1", []byte("n1"), now) {
		t.Fatal("replayed nonce not detected")
	}
	if c.SeenBefore("fp2", []byte("n1"), now) {
		t.Fatal("same nonce from a different key reported as seen")
	}
}

func TestExpiry(t *testing.T) {
	c := nonces.New(time.Minute)
	now := time.Now()
	c.SeenBefore("fp", []byte("n"), now)
	// After more than 2× the window the entry is expired (timestamps that old
	// are rejected before the nonce check, so forgetting them is safe).
	later := now.Add(3 * time.Minute)
	if c.SeenBefore("fp", []byte("n"), later) {
		t.Fatal("expired nonce still reported as seen")
	}
}
