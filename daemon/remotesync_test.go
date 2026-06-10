package daemon_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/draganm/amber-store/reference"
)

// lastEvent decodes the final NDJSON line of a sync response.
func lastEvent(t *testing.T, body []byte) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	var ev map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &ev); err != nil {
		t.Fatalf("decoding final event %q: %v", lines[len(lines)-1], err)
	}
	return ev
}

func TestPushPullRoundTripThroughDaemon(t *testing.T) {
	h := newRemoteHarness(t)
	h.addRemote(t, "origin")
	signer := testSignerD(t)
	root := buildTree(t, h.store)
	h.putLocalRef(t, "backup", root, signer)

	// push objects
	code, body := h.doReq(t, "POST", "/v1/remote/push-objects?remote=origin&name=backup", nil)
	if code != 200 {
		t.Fatalf("push-objects = %d: %s", code, body)
	}
	ev := lastEvent(t, body)
	if ev["error"] != nil {
		t.Fatalf("push-objects error: %v", ev["error"])
	}
	ps, ok := ev["push_stats"].(map[string]any)
	if !ok || ps["objects_pushed"].(float64) != 4 {
		t.Fatalf("final event = %v, want 4 objects pushed", ev)
	}

	// push ref
	if code, body := h.doReq(t, "POST", "/v1/remote/push-ref?remote=origin&name=backup", nil); code != 204 {
		t.Fatalf("push-ref = %d: %s", code, body)
	}
	if _, err := h.srvRefs.Get("backup"); err != nil {
		t.Fatalf("ref not on server: %v", err)
	}

	// a second daemon pulls everything back
	h2 := newRemoteHarnessWithServer(t, h, h.clientSigner)
	h2.addRemote(t, "origin")

	code, body = h2.doReq(t, "POST", "/v1/remote/pull-objects?remote=origin&name=backup", nil)
	if code != 200 {
		t.Fatalf("pull-objects = %d: %s", code, body)
	}
	ev = lastEvent(t, body)
	if ev["error"] != nil {
		t.Fatalf("pull-objects error: %v", ev["error"])
	}
	if code, body := h2.doReq(t, "POST", "/v1/remote/pull-ref?remote=origin&name=backup", nil); code != 204 {
		t.Fatalf("pull-ref = %d: %s", code, body)
	}
	rec, err := h2.refs.Get("backup")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := reference.Decode(rec)
	if err != nil {
		t.Fatal(err)
	}
	if string(ref.Key) != string(root[:]) {
		t.Fatal("pulled ref points elsewhere")
	}
	if keys, err := fstree.ReachableKeys(root, h2.store.Get); err != nil || len(keys) != 4 {
		t.Fatalf("pulled tree incomplete: %d keys, %v", len(keys), err)
	}
}

func TestPushRefRejectsUnsigned(t *testing.T) {
	h := newRemoteHarness(t)
	h.addRemote(t, "origin")
	root := buildTree(t, h.store)
	rec, err := (reference.Reference{
		Name: "plain", Key: root[:], User: "u", CreatedAt: time.Now().UnixNano(),
	}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := h.refs.Put("plain", rec); err != nil {
		t.Fatal(err)
	}
	code, body := h.doReq(t, "POST", "/v1/remote/push-ref?remote=origin&name=plain", nil)
	if code != 422 || !strings.Contains(string(body), "signed") {
		t.Fatalf("push-ref unsigned = %d: %s, want 422 mentioning signing", code, body)
	}
}

func TestPullRefBeforeObjectsConflicts(t *testing.T) {
	h := newRemoteHarness(t)
	h.addRemote(t, "origin")
	signer := testSignerD(t)
	root := buildTree(t, h.store)
	h.putLocalRef(t, "early", root, signer)
	if code, _ := h.doReq(t, "POST", "/v1/remote/push-objects?remote=origin&name=early", nil); code != 200 {
		t.Fatal("push-objects failed")
	}
	if code, _ := h.doReq(t, "POST", "/v1/remote/push-ref?remote=origin&name=early", nil); code != 204 {
		t.Fatal("push-ref failed")
	}
	h2 := newRemoteHarnessWithServer(t, h, h.clientSigner)
	h2.addRemote(t, "origin")
	// pull-ref without pull-objects → 409 with a hint
	code, body := h2.doReq(t, "POST", "/v1/remote/pull-ref?remote=origin&name=early", nil)
	if code != 409 || !strings.Contains(string(body), "pull-objects") {
		t.Fatalf("pull-ref before objects = %d: %s, want 409 hinting pull-objects", code, body)
	}
}

func TestRemoteLsRefs(t *testing.T) {
	h := newRemoteHarness(t)
	h.addRemote(t, "origin")
	signer := testSignerD(t)
	root := buildTree(t, h.store)
	h.putLocalRef(t, "listme", root, signer)
	h.doReq(t, "POST", "/v1/remote/push-objects?remote=origin&name=listme", nil)
	h.doReq(t, "POST", "/v1/remote/push-ref?remote=origin&name=listme", nil)
	code, body := h.doReq(t, "GET", "/v1/remote/refs?remote=origin", nil)
	if code != 200 || !strings.Contains(string(body), `"listme"`) {
		t.Fatalf("ls-refs = %d: %s", code, body)
	}
}

func TestUnknownRemoteAndRef(t *testing.T) {
	h := newRemoteHarness(t)
	h.addRemote(t, "origin")
	if code, _ := h.doReq(t, "POST", "/v1/remote/push-objects?remote=nope&name=x", nil); code != 404 {
		t.Fatal("unknown remote should 404")
	}
	if code, _ := h.doReq(t, "POST", "/v1/remote/push-objects?remote=origin&name=nope", nil); code != 404 {
		t.Fatal("unknown local ref should 404")
	}
}
