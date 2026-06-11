package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

// startServe runs the serve command against fresh fixtures and waits
// until it answers; extraEnv entries are set for the run.
func startServe(t *testing.T, adminPassword string) (baseURL string) {
	t.Helper()
	if adminPassword != "" {
		t.Setenv("AMBER_ADMIN_PASSWORD", adminPassword)
	}
	identity := writeIdentityFixture(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- newApp().RunContext(ctx, []string{
			"amber-store", "serve", "--store", t.TempDir(),
			"--identity", identity,
			"--listen", addr, "--sync=false",
		})
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve returned: %v", err)
		}
	})

	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/v1/identity")
		if err == nil {
			resp.Body.Close()
			return "http://" + addr
		}
		if time.Now().After(deadline) {
			t.Fatalf("server did not come up: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestServeAdminUI checks the env-enabled admin UI end to end against
// the embedded dist: the SPA is served, login works with the env
// password, and the keys API answers behind the session.
func TestServeAdminUI(t *testing.T) {
	base := startServe(t, "swordfish")

	resp, err := http.Get(base + "/admin/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `<div id="root">`) {
		t.Fatalf("GET /admin/ = %d %q, want the embedded SPA", resp.StatusCode, body)
	}

	resp, err = http.Post(base+"/admin/api/login", "application/json",
		strings.NewReader(`{"password":"swordfish"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("login = %d, want 204", resp.StatusCode)
	}
	var session *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == "amber_admin_session" {
			session = c
		}
	}
	if session == nil {
		t.Fatal("no session cookie set")
	}

	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	line := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPub)))
	req, err := http.NewRequest("POST", base+"/admin/api/keys",
		strings.NewReader(`{"line":"`+line+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.AddCookie(session)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("add key = %d, want 204", resp.StatusCode)
	}

	req, err = http.NewRequest("GET", base+"/admin/api/keys", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.AddCookie(session)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "ssh-ed25519") {
		t.Fatalf("keys = %d %q, want the added key listed", resp.StatusCode, body)
	}
}

// TestServeAdminUIDisabled checks that without the password the admin
// surface simply does not exist.
func TestServeAdminUIDisabled(t *testing.T) {
	base := startServe(t, "")
	resp, err := http.Get(base + "/admin/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /admin/ without password = %d, want 404", resp.StatusCode)
	}
}
