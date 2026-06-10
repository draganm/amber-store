package server

import (
	"net/http"

	"github.com/draganm/amber-store/internal/httpsig"
)

// signAndWrite signs {nonce, status, blake3(body)} with the server identity
// and writes the response. Every non-streaming response goes through here so
// clients can verify the pinned server key on everything they receive.
func (h *handler) signAndWrite(w http.ResponseWriter, nonce []byte, status int, contentType string, body []byte) {
	sig, err := httpsig.SignResponse(h.identity, nonce, status, httpsig.HashBody(body))
	if err != nil {
		h.log.Error("signing response failed", "error", err)
		// Unreachable with a healthy local signer; this is the single path
		// where a client can see an unsigned (500) response.
		http.Error(w, "signing response failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set(httpsig.HeaderSignature, sig)
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	w.WriteHeader(status)
	if len(body) > 0 {
		_, _ = w.Write(body)
	}
}

// signError is the signed analogue of http.Error.
func (h *handler) signError(w http.ResponseWriter, nonce []byte, status int, msg string) {
	h.signAndWrite(w, nonce, status, "text/plain; charset=utf-8", []byte(msg+"\n"))
}
