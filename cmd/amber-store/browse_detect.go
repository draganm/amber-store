package main

import "unicode/utf8"

// viewMode is how the file viewer renders the loaded bytes.
type viewMode int

const (
	modeText viewMode = iota
	modeHex
	modeJSON
)

func (m viewMode) String() string {
	switch m {
	case modeText:
		return "TEXT"
	case modeHex:
		return "HEX"
	case modeJSON:
		return "JSON"
	default:
		return "?"
	}
}

// detectMode chooses the default view for freshly loaded bytes: text when the
// content looks textual, else JSON when it is one complete CBOR value, else hex.
func detectMode(data []byte) viewMode {
	if isTextual(data) {
		return modeText
	}
	if _, err := cborToJSON(data); err == nil {
		return modeJSON
	}
	return modeHex
}

// isTextual reports whether the first 8 KB is valid UTF-8 with no NUL byte and a
// low ratio of control characters (tab/newline/carriage-return excepted).
func isTextual(data []byte) bool {
	sample := data
	if len(sample) > 8192 {
		sample = sample[:8192]
	}
	if len(sample) == 0 {
		return true
	}
	if !utf8.Valid(sample) {
		return false
	}
	ctrl := 0
	for _, r := range string(sample) {
		switch {
		case r == '\t' || r == '\n' || r == '\r':
		case r < 0x20:
			ctrl++
		}
		if r == 0 {
			return false
		}
	}
	return ctrl*100 <= len(sample)*30
}
