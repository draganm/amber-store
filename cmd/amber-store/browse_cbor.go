package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

// cborToJSON decodes exactly one CBOR value from data and renders it as indented
// JSON. cbor.Unmarshal returns an error on invalid input or trailing bytes, so a
// successful call means data is exactly one complete CBOR value. CBOR types JSON
// lacks are mapped: byte strings to hex strings, non-string map keys to their
// stringified form, and tags to their unwrapped content.
func cborToJSON(data []byte) (string, error) {
	var v any
	if err := cbor.Unmarshal(data, &v); err != nil {
		return "", err
	}
	b, err := json.MarshalIndent(toJSONable(v), "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// toJSONable rewrites a CBOR-decoded value into something json.Marshal renders
// the way we want.
func toJSONable(v any) any {
	switch t := v.(type) {
	case []byte:
		return hex.EncodeToString(t)
	case map[any]any:
		m := make(map[string]any, len(t))
		for k, val := range t {
			m[keyString(k)] = toJSONable(val)
		}
		return m
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, val := range t {
			m[k] = toJSONable(val)
		}
		return m
	case []any:
		s := make([]any, len(t))
		for i, e := range t {
			s[i] = toJSONable(e)
		}
		return s
	case cbor.Tag:
		return toJSONable(t.Content)
	default:
		return v
	}
}

// keyString renders a CBOR map key as a JSON object key.
func keyString(k any) string {
	switch t := k.(type) {
	case string:
		return t
	case []byte:
		return hex.EncodeToString(t)
	default:
		return fmt.Sprintf("%v", t)
	}
}
