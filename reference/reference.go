// Package reference defines the named-pointer record: a global name pointing
// at a store key, with creator, creation time, and an optional opaque
// signature. Encoding is RFC 8949 §4.2 core-deterministic CBOR, matching the
// fstree object convention (canonical map, integer keys).
package reference

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/draganm/amber-store/key"
	"github.com/fxamacker/cbor/v2"
)

// MaxNameLen is the maximum reference name length in bytes.
const MaxNameLen = 1024

// encMode is the shared deterministic encoder, mirroring fstree.encMode.
var encMode cbor.EncMode

func init() {
	opts := cbor.CoreDetEncOptions()
	opts.NilContainers = cbor.NilContainerAsEmpty
	m, err := opts.EncMode()
	if err != nil {
		panic(fmt.Sprintf("reference: building CBOR enc mode: %v", err))
	}
	encMode = m
}

// Reference is the record stored under a name. Fields are encoded as a
// canonical CBOR map with integer keys 0-4; Signature (key 4) is omitted when
// absent so the unsigned encoding doubles as the signature payload.
type Reference struct {
	Name      string `cbor:"0,keyasint"`
	Key       []byte `cbor:"1,keyasint"` // 32-byte canonical store key
	User      string `cbor:"2,keyasint"`
	CreatedAt int64  `cbor:"3,keyasint"` // ns since the Unix epoch
	Signature []byte `cbor:"4,keyasint,omitempty"`
}

// ValidateName checks the reference-name rules: 1..MaxNameLen bytes of valid
// UTF-8, no '@' (the ref/path separator) and no control characters. '/' is
// allowed; names are opaque strings with no structural meaning.
func ValidateName(name string) error {
	if name == "" {
		return errors.New("reference name must not be empty")
	}
	if len(name) > MaxNameLen {
		return fmt.Errorf("reference name exceeds %d bytes", MaxNameLen)
	}
	if !utf8.ValidString(name) {
		return errors.New("reference name must be valid UTF-8")
	}
	for _, r := range name {
		if r == '@' {
			return errors.New("reference name must not contain '@'")
		}
		if r < 0x20 || r == 0x7f {
			return errors.New("reference name must not contain control characters")
		}
	}
	return nil
}

// validate checks the whole record: name rules plus a canonical key.
func (r Reference) validate() error {
	if err := ValidateName(r.Name); err != nil {
		return err
	}
	if _, err := key.Parse(r.Key); err != nil {
		return fmt.Errorf("reference key: %w", err)
	}
	return nil
}

// Encode returns the deterministic CBOR encoding of a validated record.
func (r Reference) Encode() ([]byte, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	return encMode.Marshal(r)
}

// Decode parses and validates a record.
func Decode(b []byte) (Reference, error) {
	var r Reference
	if err := cbor.Unmarshal(b, &r); err != nil {
		return Reference{}, fmt.Errorf("decoding reference: %w", err)
	}
	if err := r.validate(); err != nil {
		return Reference{}, err
	}
	return r, nil
}

// SignaturePayload returns the bytes a signature runs over: the deterministic
// encoding of the record without its Signature field.
func (r Reference) SignaturePayload() ([]byte, error) {
	r.Signature = nil
	return r.Encode()
}
