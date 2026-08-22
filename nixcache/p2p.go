package nixcache

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/draganm/amber-store/fstree"
	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/iroh"
	ikey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// The swarm speaks one ALPN. Each request is a postcard enum on its own
// bidi stream followed by FIN; the response is a postcard status enum (one
// varint byte) followed by the raw body until FIN.
const swarmALPN = "nix-cached/1"

// DefaultP2PPort is the UDP port the swarm endpoint binds by default.
const DefaultP2PPort = 8322

const (
	reqObjects = 0 // Objects{keys: bytes}   -> amberpack
	reqKeys    = 1 // Keys{root: [u8;32]}    -> keylist
	reqIndex   = 2 // Index                  -> 32-byte root, zero = none
	reqNarinfo = 3 // Narinfo{hashpart: str} -> narinfo text
)

const (
	statusOK       = 0
	statusNotFound = 1
	statusError    = 2
)

// maxP2PRequest bounds a request: the largest legitimate one is a
// maxPeerKeys keylist plus envelope.
const maxP2PRequest = maxPeerKeys*32 + 16

type request struct {
	kind     byte
	keys     []byte // reqObjects: flattened keylist
	root     [32]byte
	hashpart string
}

// encode produces the postcard encoding of the request enum.
func (r request) encode() []byte {
	b := []byte{r.kind}
	switch r.kind {
	case reqObjects:
		b = binary.AppendUvarint(b, uint64(len(r.keys)))
		b = append(b, r.keys...)
	case reqKeys:
		b = append(b, r.root[:]...)
	case reqNarinfo:
		b = binary.AppendUvarint(b, uint64(len(r.hashpart)))
		b = append(b, r.hashpart...)
	}
	return b
}

func decodeRequest(b []byte) (request, error) {
	bad := errors.New("nixcache: malformed peer request")
	if len(b) == 0 {
		return request{}, bad
	}
	r := request{kind: b[0]}
	rest := b[1:]
	switch r.kind {
	case reqObjects, reqNarinfo:
		n, k := binary.Uvarint(rest)
		if k <= 0 || uint64(len(rest)-k) != n {
			return request{}, bad
		}
		if r.kind == reqObjects {
			r.keys = rest[k:]
		} else {
			r.hashpart = string(rest[k:])
		}
	case reqKeys:
		if len(rest) != 32 {
			return request{}, bad
		}
		copy(r.root[:], rest)
	case reqIndex:
		if len(rest) != 0 {
			return request{}, bad
		}
	default:
		return request{}, bad
	}
	return r, nil
}

func readRequest(s *iroh.Stream) (request, error) {
	s.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer s.SetReadDeadline(time.Time{})
	b, err := io.ReadAll(io.LimitReader(s, maxP2PRequest+1))
	if err != nil {
		return request{}, err
	}
	if len(b) > maxP2PRequest {
		return request{}, fmt.Errorf("nixcache: request over %d bytes", maxP2PRequest)
	}
	return decodeRequest(b)
}

// readStatus maps the response status. The stream carries the body after it.
func readStatus(r io.Reader) error {
	var st [1]byte
	if _, err := io.ReadFull(r, st[:]); err != nil {
		return err
	}
	switch st[0] {
	case statusOK:
		return nil
	case statusNotFound:
		return fstree.ErrNotFound
	default:
		return errors.New("nixcache: peer error")
	}
}

func loadIdentity(path string) (ikey.SecretKey, error) {
	if b, err := os.ReadFile(path); err == nil {
		return ikey.SecretKeyFromSlice(b)
	} else if !os.IsNotExist(err) {
		return ikey.SecretKey{}, err
	}
	var seed [32]byte
	rand.Read(seed[:])
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return ikey.SecretKey{}, err
	}
	return ikey.NewSecretKey(seed), os.WriteFile(path, seed[:], 0o600)
}

// ParsePeers parses "<endpointid>@host:port[,host:port...]" or endpoint
// tickets. Host names are resolved now.
func ParsePeers(specs []string) ([]netaddr.EndpointAddr, error) {
	var out []netaddr.EndpointAddr
	for _, s := range specs {
		a, err := parsePeer(s)
		if err != nil {
			return nil, fmt.Errorf("peer %q: %w", s, err)
		}
		out = append(out, a)
	}
	return out, nil
}

func parsePeer(s string) (netaddr.EndpointAddr, error) {
	if strings.HasPrefix(s, "endpoint") {
		return endpointticket.Decode(s)
	}
	idStr, hosts, ok := strings.Cut(s, "@")
	if !ok || hosts == "" {
		return netaddr.EndpointAddr{}, errors.New("want <id>@host:port or an endpoint ticket")
	}
	id, err := ikey.ParseEndpointID(idStr)
	if err != nil {
		return netaddr.EndpointAddr{}, err
	}
	addr := netaddr.NewEndpointAddr(id)
	for _, hp := range strings.Split(hosts, ",") {
		aps, err := resolveAddrPort(hp)
		if err != nil {
			return netaddr.EndpointAddr{}, err
		}
		for _, ap := range aps {
			addr = addr.WithIP(ap)
		}
	}
	return addr, nil
}

func resolveAddrPort(hp string) ([]netip.AddrPort, error) {
	if ap, err := netip.ParseAddrPort(hp); err == nil {
		return []netip.AddrPort{ap}, nil
	}
	host, portStr, err := net.SplitHostPort(hp)
	if err != nil {
		return nil, err
	}
	port, err := net.LookupPort("udp", portStr)
	if err != nil {
		return nil, err
	}
	ips, err := net.LookupHost(host)
	if err != nil {
		return nil, err
	}
	var out []netip.AddrPort
	for _, ip := range ips {
		if a, err := netip.ParseAddr(ip); err == nil {
			out = append(out, netip.AddrPortFrom(a.Unmap(), uint16(port)))
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s: no addresses", host)
	}
	return out, nil
}

// PeerSpec formats addr as a --peer argument using its IP addresses.
func PeerSpec(addr netaddr.EndpointAddr) string {
	ips := addr.IPAddrs()
	parts := make([]string, len(ips))
	for i, ap := range ips {
		parts[i] = ap.String()
	}
	return addr.ID.String() + "@" + strings.Join(parts, ",")
}
