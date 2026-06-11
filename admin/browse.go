package admin

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/draganm/amber-store/key"
	"github.com/draganm/amber-store/reference"
	"github.com/draganm/amber-store/refstore"
)

// ObjectGetter is the read-only object-store view the ref browser needs;
// *diskstore.Store implements it.
type ObjectGetter interface {
	Get(k key.Key) ([]byte, error)
}

// RefStore is the read-only reference view the ref browser needs;
// *refstore.Store implements it.
type RefStore interface {
	Get(name string) ([]byte, error)
	All() ([]refstore.Record, error)
}

// refKind classifies a parsed store key for the refs listing.
func refKind(k key.Key) string {
	switch k.Type() {
	case key.DirLeaf, key.DirNode:
		return "dir"
	case key.Blob, key.FileNode:
		return "file"
	default:
		return "invalid"
	}
}

type refInfo struct {
	Name      string `json:"name"`
	Key       string `json:"key,omitempty"`
	User      string `json:"user,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
	Kind      string `json:"kind"`
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// listRefs serves every reference with its target kind. Records that fail
// to decode are listed as kind "invalid" rather than hidden — an operator
// tool must show what exists.
func (h *handler) listRefs(w http.ResponseWriter, r *http.Request) {
	recs, err := h.refs.All()
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]refInfo, 0, len(recs))
	for _, rec := range recs {
		ref, err := reference.Decode(rec.Data)
		if err != nil {
			out = append(out, refInfo{Name: rec.Name, Kind: "invalid"})
			continue
		}
		k, err := key.Parse(ref.Key)
		if err != nil {
			out = append(out, refInfo{Name: rec.Name, Kind: "invalid"})
			continue
		}
		out = append(out, refInfo{
			Name:      rec.Name,
			Key:       k.String(),
			User:      ref.User,
			CreatedAt: time.Unix(0, ref.CreatedAt).UTC().Format(time.RFC3339Nano),
			Kind:      refKind(k),
		})
	}
	writeJSON(w, map[string]any{"refs": out})
}
