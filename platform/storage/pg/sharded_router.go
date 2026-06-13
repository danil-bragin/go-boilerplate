package pg

import "hash/fnv"

// defaultLogicalShards is the FIXED number of logical shards. Keys hash into
// logical shards; logical shards map to physical shards via a static assignment.
// The logical layer is forward-prep for a future resharding mechanism (keys
// never rehash; only the logical→physical assignment would move). It is NOT
// changed at runtime — changing it would re-route every key. See ADR-0019.
const defaultLogicalShards = 256

// Router maps an aggregate key to a physical shard index in [0, m). The hash is
// pinned FNV-1a 64-bit (stdlib hash/fnv) — deterministic and identical across
// processes, unlike maphash (whose per-process random seed would send the same
// key to different shards in different services). A key maps to exactly one
// physical shard for the life of the deployment.
type Router struct {
	logicalShards int
	assign        []int // len == logicalShards; assign[l] ∈ [0, m)
	m             int
}

// NewRouter builds a Router over m physical shards with the default 256 logical
// shards and the canonical assignment assign[l] = l % m (an even spread). m must
// be >= 1.
func NewRouter(m int) *Router {
	if m < 1 {
		m = 1
	}
	assign := make([]int, defaultLogicalShards)
	for l := range assign {
		assign[l] = l % m
	}
	return &Router{logicalShards: defaultLogicalShards, assign: assign, m: m}
}

// Physical returns the physical shard index for key.
func (r *Router) Physical(key string) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	// defaultLogicalShards is a typed constant (256), so the conversion to uint64
	// is safe and gosec G115 does not apply.
	logical := int(h.Sum64() % defaultLogicalShards)
	return r.assign[logical]
}
