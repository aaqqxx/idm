package download

// ConnectionPlan describes how many parallel connections a transfer should use
// and why. Keeping the decision explicit makes it testable and loggable.
type ConnectionPlan struct {
	// Connections is the chosen parallelism.
	Connections int

	// Remaining is the outstanding byte count at decision time.
	Remaining int64

	// BySize is the count implied purely by the per-connection floor.
	BySize int

	// ByCap is the user ceiling.
	ByCap int

	// Limited marks a plan clamped by a protocol or host restriction rather
	// than by size or user settings.
	Limited bool
}

// MinTailBytes is the size below which an unfinished transfer is finished on a
// single connection instead of fanning out. IDM compares against 0x2801
// (10241) bytes, i.e. one full 10 KiB work unit plus a byte.
func MinTailBytes(minChunk int64) int64 {
	if minChunk <= 0 {
		minChunk = 10 * 1024
	}
	return minChunk + 1
}

// PlanConnections reproduces the connection-count decision recovered from IDM's
// FUN_005bad00:
//
//	n = remaining / minChunk                 // per-connection floor
//	n = min(n, userMax)                      // user ceiling
//	n = min(n, hardCap)                      // absolute ceiling (32)
//
// plus the special cases: nothing sensible to do yields 1, and a transfer with
// less than one work unit left is finished single-threaded.
//
// The returned plan never exceeds cfg.HardCap and is always >= 1.
func PlanConnections(remaining int64, cfg Limits) ConnectionPlan {
	if cfg.MinChunk <= 0 {
		cfg.MinChunk = 10 * 1024
	}
	if cfg.HardCap <= 0 {
		cfg.HardCap = 32
	}
	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = 1
	}

	plan := ConnectionPlan{
		Remaining: remaining,
		ByCap:     cfg.MaxConnections,
	}

	if remaining <= 0 {
		plan.Connections = 1
		return plan
	}

	// Too little left to justify parallelism: drain it on one connection.
	if remaining < MinTailBytes(cfg.MinChunk) {
		plan.Connections = 1
		plan.BySize = 1
		return plan
	}

	bySize := int(remaining / cfg.MinChunk)
	if bySize < 1 {
		bySize = 1
	}
	plan.BySize = bySize

	n := bySize
	if n > cfg.MaxConnections {
		n = cfg.MaxConnections
	}
	if n > cfg.HardCap {
		n = cfg.HardCap
	}
	if n < 1 {
		n = 1
	}
	plan.Connections = n
	return plan
}

// Limits is the subset of engine configuration that affects the connection
// count decision.
type Limits struct {
	MaxConnections int
	HardCap        int
	MinChunk       int64

	// MediaCap clamps adaptive streaming manifests to a small connection count
	// (IDM uses 4 for media). Zero disables the clamp.
	MediaCap int

	// HostCap clamps hosts that advertise a low concurrency tolerance (IDM
	// uses 8). Zero disables the clamp.
	HostCap int

	// IsMedia/HostLimited select which clamp applies.
	IsMedia     bool
	HostLimited bool
}

// WithClamps applies the protocol/host clamps. It is separate from
// PlanConnections so the base arithmetic stays directly comparable to the
// disassembly.
func (p ConnectionPlan) WithClamps(cfg Limits) ConnectionPlan {
	n := p.Connections

	if cfg.IsMedia && cfg.MediaCap > 0 && n > cfg.MediaCap {
		n = cfg.MediaCap
		p.Limited = true
	}
	if cfg.HostLimited && cfg.HostCap > 0 && n > cfg.HostCap {
		n = cfg.HostCap
		p.Limited = true
	}
	if n < 1 {
		n = 1
	}
	p.Connections = n
	return p
}
