package download

// EngineConfig is the tuning surface the engine consumes. It is deliberately
// a plain struct so the logic package can map configs.Engine onto it without
// pulling config plumbing into the transfer core.
type EngineConfig struct {
	// MaxConnections is the user-visible ceiling.
	MaxConnections int

	// HardCap is the absolute protocol ceiling.
	HardCap int

	// MediaCap clamps adaptive streams (IDM uses 4).
	MediaCap int

	// HostCap clamps hosts that tolerate few connections (IDM uses 8).
	HostCap int

	// IsMedia marks the payload as an adaptive media stream.
	IsMedia bool

	// HostLimited marks the host as concurrency-averse.
	HostLimited bool
}

// DefaultEngineConfig mirrors the defaults in configs.DefaultEngine.
func DefaultEngineConfig() EngineConfig {
	return EngineConfig{
		MaxConnections: 16,
		HardCap:        32,
		MediaCap:       4,
		HostCap:        8,
	}
}
