package download

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"time"
)

// SessionConfig is the top-level configuration for one download.
type SessionConfig struct {
	URL string

	// SavePath is the destination file. When empty, it is derived from the
	// URL or the Content-Disposition header.
	SavePath string

	// StateDir holds resume checkpoints. Empty means "next to SavePath".
	StateDir string

	// EnableResume persists and restores segment state.
	EnableResume bool

	// Engine ceilings.
	Engine EngineConfig

	// MaxConnections overrides Engine.MaxConnections when non-zero.
	MaxConnections int

	// MinChunk is the per-connection work floor. Defaults to 10 KiB, the value
	// recovered from IDM.
	MinChunk int64

	// MinWorkUnit is the smallest piece a split creates.
	MinWorkUnit int64

	// IdleTimeout aborts a stalled connection. Defaults to 30s, matching IDM.
	IdleTimeout time.Duration

	// MaxRetries bounds per-segment retries.
	MaxRetries int

	// BufferSize is the per-connection copy buffer.
	BufferSize int

	// UserAgent is sent on probe and range requests.
	UserAgent string

	// Request customises headers, Referer, cookies, and Basic Auth. It is the
	// hook for CDN Referer checks, signed URLs, and authenticated downloads.
	Request RequestOptions

	// DialTimeout bounds connection establishment.
	DialTimeout time.Duration

	// MaxBytesPerSecond caps the aggregate transfer rate across all
	// connections. Zero means unlimited.
	MaxBytesPerSecond int64

	// NoOverwrite keeps an existing file intact by picking a unique name
	// instead of replacing it. Default behaviour (false) is to replace.
	NoOverwrite bool

	// DisablePreallocate skips reserving the full size up front. Useful for
	// sparse-friendly filesystems or when the final size is unknown.
	DisablePreallocate bool

	// Progress receives periodic aggregate updates.
	Progress func(Progress)

	// SinkFactory overrides where payload bytes land. When nil the engine
	// writes straight to SavePath on the local filesystem. A remote backend
	// (object store) supplies a spooling sink here.
	SinkFactory func(ctx context.Context, savePath string) (Sink, error)
}

// SessionResult is the outcome of a completed download.
type SessionResult struct {
	Path     string
	Probe    Probe
	Result   Result
	Resumed  bool
	Segments int
}

// newHTTPClient builds the per-download client. A dedicated client (rather
// than http.DefaultClient) lets every connection carry its own idle pool entry
// so that MaxConnsPerHost actually bounds parallelism instead of serialising
// on the shared default transport.
func newHTTPClient(cfg SessionConfig, parallelism int) *http.Client {
	dial := cfg.DialTimeout
	if dial <= 0 {
		dial = 15 * time.Second
	}
	maxConns := parallelism
	if maxConns < 1 {
		maxConns = 1
	}

	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   dial,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          maxConns * 2,
		MaxIdleConnsPerHost:   maxConns,
		MaxConnsPerHost:       maxConns,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
		// Range downloads must not be transparently decompressed, otherwise
		// the byte offsets stop matching the remote file.
		DisableCompression: true,
	}

	return &http.Client{
		Transport: tr,
		Timeout:   0, // managed per-connection via context and idle watchdog
	}
}

// RunSession performs a complete segmented download: probe, plan, resume or
// start fresh, execute, then clean up the checkpoint.
func RunSession(ctx context.Context, cfg SessionConfig) (SessionResult, error) {
	if cfg.URL == "" {
		return SessionResult{}, errors.New("download: empty URL")
	}

	engine := cfg.Engine
	def := DefaultEngineConfig()
	if engine.MaxConnections <= 0 {
		engine.MaxConnections = def.MaxConnections
	}
	if engine.HardCap <= 0 {
		engine.HardCap = def.HardCap
	}
	if cfg.MaxConnections > 0 {
		engine.MaxConnections = cfg.MaxConnections
	}
	if engine.MaxConnections > engine.HardCap {
		engine.MaxConnections = engine.HardCap
	}
	if engine.MediaCap <= 0 {
		engine.MediaCap = def.MediaCap
	}
	if engine.HostCap <= 0 {
		engine.HostCap = def.HostCap
	}

	minChunk := cfg.MinChunk
	if minChunk <= 0 {
		minChunk = 10 * 1024
	}
	minUnit := cfg.MinWorkUnit
	if minUnit < minChunk {
		minUnit = 256 * 1024
	}
	idle := cfg.IdleTimeout
	if idle <= 0 {
		idle = 30 * time.Second
	}
	retries := cfg.MaxRetries
	if retries <= 0 {
		retries = 5
	}
	bufSize := cfg.BufferSize
	if bufSize <= 0 {
		bufSize = 128 * 1024
	}
	ua := cfg.UserAgent
	if ua == "" {
		ua = "idm-segmented/1.0"
	}

	// Fold the User-Agent into the request options so probe and range
	// requests share exactly the same customisation.
	reqOpts := cfg.Request
	if reqOpts.UserAgent == "" {
		reqOpts.UserAgent = ua
	}
	if err := reqOpts.Validate(); err != nil {
		return SessionResult{}, err
	}

	client := newHTTPClient(cfg, engine.MaxConnections)

	// Probe first: size, Range support, and validators. The probe round-trip
	// also seeds the growth policy: a fast handshake authorises a burst ramp.
	probeStart := time.Now()
	probe, err := ProbeURL(ctx, client, cfg.URL, reqOpts)
	if err != nil {
		return SessionResult{}, fmt.Errorf("download: probe failed: %w", err)
	}
	probeMS := time.Since(probeStart).Milliseconds()

	if probe.Size <= 0 {
		return SessionResult{}, errors.New("download: server did not report a content length")
	}

	savePath := cfg.SavePath
	if savePath == "" {
		savePath = filepath.Join(".", probe.FileName)
	}

	// Decide parallelism. Fast handshakes may burst; slow ones ramp up.
	lim := Limits{
		MaxConnections: engine.MaxConnections,
		HardCap:        engine.HardCap,
		MinChunk:       minChunk,
		MediaCap:       engine.MediaCap,
		HostCap:        engine.HostCap,
		IsMedia:        engine.IsMedia,
		HostLimited:    engine.HostLimited,
	}
	plan := PlanConnections(probe.Size, lim).WithClamps(lim)
	initial := plan.Connections

	var table *SegmentTable
	var ckptPath string
	resumedPath := ""

	if cfg.EnableResume {
		stateDir := StateDir(cfg.StateDir, filepath.Dir(savePath))
		ckptPath = CheckpointPath(stateDir, probe.FinalURL)
		cp, cerr := LoadCheckpoint(ckptPath, probe)
		if cerr != nil {
			return SessionResult{}, cerr
		}
		if cp != nil {
			table = RestoreSegmentTable(cp.Size, minChunk, cp.Segments)
			// Resume must target the exact path the checkpoint was taken for,
			// otherwise we would scatter partial bytes across two files.
			if cfg.SavePath == "" && cp.SavePath != "" {
				resumedPath = cp.SavePath
			}
		}
	}

	// Pick the destination. A resumed transfer keeps its original path; a
	// fresh one may take a unique name so an existing file is never clobbered.
	if resumedPath != "" {
		savePath = resumedPath
	} else if cfg.NoOverwrite && cfg.SinkFactory == nil {
		savePath = UniquePath(savePath)
	}

	result := SessionResult{Path: savePath, Probe: probe, Resumed: table != nil}

	var sink Sink
	if cfg.SinkFactory != nil {
		var err error
		sink, err = cfg.SinkFactory(ctx, savePath)
		if err != nil {
			return SessionResult{}, err
		}
	} else {
		fs, ferr := NewFileSinkWithOptions(savePath, FileSinkOptions{
			Preallocate: !cfg.DisablePreallocate,
		})
		if ferr != nil {
			return SessionResult{}, ferr
		}
		sink = fs
	}
	if closer, ok := sink.(interface{ Close() error }); ok {
		defer closer.Close()
	}

	// A non-range-capable server forces a single ordered connection.
	if !probe.SupportsRange {
		initial = 1
	}

	eng, err := NewEngine(Options{
		URL:         probe.FinalURL,
		Size:        probe.Size,
		Connections: initial,
		Engine:      engine,
		MinChunk:    minChunk,
		MinWorkUnit: minUnit,
		IdleTimeout: idle,
		MaxRetries:  retries,
		BufferSize:  bufSize,
		UserAgent:   ua,
		Request:     reqOpts,
		Throttle:    NewThrottle(cfg.MaxBytesPerSecond),
		ProbeMS:     probeMS,
		Progress:    cfg.Progress,
	}, client, sink)
	if err != nil {
		return SessionResult{}, err
	}
	if table != nil {
		eng.Seed(table)
	}

	res, runErr := eng.Run(ctx, probe.Size)
	if runErr != nil && !errors.Is(runErr, ErrAlreadyComplete) {
		// Persist whatever progress was made so a retry can resume.
		if cfg.EnableResume && eng.Table() != nil {
			_ = SaveCheckpoint(ckptPath, SnapshotCheckpoint(probe, eng.Table(), savePath))
		}
		return result, runErr
	}

	result.Result = res
	result.Segments = res.Segments

	if cfg.EnableResume && ckptPath != "" {
		_ = DropCheckpoint(ckptPath)
	}

	return result, nil
}
