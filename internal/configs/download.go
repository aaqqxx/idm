package configs

type DownloadMode string

const (
	DownloadModeLocal DownloadMode = "local"
	DownloadModeS3    DownloadMode = "s3"
)

type Download struct {
	Mode              DownloadMode `yaml:"mode"`
	DownloadDirectory string       `yaml:"download_directory"`
	Bucket            string       `yaml:"bucket"`
	Address           string       `yaml:"address"`
	Username          string       `yaml:"username"`
	Password          string       `yaml:"password"`
	Engine            Engine       `yaml:"engine"`
}

// Engine carries the segmented downloader tuning knobs. The zero value is
// replaced by DefaultEngine at Normalize time; the defaults mirror the
// constants recovered from Internet Download Manager 6.43.
type Engine struct {
	// MaxConnections is the user-visible ceiling, equivalent to IDM's
	// "Max. connections number" setting. Hard-capped by HardCap.
	MaxConnections int `yaml:"max_connections"`

	// HardCap is the absolute protocol ceiling (IDM 0x20 = 32).
	HardCap int `yaml:"hard_cap"`

	// MinChunkSize is the smallest span worth a dedicated connection, so that
	// a parallel connection is only opened when it can carry real work
	// (IDM 0x2800 = 10 KiB).
	MinChunkSize int64 `yaml:"min_chunk_size"`

	// MinWorkUnit is the smallest slice the scheduler will carve when it splits
	// an in-flight segment for an idle connection.
	MinWorkUnit int64 `yaml:"min_work_unit"`

	// IdleTimeout aborts and retries a connection that has produced no bytes
	// for this long (IDM uses 0x1e = 30s).
	IdleTimeout string `yaml:"idle_timeout"`

	// MaxRetries bounds per-segment retry attempts after transient failures.
	MaxRetries int `yaml:"max_retries"`

	// BufferSize is the per-connection read/write buffer.
	BufferSize int `yaml:"buffer_size"`

	// EnableResume persists the segment table so an interrupted download can
	// continue without re-reading already written bytes.
	EnableResume bool `yaml:"enable_resume"`

	// StateDirectory holds resume checkpoints. Defaults next to the download
	// directory so the state travels with the payload.
	StateDirectory string `yaml:"state_directory"`

	// UserAgent is sent on probe and range requests.
	UserAgent string `yaml:"user_agent"`

	// DialTimeout bounds TCP/TLS establishment per connection.
	DialTimeout string `yaml:"dial_timeout"`

	// MaxBytesPerSecond caps the aggregate transfer rate. Zero means
	// unlimited.
	MaxBytesPerSecond int64 `yaml:"max_bytes_per_second"`

	// NoOverwrite picks a unique destination name instead of replacing an
	// existing file.
	NoOverwrite bool `yaml:"no_overwrite"`

	// DisablePreallocate skips reserving the full size before the transfer.
	DisablePreallocate bool `yaml:"disable_preallocate"`

	// Headers are applied to every request. Framing/Range headers are rejected.
	Headers map[string]string `yaml:"headers"`

	// Referer sets the Referer header (many CDNs require it).
	Referer string `yaml:"referer"`

	// Cookies are sent as one Cookie header.
	Cookies []string `yaml:"cookies"`

	// Username/Password enable HTTP Basic Auth.
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

func DefaultEngine() Engine {
	return Engine{
		MaxConnections: 16,
		HardCap:        32,
		MinChunkSize:   10 * 1024,
		MinWorkUnit:    256 * 1024,
		IdleTimeout:    "30s",
		MaxRetries:     5,
		BufferSize:     128 * 1024,
		EnableResume:   true,
		StateDirectory: "",
		UserAgent:      "idm-segmented/1.0",
		DialTimeout:    "15s",

		MaxBytesPerSecond:  0,
		NoOverwrite:        false,
		DisablePreallocate: false,

		Headers:  nil,
		Referer:  "",
		Cookies:  nil,
		Username: "",
		Password: "",
	}
}

// Normalize fills unset fields with defaults and repairs nonsensical values.
func (e Engine) Normalize() Engine {
	d := DefaultEngine()

	if e.MaxConnections <= 0 {
		e.MaxConnections = d.MaxConnections
	}
	if e.HardCap <= 0 {
		e.HardCap = d.HardCap
	}
	if e.MaxConnections > e.HardCap {
		e.MaxConnections = e.HardCap
	}
	if e.MinChunkSize <= 0 {
		e.MinChunkSize = d.MinChunkSize
	}
	if e.MinWorkUnit < e.MinChunkSize {
		e.MinWorkUnit = d.MinWorkUnit
	}
	if e.IdleTimeout == "" {
		e.IdleTimeout = d.IdleTimeout
	}
	if e.MaxRetries <= 0 {
		e.MaxRetries = d.MaxRetries
	}
	if e.BufferSize <= 0 {
		e.BufferSize = d.BufferSize
	}
	if e.UserAgent == "" {
		e.UserAgent = d.UserAgent
	}
	if e.DialTimeout == "" {
		e.DialTimeout = d.DialTimeout
	}

	return e
}
