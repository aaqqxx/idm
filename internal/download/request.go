package download

import (
	"fmt"
	"net/http"
	"strings"
)

// RequestOptions customises the HTTP requests the engine issues. The zero
// value reproduces the previous behaviour (just a User-Agent). These knobs are
// what make the difference between "downloads the easy files" and "downloads
// the files you actually want": CDN Referer checks, signed URLs, cookie-gated
// links, and Basic Auth all need request customisation.
type RequestOptions struct {
	// UserAgent is sent on every request.
	UserAgent string

	// Headers are applied to every probe and range request. Hop-by-hop and
	// framing headers are rejected because setting them breaks the transfer.
	Headers map[string]string

	// Referer sets the Referer header. Many CDNs reject requests without it.
	Referer string

	// Cookies are sent as one Cookie header.
	Cookies []string

	// Username/Password enable HTTP Basic Auth.
	Username string
	Password string
}

// forbiddenHeaders are headers the transport must own. Letting a caller set
// them corrupts framing (Content-Length), defeats Range (Accept-Encoding with
// a compressed body), or fights the watchdog (Connection).
var forbiddenHeaders = map[string]bool{
	"host":              true,
	"content-length":    true,
	"transfer-encoding": true,
	"connection":        true,
	"accept-encoding":   true, // the engine forces identity for Range integrity
	"range":             true, // the engine owns Range
	"if-range":          true,
}

// Validate rejects request options that would corrupt the transfer.
func (o RequestOptions) Validate() error {
	for name := range o.Headers {
		if forbiddenHeaders[strings.ToLower(strings.TrimSpace(name))] {
			return fmt.Errorf("download: header %q is managed by the engine and cannot be overridden", name)
		}
	}
	return nil
}

// apply writes the customised headers onto req. It never overrides the Range,
// Accept-Encoding, or auth the engine depends on.
func (o RequestOptions) apply(req *http.Request) {
	if o.UserAgent != "" {
		req.Header.Set("User-Agent", o.UserAgent)
	}
	req.Header.Set("Accept-Encoding", "identity")

	if o.Referer != "" {
		req.Header.Set("Referer", o.Referer)
	}
	if len(o.Cookies) > 0 {
		req.Header.Set("Cookie", strings.Join(o.Cookies, "; "))
	}
	for name, value := range o.Headers {
		req.Header.Set(name, value)
	}
	if o.Username != "" || o.Password != "" {
		req.SetBasicAuth(o.Username, o.Password)
	}
}
