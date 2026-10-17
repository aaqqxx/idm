package download

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
)

// Probe is the outcome of the capability negotiation that precedes a
// segmented transfer. It mirrors what IDM does with its small "bytes=0-"
// request: learn the size, decide whether the server honours Range, and
// capture the validators that make a resumed transfer safe.
type Probe struct {
	URL           string `json:"url"`
	FinalURL      string `json:"final_url"`
	Size          int64  `json:"size"`
	SupportsRange bool   `json:"supports_range"`
	ETag          string `json:"etag"`
	LastModified  string `json:"last_modified"`
	ContentType   string `json:"content_type"`
	FileName      string `json:"file_name"`
}

// Validator identifies a specific version of the remote resource. A resumed
// transfer is only valid when the validator still matches.
func (p Probe) Validator() string {
	if p.ETag != "" {
		return p.ETag
	}
	return p.LastModified
}

// probeRangeBytes is the single-byte probe span. IDM uses the same trick: a
// one-byte Range request is cheap and proves Range support without pulling the
// whole object.
const probeRangeBytes = "bytes=0-0"

// ProbeURL negotiates capabilities for rawURL. It never reads more than a
// single byte of payload.
func ProbeURL(ctx context.Context, client *http.Client, rawURL string, opts RequestOptions) (Probe, error) {
	p := Probe{URL: rawURL}

	if err := p.head(ctx, client, opts); err != nil || p.Size <= 0 || !looksRangeCapable(p) {
		// HEAD is unreliable: many servers omit Accept-Ranges, some reject HEAD
		// outright. Fall back to the one-byte ranged GET.
		if err := p.rangedProbe(ctx, client, opts); err != nil {
			return Probe{}, err
		}
	}

	if p.FinalURL == "" {
		p.FinalURL = rawURL
	}
	if p.FileName == "" {
		p.FileName = filenameFromURL(p.FinalURL)
	}
	return p, nil
}

func (p *Probe) head(ctx context.Context, client *http.Client, opts RequestOptions) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, p.URL, http.NoBody)
	if err != nil {
		return err
	}
	opts.apply(req)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("probe: HEAD returned %s", resp.Status)
	}

	p.FinalURL = resp.Request.URL.String()
	p.applyHeaders(resp)
	return nil
}

func (p *Probe) rangedProbe(ctx context.Context, client *http.Client, opts RequestOptions) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.URL, http.NoBody)
	if err != nil {
		return err
	}
	opts.apply(req)
	req.Header.Set("Range", probeRangeBytes)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer drainClose(resp.Body)

	p.FinalURL = resp.Request.URL.String()
	p.applyHeaders(resp)

	switch resp.StatusCode {
	case http.StatusPartialContent:
		p.SupportsRange = true
		if total, ok := parseContentRangeTotal(resp.Header.Get("Content-Range")); ok {
			p.Size = total
		}
		// Consume the single byte so the connection can be reused.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64))
	case http.StatusOK:
		// Server ignored Range and is sending the whole body: a single
		// connection is the only safe strategy.
		p.SupportsRange = false
	default:
		return fmt.Errorf("probe: ranged GET returned %s", resp.Status)
	}
	return nil
}

func (p *Probe) applyHeaders(resp *http.Response) {
	if resp.ContentLength > 0 {
		p.Size = resp.ContentLength
	}
	if ar := resp.Header.Get("Accept-Ranges"); strings.Contains(strings.ToLower(ar), "bytes") {
		p.SupportsRange = true
	}
	if v := resp.Header.Get("ETag"); v != "" {
		p.ETag = v
	}
	if v := resp.Header.Get("Last-Modified"); v != "" {
		p.LastModified = v
	}
	if v := resp.Header.Get("Content-Type"); v != "" {
		p.ContentType = v
	}
	if v := filenameFromDisposition(resp.Header.Get("Content-Disposition")); v != "" {
		p.FileName = v
	}
}

func looksRangeCapable(p Probe) bool { return p.SupportsRange }

func parseContentRangeTotal(v string) (int64, bool) {
	// Expected: "bytes 0-0/12345"
	slash := strings.LastIndexByte(v, '/')
	if slash < 0 {
		return 0, false
	}
	total := strings.TrimSpace(v[slash+1:])
	if total == "*" || total == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(total, 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

func filenameFromDisposition(v string) string {
	if v == "" {
		return ""
	}
	lower := strings.ToLower(v)
	if i := strings.Index(lower, "filename*="); i >= 0 {
		rest := v[i+len("filename*="):]
		if j := strings.IndexByte(rest, ';'); j >= 0 {
			rest = rest[:j]
		}
		rest = strings.TrimSpace(rest)
		// RFC 5987: charset'lang'percent-encoded
		if parts := strings.SplitN(rest, "'", 3); len(parts) == 3 {
			if decoded, err := url.PathUnescape(parts[2]); err == nil && decoded != "" {
				return sanitizeFilename(decoded)
			}
		}
	}
	if i := strings.Index(lower, "filename="); i >= 0 {
		rest := v[i+len("filename="):]
		if j := strings.IndexByte(rest, ';'); j >= 0 {
			rest = rest[:j]
		}
		rest = strings.Trim(strings.TrimSpace(rest), `"`)
		if rest != "" {
			return sanitizeFilename(rest)
		}
	}
	return ""
}

func filenameFromURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "download"
	}
	base := path.Base(u.Path)
	if base == "" || base == "." || base == "/" {
		return "download"
	}
	return sanitizeFilename(base)
}

func sanitizeFilename(name string) string {
	name = strings.TrimSpace(name)
	replacer := strings.NewReplacer(
		"/", "_", "\\", "_", ":", "_", "*", "_",
		"?", "_", `"`, "_", "<", "_", ">", "_", "|", "_",
	)
	name = replacer.Replace(name)
	if name == "" || name == "." || name == ".." {
		return "download"
	}
	return name
}

func drainClose(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 32*1024))
	_ = body.Close()
}
