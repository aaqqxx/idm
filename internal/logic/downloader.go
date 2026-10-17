package logic

import (
	"context"
	"fmt"
	"time"

	"github.com/maxuanquang/idm/internal/configs"
	"github.com/maxuanquang/idm/internal/dataaccess/file"
	"github.com/maxuanquang/idm/internal/download"
	"github.com/maxuanquang/idm/internal/utils"
	"go.uber.org/zap"
)

const (
	HTTPMetadataKeyContentType    = "Content-Type"
	HTTPResponseHeaderContentType = "Content-Type"

	MetadataKeyFileName     = "file-name"
	MetadataKeySize         = "size"
	MetadataKeySegments     = "segments"
	MetadataKeyConnections  = "peak-connections"
	MetadataKeyResumed      = "resumed"
	MetadataKeyElapsedMS    = "elapsed-ms"
	MetadataKeyRemoteName   = "remote-name"
	MetadataKeySupportsRage = "supports-range"
)

type Downloader interface {
	Download(ctx context.Context, fileName string) (map[string]any, error)
}

func NewHTTPDownloader(
	url string,
	downloadConfig configs.Download,
	fileClient file.Client,
	logger *zap.Logger,
) (Downloader, error) {
	return &httpDownloader{
		url:    url,
		cfg:    downloadConfig,
		files:  fileClient,
		logger: logger,
	}, nil
}

func NewFTPDownloader() (Downloader, error) {
	panic("unimplemented")
}

type httpDownloader struct {
	url    string
	cfg    configs.Download
	files  file.Client
	logger *zap.Logger
}

// Download implements Downloader.
//
// The segmented engine probes the server first (size, Range support, ETag),
// plans parallelism from the byte count, then drives a worker pool that
// claims, splits, and steals segments until the table completes. The payload
// lands in the file sink via random-access writes, so there is no join step.
func (h *httpDownloader) Download(ctx context.Context, fileName string) (map[string]any, error) {
	logger := utils.LoggerWithContext(ctx, h.logger).With(
		zap.String("url", h.url),
		zap.String("file_name", fileName),
	)

	eng := h.cfg.Engine.Normalize()

	var lastLog time.Time
	res, err := download.RunSession(ctx, download.SessionConfig{
		URL:          h.url,
		Engine:       download.EngineConfig{MaxConnections: eng.MaxConnections, HardCap: eng.HardCap},
		MinChunk:     eng.MinChunkSize,
		MinWorkUnit:  eng.MinWorkUnit,
		IdleTimeout:  mustParseDuration(eng.IdleTimeout, 30*time.Second),
		MaxRetries:   eng.MaxRetries,
		BufferSize:   eng.BufferSize,
		UserAgent:    eng.UserAgent,
		EnableResume: eng.EnableResume,
		StateDir:     eng.StateDirectory,
		DialTimeout:  mustParseDuration(eng.DialTimeout, 15*time.Second),

		MaxBytesPerSecond:  eng.MaxBytesPerSecond,
		NoOverwrite:        eng.NoOverwrite,
		DisablePreallocate: eng.DisablePreallocate,

		Request: download.RequestOptions{
			UserAgent: eng.UserAgent,
			Headers:   eng.Headers,
			Referer:   eng.Referer,
			Cookies:   eng.Cookies,
			Username:  eng.Username,
			Password:  eng.Password,
		},

		SinkFactory: func(ctx context.Context, _ string) (download.Sink, error) {
			return h.files.Sink(ctx, fileName)
		},
		Progress: func(p download.Progress) {
			now := time.Now()
			if now.Sub(lastLog) < 5*time.Second && p.Completed != p.Total {
				return
			}
			lastLog = now
			pct := 0.0
			if p.Total > 0 {
				pct = float64(p.Completed) / float64(p.Total) * 100
			}
			logger.Info("download progress",
				zap.Int64("completed", p.Completed),
				zap.Int64("total", p.Total),
				zap.Float64("percent", pct),
				zap.Int("active", p.Active),
				zap.Int("segments", p.Segments),
				zap.Int64("speed_bps", p.BytesPerS),
			)
		},
	})
	if err != nil {
		logger.With(zap.Error(err)).Error("segmented download failed")
		return nil, fmt.Errorf("download %s: %w", h.url, err)
	}

	logger.Info("download complete",
		zap.Int64("size", res.Result.Size),
		zap.Int("segments", res.Segments),
		zap.Int("peak_active", res.Result.PeakActive),
		zap.Int("retries", res.Result.Retries),
		zap.Duration("elapsed", res.Result.Elapsed),
		zap.Bool("resumed", res.Resumed),
	)

	return map[string]any{
		HTTPMetadataKeyContentType: res.Probe.ContentType,
		MetadataKeyFileName:        fileName,
		MetadataKeyRemoteName:      res.Probe.FileName,
		MetadataKeySize:            res.Result.Size,
		MetadataKeySegments:        res.Segments,
		MetadataKeyConnections:     res.Result.PeakActive,
		MetadataKeyResumed:         res.Resumed,
		MetadataKeyElapsedMS:       res.Result.Elapsed.Milliseconds(),
		MetadataKeySupportsRage:    res.Probe.SupportsRange,
	}, nil
}

func mustParseDuration(raw string, fallback time.Duration) time.Duration {
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}
