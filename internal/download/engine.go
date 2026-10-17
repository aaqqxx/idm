package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Options configures a segmented transfer.
type Options struct {
	URL  string
	Size int64 // expected payload size; 0 means unknown until probe

	// Connections is the initial parallelism. Zero means "decide from size".
	Connections int

	// Engine carries the connection-planning ceilings.
	Engine EngineConfig

	// MinChunk is the per-connection work floor.
	MinChunk int64

	// MinWorkUnit is the smallest slice a split may create.
	MinWorkUnit int64

	// IdleTimeout aborts a connection stalled longer than this.
	IdleTimeout time.Duration

	// MaxRetries bounds per-segment retries.
	MaxRetries int

	// BufferSize is the per-connection copy buffer.
	BufferSize int

	// UserAgent for range requests.
	UserAgent string

	// Request customises headers, Referer, cookies, and Basic Auth on every
	// range request.
	Request RequestOptions

	// Throttle caps the aggregate transfer rate. Nil means unlimited.
	Throttle *Throttle

	// ProbeMS is the measured handshake round-trip in milliseconds. A fast
	// probe authorises a multiplicative burst when widening the pool; a slow
	// one ramps additively. Zero is treated as "fast".
	ProbeMS int64

	// Progress, when set, receives per-segment completion callbacks.
	Progress func(Progress)

	// ResumeFrom, when non-nil, seeds the segment table.
	ResumeFrom *SegmentTable
}

// Progress reports aggregate state to callers (UI, logs, metrics).
type Progress struct {
	Completed int64
	Total     int64
	Active    int
	Segments  int
	BytesPerS int64
}

// Result summarises a completed transfer.
type Result struct {
	Size       int64
	Segments   int
	Elapsed    time.Duration
	PeakActive int
	Retries    int
}

// ErrAlreadyComplete signals the table had nothing left to do.
var ErrAlreadyComplete = errors.New("download: transfer already complete")

// errSegmentReassigned means the scheduler split the span out from under the
// connection. It is a normal control-flow signal, not a failure.
var errSegmentReassigned = errors.New("download: segment reassigned")

// Engine executes one segmented HTTP transfer. It owns the segment table, the
// worker pool, and the adaptive growth loop �?the same three pieces IDM splits
// across Session, ChunksList, and the optional-connection controller.
type Engine struct {
	opts   Options
	client *http.Client
	table  *SegmentTable
	sink   Sink

	written atomic.Int64

	mu         sync.Mutex
	active     int
	peakActive int
	retries    int

	stopCh  chan struct{}
	stopOne sync.Once
}

// NewEngine wires an engine over an existing sink.
func NewEngine(opts Options, client *http.Client, sink Sink) (*Engine, error) {
	if opts.URL == "" {
		return nil, errors.New("download: empty URL")
	}
	if client == nil {
		client = http.DefaultClient
	}
	if opts.MinChunk <= 0 {
		opts.MinChunk = 10 * 1024
	}
	if opts.MinWorkUnit < opts.MinChunk {
		opts.MinWorkUnit = 256 * 1024
	}
	if opts.BufferSize <= 0 {
		opts.BufferSize = 128 * 1024
	}
	if opts.IdleTimeout <= 0 {
		opts.IdleTimeout = 30 * time.Second
	}
	if opts.MaxRetries <= 0 {
		opts.MaxRetries = 5
	}
	return &Engine{
		opts:   opts,
		client: client,
		sink:   sink,
		stopCh: make(chan struct{}),
	}, nil
}

// Table exposes the segment table for checkpointing.
func (e *Engine) Table() *SegmentTable { return e.table }

// Seed installs a resumed table.
func (e *Engine) Seed(t *SegmentTable) { e.table = t }

// Run drives the transfer to completion or context cancellation.
func (e *Engine) Run(ctx context.Context, size int64) (Result, error) {
	start := time.Now()

	if e.table == nil {
		initial := e.opts.Connections
		if initial <= 0 {
			initial = PlanConnections(size, e.limits()).WithClamps(e.limits()).Connections
		}
		e.table = NewSegmentTable(size, e.opts.MinChunk, initial)
	}
	if e.table.IsComplete() {
		return Result{Size: size, Segments: 1, Elapsed: time.Since(start)}, ErrAlreadyComplete
	}
	if err := e.sink.Truncate(size); err != nil {
		return Result{}, err
	}

	desired := desiredConns(e.remaining(), e.limits())

	var wg sync.WaitGroup
	// spawnTo widens the pool toward want. A fast handshake bursts (up to x5),
	// a slow one ramps one slot at a time so a congested server is not
	// hammered during ramp-up. Workers never exit while work remains.
	burst := burstAllowed(e.opts.ProbeMS)
	live := 0
	spawnTo := func(want int) {
		for live < want {
			step := GrowthStep(live, want, burst)
			if step <= 0 {
				step = 1
			}
			for i := 0; i < step && live < want; i++ {
				e.spawn(ctx, &wg, live+1)
				live++
			}
		}
	}
	spawnTo(desired)

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	var tp Throughput
	for {
		select {
		case <-ctx.Done():
			e.stop()
			wg.Wait()
			return Result{}, ctx.Err()
		case <-e.stopCh:
			wg.Wait()
			return e.finish(start, size, live)
		case <-ticker.C:
			tp.Tick(time.Now())
			if e.opts.Progress != nil {
				e.opts.Progress(Progress{
					Completed: e.table.CompletedBytes(),
					Total:     size,
					Active:    e.activeCount(),
					Segments:  len(e.table.Segments()),
					BytesPerS: int64(tp.BytesPerSec()),
				})
			}
			if e.table.IsComplete() {
				e.stop()
				wg.Wait()
				return e.finish(start, size, live)
			}
			// Decide how wide the transfer should be now, widen the span map so
			// there is a claimable piece per worker, then grow the pool.
			want := desiredConns(e.remaining(), e.limits())
			e.table.EnsureSpans(want, e.opts.MinWorkUnit)
			spawnTo(want)
		}
	}
}

func (e *Engine) finish(start time.Time, size int64, spawned int) (Result, error) {
	if err := e.sink.Commit(context.Background()); err != nil {
		return Result{}, err
	}
	return Result{
		Size:       size,
		Segments:   len(e.table.Segments()),
		Elapsed:    time.Since(start),
		PeakActive: e.peakActive,
		Retries:    e.retries,
	}, nil
}

func (e *Engine) limits() Limits {
	return Limits{
		MaxConnections: e.opts.Engine.MaxConnections,
		HardCap:        e.opts.Engine.HardCap,
		MinChunk:       e.opts.MinChunk,
		MediaCap:       e.opts.Engine.MediaCap,
		HostCap:        e.opts.Engine.HostCap,
		IsMedia:        e.opts.Engine.IsMedia,
		HostLimited:    e.opts.Engine.HostLimited,
	}
}

func (e *Engine) remaining() int64 {
	return e.table.Size() - e.table.CompletedBytes()
}

func (e *Engine) activeCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.active
}

func (e *Engine) noteActive(delta int) {
	e.mu.Lock()
	e.active += delta
	if e.active > e.peakActive {
		e.peakActive = e.active
	}
	e.mu.Unlock()
}

func (e *Engine) noteRetry() {
	e.mu.Lock()
	e.retries++
	e.mu.Unlock()
}

func (e *Engine) stop() {
	e.stopOne.Do(func() { close(e.stopCh) })
}

func (e *Engine) stopped() bool {
	select {
	case <-e.stopCh:
		return true
	default:
		return false
	}
}

// spawn starts one worker bound to slot.
func (e *Engine) spawn(ctx context.Context, wg *sync.WaitGroup, slot int) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		e.worker(ctx, slot)
	}()
}

// worker is the per-connection loop. It claims work, fetches it, and on
// completion tries to steal a neighbour's tail before going idle. That last
// step is what keeps a straggler from dominating wall-clock time.
//
// A worker only returns when the transfer is complete, cancelled, or stopped.
// If it temporarily finds nothing claimable (every span is owned and none is
// small enough to steal), it waits briefly instead of exiting, so a span that
// a slower peer releases later still gets picked up.
func (e *Engine) worker(ctx context.Context, slot int) {
	for {
		if ctx.Err() != nil || e.stopped() {
			return
		}

		seg, err := e.table.Claim(slot)
		if err != nil {
			// Nothing unowned. Try to take over a slow neighbour's small tail.
			if tail, terr := e.table.StealTail(slot, e.opts.MinWorkUnit); terr == nil {
				seg = tail
			} else {
				if e.table.IsComplete() {
					e.stop()
					return
				}
				// Everything is owned by peers right now. Wait a beat rather
				// than exiting, otherwise the remaining span would be orphaned
				// the moment its owner finishes and no worker takes over.
				select {
				case <-ctx.Done():
					return
				case <-e.stopCh:
					return
				case <-time.After(25 * time.Millisecond):
				}
				continue
			}
		}

		if err := e.fetchSegment(ctx, slot, seg); err != nil {
			if err == errSegmentReassigned {
				// The span was split and the tail belongs to another worker.
				// Nothing to retry; go find fresh work.
				continue
			}
			if ctx.Err() != nil || e.stopped() {
				return
			}
			e.noteRetry()
			e.table.Release(seg.ID)
			if !e.table.BumpRetry(seg.ID, e.opts.MaxRetries) {
				// Budget exhausted; leave it for another worker but do not
				// spin. A slow retry delay avoids a hot loop.
				select {
				case <-ctx.Done():
					return
				case <-e.stopCh:
					return
				case <-time.After(time.Second):
				}
				e.table.Requeue(seg.ID)
			}
			continue
		}

		if e.table.IsComplete() {
			e.stop()
			return
		}
	}
}

// fetchSegment issues a Range request for the outstanding part of seg and
// streams bytes into the sink at their absolute offset. It aborts when no
// bytes arrive within IdleTimeout, matching IDM's 30-second stall watchdog.
func (e *Engine) fetchSegment(ctx context.Context, slot int, seg *Segment) error {
	start := seg.OffsetAt()
	end := seg.EndAt()
	if start > end {
		seg.MarkDone()
		return nil
	}

	segCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	lastByte := time.Now()
	var stalled atomic.Bool

	watchdog := time.AfterFunc(e.opts.IdleTimeout, func() {
		if time.Since(lastByte) >= e.opts.IdleTimeout {
			stalled.Store(true)
			cancel()
		}
	})
	defer watchdog.Stop()

	req, err := http.NewRequestWithContext(segCtx, http.MethodGet, e.opts.URL, http.NoBody)
	if err != nil {
		return err
	}
	// Apply custom headers/auth, then set the Range the engine owns. Order
	// matters: apply() never sets Range, so the engine's boundary always wins.
	ua := e.opts.Request.UserAgent
	if ua == "" {
		ua = e.opts.UserAgent
	}
	reqOpts := e.opts.Request
	reqOpts.UserAgent = ua
	reqOpts.apply(req)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	e.noteActive(1)
	defer e.noteActive(-1)

	resp, err := e.client.Do(req)
	if err != nil {
		if stalled.Load() {
			return fmt.Errorf("download: segment %d stalled at %d", seg.ID, seg.OffsetAt())
		}
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download: segment %d got %s", seg.ID, resp.Status)
	}

	buf := make([]byte, e.opts.BufferSize)
	for {
		if segCtx.Err() != nil {
			if stalled.Load() {
				return fmt.Errorf("download: segment %d stalled", seg.ID)
			}
			return segCtx.Err()
		}

		// The span's end may shrink underneath us when the scheduler splits it
		// for an idle connection. Re-read the live end every iteration and
		// never read past it, so a shrinking span stops exactly at its new
		// boundary instead of overwriting the split-off tail.
		curEnd := seg.EndAt()
		off := seg.OffsetAt()
		if off > curEnd {
			// Our whole remainder was reassigned. Hand the span back without
			// marking it done: the new owner will finish it.
			return errSegmentReassigned
		}
		// Clamp the read to the current boundary so we never write past it.
		maxRead := curEnd - off + 1
		if maxRead <= 0 {
			return errSegmentReassigned
		}
		readBuf := buf
		if int64(len(readBuf)) > maxRead {
			readBuf = buf[:maxRead]
		}

		n, rerr := resp.Body.Read(readBuf)
		if n > 0 {
			lastByte = time.Now()
			watchdog.Reset(e.opts.IdleTimeout)

			if _, werr := e.sink.WriterAt().WriteAt(readBuf[:n], off); werr != nil {
				return fmt.Errorf("download: write segment %d: %w", seg.ID, werr)
			}
			e.written.Add(int64(n))
			// Throttle after the write: the bucket then reflects bytes that
			// actually landed, and a stalled server never burns tokens.
			if takeErr := e.opts.Throttle.Take(segCtx, n); takeErr != nil {
				return takeErr
			}
			if seg.Advance(int64(n)) {
				seg.MarkDone()
				return nil
			}
			// If the boundary moved while we were reading, stop cleanly here.
			if seg.OffsetAt() >= seg.EndAt()+1 {
				seg.MarkDone()
				return nil
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				// EOF before the requested span ended: treat as a short read
				// and let the retry path re-request the remainder.
				if seg.OffsetAt() <= seg.EndAt() {
					return fmt.Errorf("download: segment %d short read at %d/%d", seg.ID, seg.OffsetAt(), seg.EndAt())
				}
				seg.MarkDone()
				return nil
			}
			if stalled.Load() {
				return fmt.Errorf("download: segment %d stalled", seg.ID)
			}
			return rerr
		}
	}
}
