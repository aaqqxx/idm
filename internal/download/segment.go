package download

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Segment is one contiguous byte span of the remote object. Its end is
// mutable: the scheduler shrinks it when a busy but slow span is split so an
// idle connection can take over the tail — the work-stealing behaviour
// observed in IDM's ChunksList::FindMaxFreeChunk.
//
// Every field that a worker mutates while the scheduler reads it is an atomic.
// The live type must never be copied by value; persistence uses the separate
// SegmentState projection.
type Segment struct {
	ID         int       `json:"id"`
	Start      int64     `json:"start"`
	End        int64     `json:"end"`
	Off        int64     `json:"-"`
	Retries    int       `json:"retries"`
	Owner      int       `json:"-"`
	StartedAt  time.Time `json:"-"` // mirror of started; use StartedAtTime
	FinishedAt time.Time `json:"-"` // mirror of finished; use FinishedAtTime

	end      atomic.Int64
	off      atomic.Int64
	done     atomic.Bool
	finished atomic.Int64 // UnixNano of completion; 0 until done
	started  atomic.Int64 // UnixNano when first claimed; 0 if never
}

// SegmentState is the plain, copyable projection of a Segment used for
// checkpointing and table restoration.
type SegmentState struct {
	ID         int       `json:"id"`
	Start      int64     `json:"start"`
	End        int64     `json:"end"`
	Off        int64     `json:"off"`
	Done       bool      `json:"done"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Retries    int       `json:"retries"`
}

// IsDone reports completion. Safe from any goroutine.
func (s *Segment) IsDone() bool { return s.done.Load() }

// MarkDone publishes completion exactly once.
func (s *Segment) MarkDone() {
	if s.done.CompareAndSwap(false, true) {
		s.finished.Store(time.Now().UnixNano())
	}
}

// FinishedAtTime returns the completion timestamp (zero if unfinished).
func (s *Segment) FinishedAtTime() time.Time {
	ns := s.finished.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// NewSegment builds a segment with the atomics initialised from values.
func NewSegment(id int, start, end int64) *Segment {
	s := &Segment{ID: id, Start: start, End: end, Off: start}
	s.end.Store(end)
	s.off.Store(start)
	return s
}

// State projects the live segment into a copyable value.
func (s *Segment) State() SegmentState {
	var started time.Time
	if ns := s.started.Load(); ns != 0 {
		started = time.Unix(0, ns)
	}
	return SegmentState{
		ID:         s.ID,
		Start:      s.Start,
		End:        s.end.Load(),
		Off:        s.off.Load(),
		Done:       s.done.Load(),
		StartedAt:  started,
		FinishedAt: s.FinishedAtTime(),
		Retries:    s.Retries,
	}
}

// MarkStarted records the first-claim time once.
func (s *Segment) MarkStarted() {
	s.started.CompareAndSwap(0, time.Now().UnixNano())
}

// StartedAtTime returns the first-claim timestamp (zero if never started).
func (s *Segment) StartedAtTime() time.Time {
	ns := s.started.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// AfterUnmarshal rehydrates atomics from plain fields, e.g. after loading a
// checkpoint.
func (s *Segment) AfterUnmarshal() {
	s.end.Store(s.End)
	s.off.Store(s.Off)
}

// Snapshot writes the atomic values back into the plain fields.
func (s *Segment) Snapshot() {
	s.End = s.end.Load()
	s.Off = s.off.Load()
}

// EndAt returns the current (possibly shrunk) inclusive end.
func (s *Segment) EndAt() int64 { return s.end.Load() }

// OffsetAt returns the next byte to fetch.
func (s *Segment) OffsetAt() int64 { return s.off.Load() }

// Advance records n freshly written bytes and reports whether the span
// completed.
func (s *Segment) Advance(n int64) bool {
	for {
		o := s.off.Load()
		e := s.end.Load()
		if o > e {
			return true
		}
		if s.off.CompareAndSwap(o, o+n) {
			return o+n > e
		}
	}
}

// Remaining is the number of bytes still to fetch for this segment.
func (s *Segment) Remaining() int64 {
	r := s.end.Load() - s.off.Load() + 1
	if r < 0 {
		return 0
	}
	return r
}

// Length is the full span size.
func (s *Segment) Length() int64 { return s.End - s.Start + 1 }

// Throughput estimates bytes/sec for a segment in flight.
func (s *Segment) Throughput(now time.Time) float64 {
	started := s.StartedAtTime()
	if started.IsZero() {
		return 0
	}
	end := now
	if fin := s.FinishedAtTime(); !fin.IsZero() {
		end = fin
	}
	elapsed := end.Sub(started).Seconds()
	if elapsed <= 0 {
		return 0
	}
	written := s.off.Load() - s.Start
	if written <= 0 {
		return 0
	}
	return float64(written) / elapsed
}

var (
	ErrNoWork      = errors.New("download: no remaining work")
	ErrSegmentDone = errors.New("download: segment already complete")
)

// SegmentTable is the shared, concurrency-safe ownership registry for all
// segments of one transfer. Every scheduler decision (claim, split, steal,
// completion) goes through it, which is what makes work stealing safe.
type SegmentTable struct {
	mu       sync.Mutex
	segments []*Segment
	size     int64

	nextID int

	minChunk int64
}

// NewSegmentTable builds a table covering [0, size) split into even spans.
// It never creates a span smaller than minChunk, which bounds the useful
// connection count exactly like IDM's 0x2800-byte floor.
func NewSegmentTable(size, minChunk int64, initial int) *SegmentTable {
	if minChunk <= 0 {
		minChunk = 10 * 1024
	}
	if initial <= 0 {
		initial = 1
	}
	t := &SegmentTable{size: size, minChunk: minChunk}

	maxSpans := int64(initial)
	if size/minChunk < maxSpans {
		maxSpans = size / minChunk
	}
	if maxSpans < 1 {
		maxSpans = 1
	}

	chunk := size / maxSpans
	if chunk < minChunk && size >= minChunk {
		chunk = minChunk
	}
	if chunk <= 0 {
		chunk = size
	}

	var id int
	var start int64
	for start < size {
		end := start + chunk - 1
		if end >= size-1 || size-end-1 < minChunk {
			end = size - 1
		}
		t.segments = append(t.segments, NewSegment(id, start, end))
		id++
		start = end + 1
	}
	t.nextID = id
	if len(t.segments) == 0 {
		t.segments = append(t.segments, NewSegment(0, 0, -1))
		t.segments[0].MarkDone()
		t.nextID = 1
	}
	return t
}

// RestoreSegmentTable rebuilds a table from persisted state.
func RestoreSegmentTable(size int64, minChunk int64, segs []SegmentState) *SegmentTable {
	t := &SegmentTable{size: size, minChunk: minChunk}
	for i := range segs {
		cp := NewSegment(segs[i].ID, segs[i].Start, segs[i].End)
		if segs[i].Done {
			cp.done.Store(true)
			cp.off.Store(cp.End + 1)
		} else {
			cp.off.Store(segs[i].Off)
			if cp.off.Load() < cp.Start {
				cp.off.Store(cp.Start)
			}
			if cp.off.Load() > cp.End+1 {
				cp.off.Store(cp.End + 1)
			}
		}
		cp.Owner = 0
		if !segs[i].StartedAt.IsZero() {
			cp.started.Store(segs[i].StartedAt.UnixNano())
		}
		if !segs[i].FinishedAt.IsZero() {
			cp.finished.Store(segs[i].FinishedAt.UnixNano())
		}
		cp.Retries = segs[i].Retries
		t.segments = append(t.segments, cp)
		if segs[i].ID >= t.nextID {
			t.nextID = segs[i].ID + 1
		}
	}
	return t
}

// Segments returns a copyable snapshot for checkpointing.
func (t *SegmentTable) Segments() []SegmentState {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := make([]SegmentState, len(t.segments))
	for i, s := range t.segments {
		out[i] = s.State()
	}
	return out
}

// Size is the total object size.
func (t *SegmentTable) Size() int64 { return t.size }

// CompletedBytes is the aggregate progress in bytes.
func (t *SegmentTable) CompletedBytes() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.completedBytesLocked()
}

func (t *SegmentTable) completedBytesLocked() int64 {
	var total int64
	for _, s := range t.segments {
		if s.IsDone() {
			total += s.End - s.Start + 1
			continue
		}
		total += s.off.Load() - s.Start
	}
	return total
}

// IsComplete reports whether every span has been fully fetched.
func (t *SegmentTable) IsComplete() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, s := range t.segments {
		if !s.IsDone() {
			return false
		}
	}
	return true
}

// Claim hands an idle connection the most valuable outstanding span. It
// prefers the largest remaining span, which reproduces IDM's "find the max
// free chunk" behaviour.
func (t *SegmentTable) Claim(slot int) (*Segment, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var best *Segment
	for _, s := range t.segments {
		if s.IsDone() {
			continue
		}
		if s.Owner != 0 && s.Owner != slot {
			continue
		}
		if best == nil || s.Remaining() > best.Remaining() {
			best = s
		}
	}
	if best == nil {
		return nil, ErrNoWork
	}
	best.Owner = slot
	best.MarkStarted()
	return best, nil
}

// SplitFor carves the tail off the largest outstanding span so an idle
// connection gets real work. This is the dynamic segmentation step: the span
// map grows at runtime instead of being fixed at start.
func (t *SegmentTable) SplitFor(slot int, minUnit int64) (*Segment, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var victim *Segment
	for _, s := range t.segments {
		if s.IsDone() {
			continue
		}
		// Never split a span another worker is actively streaming: shrinking
		// its End mid-flight would race with its writes. Only unowned spans or
		// the caller's own span are candidates.
		if s.Owner != 0 && s.Owner != slot {
			continue
		}
		if s.Remaining() < minUnit*2 {
			continue
		}
		if victim == nil || s.Remaining() > victim.Remaining() {
			victim = s
		}
	}
	if victim == nil {
		return nil, ErrNoWork
	}

	rem := victim.Remaining()
	half := rem / 2
	if half < minUnit {
		half = minUnit
	}
	// The tail starts here; the head keeps [Offset, splitAt-1].
	splitAt := victim.off.Load() + (rem - half)
	curEnd := victim.end.Load()
	if splitAt <= victim.off.Load() || splitAt > curEnd {
		return nil, ErrNoWork
	}

	tail := NewSegment(t.nextID, splitAt, curEnd)
	t.nextID++
	tail.Owner = slot
	tail.MarkStarted()

	victim.end.Store(splitAt - 1)
	t.segments = append(t.segments, tail)
	sort.SliceStable(t.segments, func(i, j int) bool { return t.segments[i].Start < t.segments[j].Start })

	return tail, nil
}

// EnsureSpans grows the span map so at least want spans are claimable. It
// only ever splits spans nobody is streaming, which keeps writes race-free.
// This is the dynamic-segmentation entry point: the map widens as the pool
// widens, instead of being fixed at start.
func (t *SegmentTable) EnsureSpans(want int, minUnit int64) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	splits := 0
	for t.claimableLocked() < want {
		victim := t.largestSplitCandidateLocked(minUnit)
		if victim == nil {
			break
		}
		rem := victim.Remaining()
		half := rem / 2
		if half < minUnit {
			half = minUnit
		}
		splitAt := victim.off.Load() + (rem - half)
		curEnd := victim.end.Load()
		if splitAt <= victim.off.Load() || splitAt > curEnd {
			break
		}
		tail := NewSegment(t.nextID, splitAt, curEnd)
		t.nextID++
		victim.end.Store(splitAt - 1)
		t.segments = append(t.segments, tail)
		splits++
	}
	if splits > 0 {
		sort.SliceStable(t.segments, func(i, j int) bool { return t.segments[i].Start < t.segments[j].Start })
	}
	return splits
}

func (t *SegmentTable) claimableLocked() int {
	n := 0
	for _, s := range t.segments {
		if s.IsDone() || s.Owner != 0 {
			continue
		}
		n++
	}
	return n
}

func (t *SegmentTable) largestSplitCandidateLocked(minUnit int64) *Segment {
	var victim *Segment
	for _, s := range t.segments {
		if s.IsDone() || s.Owner != 0 {
			continue
		}
		if s.Remaining() < minUnit*2 {
			continue
		}
		if victim == nil || s.Remaining() > victim.Remaining() {
			victim = s
		}
	}
	return victim
}

// StealTail hands an idle connection the remaining tail of a span that another
// connection has already *stopped* working on. It deliberately refuses spans
// whose owner is still live: two connections streaming the same offsets would
// double-count progress and leave holes, which is exactly the failure mode
// IDM avoids by reassigning bounds rather than letting writers overlap.
//
// In practice the only spans eligible here are ones an owner released after an
// error or a boundary change, so this is a cheap recovery path; the primary
// dynamic mechanism is SplitFor.
func (t *SegmentTable) StealTail(slot int, maxTail int64) (*Segment, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var victim *Segment
	for _, s := range t.segments {
		if s.IsDone() || s.Owner != 0 {
			continue
		}
		rem := s.Remaining()
		if rem <= 0 || rem > maxTail {
			continue
		}
		if victim == nil || rem < victim.Remaining() {
			victim = s
		}
	}
	if victim == nil {
		return nil, ErrNoWork
	}
	victim.Owner = slot
	return victim, nil
}

// Release frees a span without completing it so another connection can pick
// it up. Used when a connection errors out.
func (t *SegmentTable) Release(id int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, s := range t.segments {
		if s.ID == id {
			s.Owner = 0
			return
		}
	}
}

// BumpRetry increments the retry counter and reports whether the budget is
// still available.
func (t *SegmentTable) BumpRetry(id, max int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, s := range t.segments {
		if s.ID == id {
			s.Retries++
			return s.Retries <= max
		}
	}
	return false
}

// Requeue makes a segment claimable again after a recoverable failure.
func (t *SegmentTable) Requeue(id int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, s := range t.segments {
		if s.ID == id {
			s.Owner = 0
			s.done.Store(false)
			return
		}
	}
}
