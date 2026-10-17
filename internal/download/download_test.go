package download

import (
	"sync"
	"testing"
)

func testLimits() Limits {
	return Limits{MaxConnections: 16, HardCap: 32, MinChunk: 10 * 1024}
}

// Mirrors IDM FUN_005bad00: remaining/0x2800 clamped by user cap and 0x20.
func TestPlanConnectionsMatchesIDM(t *testing.T) {
	cases := []struct {
		name      string
		remaining int64
		want      int
	}{
		{"zero", 0, 1},
		{"negative", -100, 1},
		{"small tail single", 5000, 1},
		{"tail boundary 10240", 10240, 1},
		{"just over floor", 10241, 1},   // 10241/10240 = 1
		{"two chunks", 30000, 2},        // 30000/10240 = 2
		{"ten megabytes", 10 << 20, 16}, // 1024 units -> capped by user max 16
		{"huge", 100 << 30, 16},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := PlanConnections(c.remaining, testLimits())
			if got.Connections != c.want {
				t.Fatalf("PlanConnections(%d) = %d, want %d", c.remaining, got.Connections, c.want)
			}
		})
	}
}

func TestPlanConnectionsHardCap(t *testing.T) {
	lim := Limits{MaxConnections: 64, HardCap: 32, MinChunk: 10 * 1024}
	got := PlanConnections(100<<30, lim)
	if got.Connections != 32 {
		t.Fatalf("want hard cap 32, got %d", got.Connections)
	}
}

func TestPlanClamps(t *testing.T) {
	lim := testLimits()
	lim.IsMedia, lim.MediaCap = true, 4
	if got := PlanConnections(10<<20, lim).WithClamps(lim); got.Connections != 4 {
		t.Fatalf("media clamp: got %d want 4", got.Connections)
	}
	lim2 := testLimits()
	lim2.HostLimited, lim2.HostCap = true, 8
	if got := PlanConnections(10<<20, lim2).WithClamps(lim2); got.Connections != 8 {
		t.Fatalf("host clamp: got %d want 8", got.Connections)
	}
}

// Work stealing: a finished slot takes over the tiny tail of a busy segment
// StealTail recovers a span whose owner gave it back (released), which is the
// only safe case: it must never hand out a span another worker is streaming.
func TestStealTail(t *testing.T) {
	table := NewSegmentTable(1<<20, 10*1024, 2)

	seg1, err := table.Claim(1)
	if err != nil {
		t.Fatal(err)
	}
	seg2, err := table.Claim(2)
	if err != nil {
		t.Fatal(err)
	}

	// Conn 1 races ahead, leaving a 30-byte tail, then releases the span
	// (e.g. its connection died). Conn 2 finishes and goes idle.
	seg1.Advance(seg1.Remaining() - 30)
	table.Release(seg1.ID)
	if seg2.Advance(seg2.Remaining()) {
		seg2.MarkDone()
	}

	stolen, err := table.StealTail(2, 256*1024)
	if err != nil {
		t.Fatalf("StealTail: %v", err)
	}
	if stolen.ID != seg1.ID {
		t.Fatalf("stole segment %d, want %d", stolen.ID, seg1.ID)
	}
	if got := stolen.Remaining(); got != 30 {
		t.Fatalf("stolen tail = %d bytes, want 30", got)
	}

	if stolen.Advance(30) {
		stolen.MarkDone()
	}
	if !table.IsComplete() {
		t.Fatal("table should be complete")
	}
}

// A span still owned by a live worker must never be stolen: two writers on the
// same offsets would corrupt progress accounting.
func TestStealTailRefusesLiveOwner(t *testing.T) {
	table := NewSegmentTable(1<<20, 10*1024, 2)
	seg1, err := table.Claim(1)
	if err != nil {
		t.Fatal(err)
	}
	seg1.Advance(seg1.Remaining() - 30)
	if _, err := table.StealTail(2, 256*1024); err == nil {
		t.Fatal("StealTail must not steal a span with a live owner")
	}
}

// SplitFor grows the segment map at runtime: an idle connection carves the
// tail off the largest unowned span.
func TestSplitFor(t *testing.T) {
	const size = 4 << 20
	table := NewSegmentTable(size, 10*1024, 1)

	before := len(table.Segments())
	total := int64(0)
	for _, s := range table.Segments() {
		total += s.End - s.Start + 1
	}
	if total != size {
		t.Fatalf("initial coverage = %d, want %d", total, size)
	}

	tail, err := table.SplitFor(2, 256*1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(table.Segments()) != before+1 {
		t.Fatalf("segments = %d, want %d", len(table.Segments()), before+1)
	}
	total = 0
	for _, s := range table.Segments() {
		total += s.End - s.Start + 1
	}
	if total != size {
		t.Fatalf("post-split coverage = %d, want %d", total, size)
	}
	if tail.Remaining() <= 0 {
		t.Fatal("tail should carry work")
	}
}

// EnsureSpans widens the map so the pool always has claimable work.
func TestEnsureSpans(t *testing.T) {
	const size = 8 << 20
	table := NewSegmentTable(size, 10*1024, 1)

	table.EnsureSpans(6, 256*1024)
	if got := len(table.Segments()); got < 6 {
		t.Fatalf("segments = %d, want >= 6", got)
	}
	var total int64
	for _, s := range table.Segments() {
		total += s.End - s.Start + 1
	}
	if total != size {
		t.Fatalf("coverage = %d after widening, want %d", total, size)
	}
}

// Parallel claims must partition the file exactly once.
func TestClaimPartition(t *testing.T) {
	const size = 1 << 20
	table := NewSegmentTable(size, 10*1024, 8)
	_ = table

	var mu sync.Mutex
	covered := make(map[int64]int)
	var slots []int
	for i := 1; i <= 8; i++ {
		slots = append(slots, i)
	}
	for _, s := range slots {
		seg, err := table.Claim(s)
		if err != nil {
			continue
		}
		mu.Lock()
		for off := seg.Start; off <= seg.EndAt(); off++ {
			covered[off]++
		}
		mu.Unlock()
	}
	for off := int64(0); off < size; off++ {
		if covered[off] != 1 {
			t.Fatalf("byte %d covered %d times, want exactly 1", off, covered[off])
		}
	}
}

func TestGrowthStep(t *testing.T) {
	if got := GrowthStep(1, 5, true); got != 4 {
		t.Fatalf("burst 1->5 step = %d, want 4", got)
	}
	if got := GrowthStep(4, 5, true); got != 1 {
		t.Fatalf("burst 4->5 step = %d, want 1", got)
	}
	if got := GrowthStep(1, 5, false); got != 1 {
		t.Fatalf("additive step = %d, want 1", got)
	}
	if got := GrowthStep(5, 5, true); got != 0 {
		t.Fatalf("saturated step = %d, want 0", got)
	}
}
