package download

import (
	"sync/atomic"
	"time"
)

// Throughput tracks the aggregate transfer rate with an exponentially weighted
// moving average, the same role as IDM's Session::CalcSpeed. The EWMA smooths
// the bursty per-tick samples so the growth controller does not oscillate.
type Throughput struct {
	bits int64 // cumulative completed bytes, updated on every write batch

	lastTick time.Time
	lastSeen int64

	// ewma is bytes/sec, scaled by 1e3 to stay integral under atomic ops.
	ewma atomic.Int64
}

// Add records n freshly written bytes.
func (t *Throughput) Add(n int64) {
	t.bits += n // called only from the single progress goroutine
}

// Tick samples the rate. Call it on a fixed cadence from the progress loop.
func (t *Throughput) Tick(now time.Time) {
	if t.lastTick.IsZero() {
		t.lastTick = now
		t.lastSeen = t.bits
		return
	}
	dt := now.Sub(t.lastTick).Seconds()
	if dt <= 0 {
		return
	}
	delta := t.bits - t.lastSeen
	inst := float64(delta) / dt
	t.lastTick = now
	t.lastSeen = t.bits

	prev := float64(t.ewma.Load()) / 1e3
	const alpha = 0.35
	next := alpha*inst + (1-alpha)*prev
	t.ewma.Store(int64(next * 1e3))
}

// BytesPerSec returns the smoothed aggregate rate.
func (t *Throughput) BytesPerSec() float64 {
	return float64(t.ewma.Load()) / 1e3
}

// desiredConns mirrors the "Optional connections" controller. Given the
// outstanding bytes it asks PlanConnections what the download wants; the
// caller then grows or shrinks the live worker pool toward that target.
func desiredConns(remaining int64, lim Limits) int {
	return PlanConnections(remaining, lim).WithClamps(lim).Connections
}

// burstAllowed reproduces the FUN_005b1970 gate: a fast handshake (latency
// under ~1.1s) authorises multiplicative growth, otherwise the pool ramps
// additively. probeMS is the measured probe round-trip in milliseconds.
func burstAllowed(probeMS int64) bool {
	return probeMS < 1100
}

// GrowthStep decides how many slots to add this tick. Fast handshakes burst
// (up to x5, like the disassembly's base*5), slow ones add one at a time so a
// congested server is not hammered during ramp-up.
func GrowthStep(active, desired int, burst bool) int {
	if active >= desired {
		return 0
	}
	if burst {
		step := active * 4 // active + step == active*5
		if step < 1 {
			step = desired - active
		}
		if active+step > desired {
			step = desired - active
		}
		return step
	}
	return 1
}
