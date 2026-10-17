package download

import (
	"context"
	"sync"
	"time"
)

// Throttle is a token bucket shared by every connection of one transfer. A
// single shared bucket (rather than one per connection) is what makes the
// configured limit the *aggregate* speed, which is what a user asking for
// "max 2 MB/s" actually means.
//
// A nil Throttle is a no-op, so an unlimited transfer pays no cost.
type Throttle struct {
	mu       sync.Mutex
	rate     int64 // bytes per second; <= 0 means unlimited
	burst    int64 // bucket capacity in bytes
	tokens   float64
	lastFill time.Time
}

// NewThrottle builds a limiter for rate bytes/sec. A non-positive rate yields
// an unlimited limiter (nil), so callers can branch cheaply.
func NewThrottle(rate int64) *Throttle {
	if rate <= 0 {
		return nil
	}
	burst := rate / 4
	if burst < 32*1024 {
		burst = 32 * 1024
	}
	return &Throttle{
		rate:     rate,
		burst:    burst,
		tokens:   float64(burst),
		lastFill: time.Now(),
	}
}

// Limit reports the configured bytes/sec (0 when unlimited).
func (t *Throttle) Limit() int64 {
	if t == nil {
		return 0
	}
	return t.rate
}

// Take blocks until n tokens are available or ctx is done. It is called after
// a successful read so that a slow server does not stall the bucket logic.
func (t *Throttle) Take(ctx context.Context, n int) error {
	if t == nil || n <= 0 {
		return nil
	}
	// Honour cancellation even when tokens are available, so a cancelled
	// transfer stops promptly instead of draining the bucket first.
	if err := ctx.Err(); err != nil {
		return err
	}
	need := float64(n)
	if need > float64(t.burst) {
		need = float64(t.burst)
	}

	for {
		wait := t.reserve(need)
		if wait <= 0 {
			return nil
		}
		// Cap the sleep so a long wait still observes cancellation promptly.
		if wait > 250*time.Millisecond {
			wait = 250 * time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// reserve refills the bucket from elapsed time and consumes need tokens if
// possible. It returns how long the caller should wait before retrying.
func (t *Throttle) reserve(need float64) time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	if elapsed := now.Sub(t.lastFill).Seconds(); elapsed > 0 {
		t.tokens += elapsed * float64(t.rate)
		if t.tokens > float64(t.burst) {
			t.tokens = float64(t.burst)
		}
		t.lastFill = now
	}

	if t.tokens >= need {
		t.tokens -= need
		return 0
	}

	deficit := need - t.tokens
	secs := deficit / float64(t.rate)
	if secs <= 0 {
		return 0
	}
	return time.Duration(secs * float64(time.Second))
}
