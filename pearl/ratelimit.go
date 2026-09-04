package main

import (
	"context"
	"io"
	"sync"
	"time"
)

// minLimiterSleep is the shortest pause a throttled worker takes. Sleeping for
// less than this burns more CPU waking up than it saves in precision, and the
// timer granularity underneath makes the extra accuracy imaginary anyway.
const minLimiterSleep = 5 * time.Millisecond

// rateLimiter is a token bucket shared by every worker in a download.
//
// It is deliberately one bucket for the whole transfer rather than one per
// connection: the number people want to cap is what pearl takes off the line
// in total, and a per-connection cap would silently multiply by -c. Sharing it
// also means the cap holds steady while work is stolen between connections and
// while mirrors are retired, because none of that changes how many tokens
// exist.
type rateLimiter struct {
	rate  float64 // bytes per second
	burst float64 // most bytes that can be spent in one go

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

// newRateLimiter caps a download at bytesPerSec, or returns nil -- meaning no
// limit at all -- when no cap was asked for. A nil *rateLimiter is usable: its
// wait is a no-op, so the unthrottled path costs a nil check rather than a
// lock.
func newRateLimiter(bytesPerSec int64, chunkSize int) *rateLimiter {
	if bytesPerSec <= 0 {
		return nil
	}
	// The bucket must hold at least one whole chunk. A worker that has just
	// read a chunk larger than the bucket could ever contain would otherwise
	// wait for tokens that can never accumulate, and the download would stall
	// forever rather than run slowly.
	burst := float64(bytesPerSec)
	if chunk := float64(chunkSize); burst < chunk {
		burst = chunk
	}
	return &rateLimiter{
		rate:  float64(bytesPerSec),
		burst: burst,
		// Starting full lets a short download finish at full speed instead of
		// paying a second of latency for a cap it was never going to reach.
		tokens: burst,
		last:   time.Now(),
	}
}

// wait blocks until n bytes' worth of allowance is available, then spends it.
// It returns early only if ctx is cancelled, so an interrupted download stops
// promptly instead of sleeping out the rest of its allowance.
func (l *rateLimiter) wait(ctx context.Context, n int) error {
	if l == nil || n <= 0 {
		return nil
	}
	need := float64(n)
	if need > l.burst {
		need = l.burst
	}

	for {
		l.mu.Lock()
		now := time.Now()
		l.tokens += now.Sub(l.last).Seconds() * l.rate
		l.last = now
		if l.tokens > l.burst {
			l.tokens = l.burst
		}
		if l.tokens >= need {
			l.tokens -= need
			l.mu.Unlock()
			return nil
		}
		pause := time.Duration((need - l.tokens) / l.rate * float64(time.Second))
		l.mu.Unlock()

		if pause < minLimiterSleep {
			pause = minLimiterSleep
		}
		timer := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// limitedReader paces a response body against a shared rateLimiter.
//
// The allowance is spent *after* a read rather than reserved before one,
// because bytes that have arrived cannot be un-received: by the time Read
// returns they are already off the network and in the socket buffer. Paying
// afterwards is what actually throttles the transfer -- the worker then stops
// reading for a while, the socket buffer fills, the receive window closes, and
// the server slows down, which is the same backpressure the pipeline depth is
// sized around. Reserving in advance would add latency without changing how
// fast anything arrived.
type limitedReader struct {
	src     io.Reader
	limiter *rateLimiter
	ctx     context.Context
}

// newLimitedReader wraps src, or hands it back untouched when there is no
// limit, so an unthrottled download keeps exactly the read path it had.
func newLimitedReader(ctx context.Context, src io.Reader, limiter *rateLimiter) io.Reader {
	if limiter == nil {
		return src
	}
	return &limitedReader{src: src, limiter: limiter, ctx: ctx}
}

func (r *limitedReader) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	if n > 0 {
		// A cancellation while paying is reported only if the read itself did
		// not already fail: the read's own error is the more useful one, and
		// the bytes it returned still need to reach the caller either way.
		if waitErr := r.limiter.wait(r.ctx, n); waitErr != nil && err == nil {
			err = waitErr
		}
	}
	return n, err
}
