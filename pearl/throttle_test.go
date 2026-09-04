package main

import (
	"bytes"
	"context"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseRetryAfterAcceptsSecondsAndDates(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		value string
		want  time.Duration
	}{
		{"delta seconds", "12", 12 * time.Second},
		{"padded", "  7  ", 7 * time.Second},
		{"zero means no wait", "0", 0},
		{"negative is nonsense", "-5", 0},
		{"missing", "", 0},
		{"garbage", "soon", 0},
		{"http date", now.Add(30 * time.Second).Format(http.TimeFormat), 30 * time.Second},
		{"http date in the past", now.Add(-time.Hour).Format(http.TimeFormat), 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := parseRetryAfter(c.value, now); got != c.want {
				t.Fatalf("parseRetryAfter(%q) = %v, want %v", c.value, got, c.want)
			}
		})
	}
}

func TestThrottleBackoffDoublesAndClamps(t *testing.T) {
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}
	for i, expected := range want {
		if got := throttleBackoff(i + 1); got != expected {
			t.Fatalf("throttleBackoff(%d) = %v, want %v", i+1, got, expected)
		}
	}
	// Past the cap the exponent must stop growing rather than shift its way to
	// a delay no download could outlive.
	if got := throttleBackoff(40); got != want[len(want)-1] {
		t.Fatalf("throttleBackoff(40) = %v, want it clamped to %v", got, want[len(want)-1])
	}
	if got := throttleBackoff(0); got != time.Second {
		t.Fatalf("throttleBackoff(0) = %v, want %v", got, time.Second)
	}
}

func TestThrottleStreakCountsPerMirror(t *testing.T) {
	pool := newSourcePool(mirrorsFor("http://a.example", "http://b.example"), 2)
	for want := 1; want <= 3; want++ {
		if got := pool.noteThrottle(0); got != want {
			t.Fatalf("noteThrottle(0) = %d, want %d", got, want)
		}
	}
	if got := pool.noteThrottle(1); got != 1 {
		t.Fatalf("a second mirror must keep its own streak, got %d", got)
	}
	// One clean range is better evidence than any cooldown we guessed at.
	pool.succeed(0)
	if got := pool.noteThrottle(0); got != 1 {
		t.Fatalf("success must clear the streak, got %d", got)
	}
}

func TestPickAvoidsAParkedMirror(t *testing.T) {
	pool := newSourcePool(mirrorsFor("http://a.example", "http://b.example"), 2)
	if !pool.park(0, time.Minute) {
		t.Fatal("parking a fresh mirror should report the sidelining as news")
	}
	if _, idx, ok := pool.pick(0); !ok || idx != 1 {
		t.Fatalf("pick moved to mirror %d (ok=%v), want the un-throttled mirror 1", idx, ok)
	}
	// A parked mirror is sidelined, never retired: it must still count as alive
	// so the failover path does not mistake throttling for a dead server.
	if pool.isDead(0) {
		t.Fatal("a rate-limited mirror must not be retired")
	}
	if pool.alive() != 2 {
		t.Fatalf("alive() = %d, want 2", pool.alive())
	}
}

func TestPickStillServesWhenEveryMirrorIsThrottled(t *testing.T) {
	pool := newSourcePool(mirrorsFor("http://a.example", "http://b.example"), 2)
	pool.park(0, time.Minute)
	pool.park(1, time.Hour)
	// Crawling is the right answer here; failing the download because every
	// server asked for less load is not.
	src, idx, ok := pool.pick(0)
	if !ok {
		t.Fatal("pick must not give up while live mirrors remain")
	}
	if idx != 0 {
		t.Fatalf("pick chose mirror %d (%s), want 0, whose cooldown expires first", idx, src.label)
	}
}

func TestSuccessUnparksAThrottledMirror(t *testing.T) {
	pool := newSourcePool(mirrorsFor("http://a.example", "http://b.example"), 2)
	pool.park(0, time.Hour)
	pool.succeed(0)
	if _, idx, _ := pool.pick(0); idx != 0 {
		t.Fatalf("a mirror that served a range should be back in rotation, got %d", idx)
	}
}

// throttlingServer answers 429 for the first `refusals` requests it sees, then
// serves ranges normally. retryAfter, when set, is sent as a Retry-After header.
func throttlingServer(t *testing.T, body []byte, refusals int32, retryAfter string) *httptest.Server {
	t.Helper()
	var seen atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if seen.Add(1) <= refusals {
			if retryAfter != "" {
				w.Header().Set("Retry-After", retryAfter)
			}
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		http.ServeContent(w, r, "f.bin", time.Time{}, bytes.NewReader(body))
	}))
	t.Cleanup(server.Close)
	return server
}

// runThrottled is runMultiSource with the downloader handed back, so a test can
// assert on what the run did to the mirror pool.
func runThrottled(t *testing.T, urls []string, size int64, workers int, timeout time.Duration) ([]byte, *downloader, error) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "out.bin")
	file, err := os.Create(out)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer file.Close()
	if err := file.Truncate(size); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	d := &downloader{
		clients:    newTCPClientSet(workers, defaultBufferSize),
		outputPath: out,
		statePath:  out + ".pearl",
		file:       file,
		totalSize:  size,
		retries:    5,
		sources:    newSourcePool(mirrorsFor(urls...), workers),
		pipe:       newPipeline(defaultBufferSize, defaultPipelineDepth),
		co:         newCoordinator(splitEvenly(size, workers)),
		meter:      &atomic.Int64{},
	}
	for i := 0; i < workers; i++ {
		stat := &workerStat{}
		stat.source.Store(-1)
		d.stats = append(d.stats, stat)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := d.run(ctx); err != nil {
		return nil, d, err
	}
	if left := d.co.remaining(); left != 0 {
		t.Fatalf("%d bytes left unfetched", left)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	return got, d, nil
}

func TestDownloadHonoursRetryAfter(t *testing.T) {
	content := make([]byte, 256<<10)
	rand.New(rand.NewSource(3)).Read(content)
	server := throttlingServer(t, content, 1, "1")

	started := time.Now()
	got, _, err := runThrottled(t, []string{server.URL}, int64(len(content)), 1, 30*time.Second)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	elapsed := time.Since(started)
	if !bytes.Equal(got, content) {
		t.Fatal("assembled file does not match the source content")
	}
	// The server asked for a second; taking less means the header was ignored.
	if elapsed < time.Second {
		t.Fatalf("download took %v, expected it to wait out the 1s Retry-After", elapsed)
	}
}

func TestRateLimitedMirrorIsThrottledNotRetired(t *testing.T) {
	content := make([]byte, 2<<20)
	rand.New(rand.NewSource(5)).Read(content)

	healthy := rangeServer(t, content)
	// A firewall that rate-limits everything, forever.
	limited := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer limited.Close()

	got, d, err := runThrottled(t, []string{healthy.URL, limited.URL}, int64(len(content)), 4, 30*time.Second)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("assembled file does not match the source content")
	}
	// The whole point of separating 429 from failure: a server asking for less
	// load has not proved itself broken, so it must survive the run.
	if d.sources.isDead(1) {
		t.Fatal("a rate-limiting mirror was retired; 429 must not count toward the failure streak")
	}
}

// A throttled worker must hand its range back before it sleeps, so an
// un-throttled connection can finish those bytes. If it slept holding the
// segment, this download would take as long as the backoff ladder (1+2+4+8+16s)
// rather than as long as the healthy mirror needs.
func TestThrottledWorkerReleasesItsSegment(t *testing.T) {
	content := make([]byte, 4<<20)
	rand.New(rand.NewSource(9)).Read(content)

	healthy := rangeServer(t, content)
	limited := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A long Retry-After: any worker that waits on this while holding work
		// stalls the download well past the deadline below.
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer limited.Close()

	started := time.Now()
	got, _, err := runThrottled(t, []string{healthy.URL, limited.URL}, int64(len(content)), 4, 25*time.Second)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("assembled file does not match the source content")
	}
	if elapsed := time.Since(started); elapsed > 20*time.Second {
		t.Fatalf("download took %v; a throttled worker appears to be holding its segment while sleeping", elapsed)
	}
}

// Cancelling must not have to sit out a server's Retry-After.
func TestThrottledWorkerStopsPromptlyOnCancel(t *testing.T) {
	content := make([]byte, 1<<20)
	limited := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "300")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer limited.Close()

	out := filepath.Join(t.TempDir(), "out.bin")
	file, err := os.Create(out)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer file.Close()
	size := int64(len(content))
	if err := file.Truncate(size); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	d := &downloader{
		clients:   newTCPClientSet(2, defaultBufferSize),
		file:      file,
		totalSize: size,
		retries:   2,
		sources:   newSourcePool(mirrorsFor(limited.URL), 2),
		pipe:      newPipeline(defaultBufferSize, defaultPipelineDepth),
		co:        newCoordinator(splitEvenly(size, 2)),
		meter:     &atomic.Int64{},
	}
	for i := 0; i < 2; i++ {
		stat := &workerStat{}
		stat.source.Store(-1)
		d.stats = append(d.stats, stat)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.run(ctx) }()

	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return promptly after cancellation; the throttle wait is not interruptible")
	}
}

// The single-stream path is what serves a file the server will not range, so
// it has no segment to hand back and no second connection to fall back on: all
// it can do is ask again later. These cover that it does.

func TestStreamWaitsOutA429AndCompletes(t *testing.T) {
	content := make([]byte, 512<<10)
	rand.New(rand.NewSource(17)).Read(content)
	server := throttlingServer(t, content, 2, "1")

	clients := newTCPClientSet(1, defaultBufferSize)
	defer clients.Close()
	out := filepath.Join(t.TempDir(), "out.bin")
	stat := &workerStat{}
	stat.source.Store(-1)
	meter := &atomic.Int64{}
	info := probeResult{url: server.URL, size: int64(len(content))}

	started := time.Now()
	err := streamToFile(context.Background(), clients.probeClient(), out, info,
		newPipeline(defaultBufferSize, defaultPipelineDepth), stat, meter, nil)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	// Two refusals at a second apiece: finishing sooner means Retry-After was
	// ignored, and failing at all means the 429 was treated as fatal.
	if elapsed := time.Since(started); elapsed < 2*time.Second {
		t.Fatalf("stream took %v, expected it to wait out two 1s Retry-After pauses", elapsed)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("streamed file does not match the source content")
	}
	if meter.Load() != int64(len(content)) {
		t.Fatalf("meter recorded %d bytes, want %d", meter.Load(), len(content))
	}
}

func TestAwaitStreamGivesUpWhenRateLimitedForever(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	clients := newTCPClientSet(1, defaultBufferSize)
	defer clients.Close()
	stat := &workerStat{}

	started := time.Now()
	response, err := awaitStream(context.Background(), clients.probeClient(), server.URL, stat, 2500*time.Millisecond)
	if err == nil {
		response.Body.Close()
		t.Fatal("a server that refuses forever must eventually be reported, not retried in silence")
	}
	// It must stop *before* sleeping past the bound, not after.
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("gave up after %v, expected it to stop around the 2.5s bound", elapsed)
	}
	if stat.state.Load() != stateFailed {
		t.Fatalf("worker state = %q, want failed", stateNames[stat.state.Load()])
	}
}

func TestAwaitStreamBacksOffWithoutARetryAfterHeader(t *testing.T) {
	var seen atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	clients := newTCPClientSet(1, defaultBufferSize)
	defer clients.Close()

	// The ladder is 1s then 2s, so a 2.5s bound allows exactly two attempts:
	// the third would sleep past it and is refused up front.
	if _, err := awaitStream(context.Background(), clients.probeClient(), server.URL, &workerStat{}, 2500*time.Millisecond); err == nil {
		t.Fatal("expected the stall bound to be reported")
	}
	if got := seen.Load(); got != 2 {
		t.Fatalf("server saw %d attempts, want 2 (a 1s then a 2s pause inside a 2.5s bound)", got)
	}
}

func TestAwaitStreamStopsPromptlyOnCancel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "300")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	clients := newTCPClientSet(1, defaultBufferSize)
	defer clients.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := awaitStream(ctx, clients.probeClient(), server.URL, &workerStat{}, time.Hour)
		done <- err
	}()

	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation to be reported")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("awaitStream did not return promptly after cancellation")
	}
}
