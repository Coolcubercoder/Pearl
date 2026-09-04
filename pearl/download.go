package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// defaultBufferSize is the chunk size for one network read and one disk
	// write. Bigger chunks mean proportionally fewer syscalls per byte.
	defaultBufferSize = 512 * 1024
	// Below the floor, per-syscall overhead starts to dominate; above the
	// ceiling a single connection holds more than any plausible
	// bandwidth-delay product calls for.
	minBufferSize = 64 << 10
	maxBufferSize = 64 << 20
	// defaultPipelineDepth buffers are kept in flight per worker so the network
	// read for chunk N+1 can proceed while chunk N is still being written to
	// disk, instead of the two alternating serially. Depth is also the slack
	// that absorbs a slow write: with none, a disk hiccup stops the reader,
	// the socket buffer fills, the receive window closes and the server
	// stalls.
	defaultPipelineDepth = 3
	// checkpointInterval caps how long a transfer can go without a checkpoint.
	// It is what paces a slow download, where little enough has been written
	// that flushing is cheap.
	checkpointInterval = 2 * time.Second
	// checkpointBytes caps how much can be written between checkpoints. An
	// fsync costs roughly in proportion to the dirty pages it has to flush, so
	// pacing purely on time makes the stall grow with throughput: at 500 MB/s a
	// two-second interval would flush a gigabyte in one go, and that stall
	// backs up the pipeline until the receive window closes. Pacing on bytes
	// keeps each individual flush bounded no matter how fast the transfer runs.
	checkpointBytes = 64 << 20
	// checkpointPoll is how often those two conditions are tested. It is not
	// how often anything is flushed.
	checkpointPoll = 250 * time.Millisecond
	// baseThrottleDelay is the pause after a mirror's first 429 that arrived
	// without a Retry-After header, and it doubles with each consecutive one.
	baseThrottleDelay = time.Second
	// maxThrottleDelay caps that doubling, and also caps whatever a server
	// asks for in Retry-After. A header is a hint from a machine we do not
	// control: a misconfigured edge node answering "Retry-After: 86400" must
	// slow the download down, not park it for a day with no way to tell that
	// anything is still alive.
	maxThrottleDelay = 60 * time.Second
	// throttlePoll is how often a paused worker checks whether the download
	// still needs it. It costs nothing while nothing is throttled.
	throttlePoll = 250 * time.Millisecond
	// maxStreamThrottleStall bounds how long the single-stream path will keep
	// waiting out 429s. That path has no ranges to resume from and no second
	// connection making progress in the meantime, so a server that keeps
	// refusing means the download is achieving nothing at all: at some point
	// saying so beats retrying in silence forever.
	maxStreamThrottleStall = 5 * time.Minute
)

// bufferPool recycles copy buffers across segments. Without it, every segment,
// every retry and every steal allocates a fresh set -- at half a megabyte
// apiece that is a steady stream of garbage for the collector to chase during
// the transfer, for buffers that are all identical and all short-lived.
type bufferPool struct {
	size int
	pool sync.Pool
}

func newBufferPool(size int) *bufferPool {
	p := &bufferPool{size: size}
	// Pooling a pointer rather than the slice itself avoids an allocation on
	// every Put to box the slice header into an interface.
	p.pool.New = func() any {
		buf := make([]byte, size)
		return &buf
	}
	return p
}

func (p *bufferPool) get() []byte  { return *(p.pool.Get().(*[]byte)) }
func (p *bufferPool) put(b []byte) { p.pool.Put(&b) }

// pipeline holds the copy parameters shared by every worker: how large each
// chunk is, and how many are kept in flight between network and disk.
type pipeline struct {
	pool  *bufferPool
	depth int
}

func newPipeline(chunkSize, depth int) *pipeline {
	return &pipeline{pool: newBufferPool(chunkSize), depth: depth}
}

// inFlight is how many bytes one connection can hold between the socket and
// the disk before the reader has to wait.
func (p *pipeline) inFlight() int64 { return int64(p.pool.size) * int64(p.depth) }

// errSegmentDone is a sentinel returned by the write callback to stop a
// transfer cleanly: either the segment is fully downloaded, or a thief took
// the rest of it and the remaining bytes on this connection are now useless.
var errSegmentDone = errors.New("segment complete")

var errInterrupted = errors.New("interrupted")

// throttleError reports that a mirror answered 429 Too Many Requests.
//
// It is carried up to runWorker rather than handled where it is detected,
// because the first thing a throttled worker owes everyone else is its range:
// waiting where the 429 was seen would hold a segment hostage for the whole
// penalty while other connections sat idle.
type throttleError struct {
	label  string        // mirror that pushed back, for the matrix
	hint   time.Duration // Retry-After, or 0 when the server sent none
	wait   time.Duration // what this worker actually pauses for
	parked bool          // the mirror has now been sidelined as throttled
}

func (e *throttleError) Error() string {
	if e.hint > 0 {
		return fmt.Sprintf("429 from %s, honouring Retry-After %s", e.label, e.wait.Round(time.Second))
	}
	return fmt.Sprintf("429 from %s, backing off %s", e.label, e.wait.Round(time.Second))
}

// parseRetryAfter reads a Retry-After header, returning 0 when there is no
// usable instruction in it and the caller should fall back to its own backoff.
//
// RFC 9110 allows the value to be either a delta-seconds count or an
// HTTP-date, and real CDNs send both, so an integer-only parser would silently
// ignore the servers that did bother to say how long to wait -- exactly the
// servers worth listening to. A date already in the past means "now", which is
// a hint to back off by the local minimum rather than to hammer immediately.
func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		if delay := when.Sub(now); delay > 0 {
			return delay
		}
	}
	return 0
}

// throttleWait sizes the pause after a 429: the server's own Retry-After when
// it sent one, and the doubling ladder when it did not. Either way the result
// is capped, because a header is a hint from a machine we do not control.
func throttleWait(hint time.Duration, streak int) time.Duration {
	wait := hint
	if wait <= 0 {
		wait = throttleBackoff(streak)
	}
	if wait > maxThrottleDelay {
		wait = maxThrottleDelay
	}
	return wait
}

// throttleBackoff is the pause after the n-th consecutive 429 from a mirror
// that sent no Retry-After: one second, then two, four, eight, sixteen. The
// exponent is clamped at maxThrottleRetries rather than left to run, because
// a streak that keeps growing would otherwise shift its way to a delay no
// download could outlive.
func throttleBackoff(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	if n > maxThrottleRetries {
		n = maxThrottleRetries
	}
	delay := baseThrottleDelay << uint(n-1)
	if delay > maxThrottleDelay {
		delay = maxThrottleDelay
	}
	return delay
}

type downloader struct {
	clients    *clientSet
	outputPath string
	statePath  string
	file       *os.File
	totalSize  int64
	retries    int

	sources *sourcePool
	pipe    *pipeline
	co      *coordinator
	stats   []*workerStat
	meter   *atomic.Int64
	// limits is shared by every worker so that -max-speed caps the download
	// rather than each connection. Nil when no cap was asked for.
	limits *rateLimiter
}

// chunk is a filled read buffer handed from the reader goroutine to the
// writer goroutine inside pipeline.copy.
type chunk struct {
	buf []byte
	n   int
}

// copy streams src through writeChunk, overlapping network reads
// with the (potentially slow) writeChunk call by running the reader and
// writer on separate goroutines connected by a small buffer pool. writeChunk
// is always called from a single goroutine, in order, so it is safe for it
// to mutate captured state (e.g. advancing a file offset) without locking.
// It returns the number of bytes writeChunk accepted, which can be less than
// the number read when a segment is truncated mid-flight by a steal.
// On a writeChunk error, cancel is invoked so a context-aware src (such as an
// HTTP response body) unblocks promptly instead of reading to completion.
func (p *pipeline) copy(cancel context.CancelFunc, src io.Reader, writeChunk func(buf []byte) (int, error)) (int64, error) {
	free := make(chan []byte, p.depth)
	// Buffers are borrowed for the life of this copy and returned together at
	// the end, by which point both goroutines below have finished with them.
	borrowed := make([][]byte, p.depth)
	for i := range borrowed {
		borrowed[i] = p.pool.get()
		free <- borrowed[i]
	}
	defer func() {
		for _, buf := range borrowed {
			p.pool.put(buf)
		}
	}()
	filled := make(chan chunk, p.depth)

	type writeResult struct {
		written int64
		err     error
	}
	results := make(chan writeResult, 1)

	go func() {
		var written int64
		for c := range filled {
			n, err := writeChunk(c.buf[:c.n])
			written += int64(n)
			if err != nil {
				cancel()
				// Keep recycling buffers back to free so the reader (which may
				// be blocked waiting for one) can observe the cancellation via
				// its next Read and unwind instead of deadlocking.
				for wc := range filled {
					free <- wc.buf
				}
				results <- writeResult{written, err}
				return
			}
			free <- c.buf
		}
		results <- writeResult{written, nil}
	}()

	var readErr error
	for {
		buf := <-free
		read, err := src.Read(buf)
		if read > 0 {
			filled <- chunk{buf: buf, n: read}
		} else {
			free <- buf
		}
		if err != nil {
			close(filled)
			if !errors.Is(err, io.EOF) {
				readErr = err
			}
			break
		}
	}

	result := <-results
	if result.err != nil {
		return result.written, result.err
	}
	return result.written, readErr
}

// run starts every worker and returns the first error any of them hit. The
// first failure cancels the rest so a doomed download stops promptly with a
// checkpoint rather than grinding through its remaining retries.
func (d *downloader) run(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	var workers sync.WaitGroup
	failures := make(chan error, len(d.stats))
	for id := range d.stats {
		workers.Add(1)
		go func(id int) {
			defer workers.Done()
			if err := d.runWorker(ctx, id); err != nil {
				failures <- err
				cancel()
			}
		}(id)
	}
	workers.Wait()
	close(failures)

	if parent.Err() != nil {
		return errInterrupted
	}
	return <-failures
}

// runWorker pulls ranges from the coordinator until there is nothing left to
// claim and nothing large enough to steal.
func (d *downloader) runWorker(ctx context.Context, id int) error {
	stat := d.stats[id]
	for {
		if ctx.Err() != nil {
			stat.setState(stateIdle)
			return ctx.Err()
		}
		stat.setState(stateStealing)
		seg := d.co.next(id)
		if seg == nil {
			stat.setState(stateDone)
			return nil
		}
		err := d.fetchSegment(ctx, id, seg)
		// Hand the range back however the attempt ended, so its remaining bytes
		// are still described by the checkpoint and can be picked up on the next
		// run. For a throttled worker this ordering is the whole point: the
		// range is back in the pool before the pause begins, so an un-throttled
		// connection can claim or steal it rather than waiting out a penalty
		// that was never charged to it.
		d.co.release(seg)
		if err == nil {
			continue
		}

		// A 429 is not a failed download, it is a server asking for less load.
		// The worker sits out its pause and then goes back for whatever work is
		// left -- which may be this same range, if nobody faster took it.
		var throttle *throttleError
		if errors.As(err, &throttle) {
			if waitErr := d.waitOutThrottle(ctx, id, throttle); waitErr != nil {
				stat.setState(stateIdle)
				return waitErr
			}
			continue
		}

		if ctx.Err() != nil {
			stat.setState(stateIdle)
			return ctx.Err()
		}
		stat.setState(stateFailed)
		return err
	}
}

// waitOutThrottle parks one worker for the length of its 429 penalty.
//
// The pause is a select on the context rather than a bare time.Sleep: a sleep
// cannot be woken, so a cancelled download -- Ctrl-C, or another worker's
// fatal error -- would have to sit out the server's full Retry-After before
// anything could stop, and the checkpoint it owes would be that late too.
func (d *downloader) waitOutThrottle(ctx context.Context, id int, throttle *throttleError) error {
	stat := d.stats[id]
	stat.setState(stateThrottled)
	stat.setNote(throttle)
	if throttle.parked {
		fmt.Fprintf(os.Stderr, "\npearl: mirror %s is rate-limiting, sidelining it for %s\n",
			throttle.label, throttle.wait.Round(time.Second))
	}
	deadline := time.NewTimer(throttle.wait)
	defer deadline.Stop()

	// Wake early once there is nothing left to fetch. run waits for every
	// worker, so without this a connection that drew a long Retry-After would
	// hold the whole download open well after the un-throttled mirrors had
	// written the last byte -- the transfer would be finished on disk and still
	// be sitting there waiting out somebody else's penalty.
	poll := time.NewTicker(throttlePoll)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return nil
		case <-poll.C:
			if d.co.remaining() == 0 {
				return nil
			}
		}
	}
}

// fetchSegment downloads one range, retrying on transport failures. Each
// retry resumes from the segment's current position, so a connection that
// dies at 90% only re-fetches the last 10%.
//
// The mirror is chosen per attempt rather than per segment: a failure retires
// the mirror once it has failed enough times in a row, and the next attempt
// then lands somewhere else. A range started against a dead mirror therefore
// finishes against a live one, keeping whatever bytes it already wrote.
func (d *downloader) fetchSegment(ctx context.Context, id int, seg *segment) error {
	stat := d.stats[id]
	var lastErr error
	for attempt := 0; attempt <= d.retries; attempt++ {
		if attempt > 0 {
			stat.setState(stateRetrying)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff(attempt)):
			}
		}
		pos, end := d.co.window(seg)
		if pos > end {
			return nil
		}

		src, idx, ok := d.sources.pick(id)
		if !ok {
			return fmt.Errorf("range %d-%d: every mirror failed: %w", seg.start, seg.end, lastErr)
		}
		stat.setSource(idx)

		stat.setState(stateConnecting)
		err := d.fetchOnce(ctx, id, seg, pos, end, src)
		if err == nil {
			d.sources.succeed(idx)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Rate limiting is deliberately kept out of the failure streak that
		// retires a mirror. A firewall that answers 429 on every connection
		// would otherwise retire each mirror in turn and fail a download that
		// was only ever being asked to slow down -- and retiring mirrors makes
		// that worse, by concentrating the same load on the ones still standing.
		var throttle *throttleError
		if errors.As(err, &throttle) {
			streak := d.sources.noteThrottle(idx)
			throttle.wait = throttleWait(throttle.hint, streak)
			// Past the retry cap this mirror has proved it means it, so it is
			// sidelined for the length of the pause and pick steers other
			// workers to mirrors that are still answering.
			if streak >= maxThrottleRetries {
				throttle.parked = d.sources.park(idx, throttle.wait)
			}
			stat.setNote(throttle)
			return throttle
		}

		if d.sources.fail(idx) {
			fmt.Fprintf(os.Stderr, "\npearl: dropping mirror %s after %d consecutive failures (%v)\n",
				src.label, maxSourceFailures, err)
		}
		lastErr = err
		stat.setNote(err)
	}
	return fmt.Errorf("range %d-%d: %w", seg.start, seg.end, lastErr)
}

func backoff(attempt int) time.Duration {
	delay := 250 * time.Millisecond * (1 << uint(attempt-1))
	if delay > 5*time.Second {
		delay = 5 * time.Second
	}
	return delay
}

func (d *downloader) fetchOnce(ctx context.Context, id int, seg *segment, start, end int64, src source) error {
	stat := d.stats[id]
	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	request, err := http.NewRequestWithContext(reqCtx, http.MethodGet, src.url, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Range", "bytes="+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10))
	// If-Range makes the server refuse the partial response outright when the
	// file has changed, instead of silently splicing new bytes into our old
	// ones. We only accept 206, so a changed file surfaces as an error rather
	// than a corrupt output. The validator has to be the one this mirror
	// issued: an ETag from a different server is not a weaker check, it is a
	// meaningless one.
	if src.validator.etag != "" {
		request.Header.Set("If-Range", src.validator.etag)
	} else if src.validator.lastModified != "" {
		request.Header.Set("If-Range", src.validator.lastModified)
	}

	response, err := d.clients.forWorker(id).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	// Stop before a single byte is copied. Nothing has been read yet, so the
	// segment's position is untouched and the range is clean to hand back --
	// which is what lets the pause happen with the work released rather than
	// held.
	if response.StatusCode == http.StatusTooManyRequests {
		return &throttleError{
			label: src.label,
			hint:  parseRetryAfter(response.Header.Get("Retry-After"), time.Now()),
		}
	}
	if response.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("expected 206, got %s", response.Status)
	}

	stat.setState(stateDownloading)
	body := newLimitedReader(reqCtx, response.Body, d.limits)
	_, err = d.pipe.copy(cancel, body, func(buf []byte) (int, error) {
		// Reserve on every chunk rather than trusting the range we asked for:
		// another worker may have stolen this segment's tail since the last
		// chunk, in which case these bytes are already someone else's job.
		pos, allowed := d.co.begin(seg, int64(len(buf)))
		if allowed <= 0 {
			return 0, errSegmentDone
		}
		buf = buf[:allowed]
		if _, err := d.file.WriteAt(buf, pos); err != nil {
			d.co.abort(seg)
			return 0, err
		}
		exhausted := d.co.commit(seg, allowed)
		stat.bytes.Add(allowed)
		d.meter.Add(allowed)
		if exhausted {
			// Nothing left in this segment: either it is finished or the rest
			// of it now belongs to a thief. Either way this connection is done.
			return int(allowed), errSegmentDone
		}
		return int(allowed), nil
	})

	if err != nil && !errors.Is(err, errSegmentDone) {
		return err
	}
	if pos, end := d.co.window(seg); pos <= end {
		return fmt.Errorf("connection closed with %s missing", humanBytes(end-pos+1))
	}
	return nil
}

// checkpointLoop keeps the .pearl file roughly in step with what is on disk
// for the whole life of the download, so even a hard kill loses at most one
// interval of work.
func (d *downloader) checkpointLoop(done <-chan struct{}) {
	ticker := time.NewTicker(checkpointPoll)
	defer ticker.Stop()
	lastAt, lastBytes := time.Now(), d.meter.Load()
	for {
		select {
		case <-ticker.C:
			// Flush on whichever comes first: enough new bytes that the fsync is
			// getting expensive, or enough time that a slow transfer still gets
			// checkpointed. Either way the amount of work at risk stays bounded.
			written := d.meter.Load()
			if written-lastBytes < checkpointBytes && time.Since(lastAt) < checkpointInterval {
				continue
			}
			d.syncAndCheckpoint()
			lastAt, lastBytes = time.Now(), written
		case <-done:
			return
		}
	}
}

// awaitStream issues a plain, unranged GET and waits out any 429 the server
// answers it with, returning the first response that is not a rate-limit
// refusal.
//
// The ranged path handles a 429 by handing its segment back so another
// connection can take it. This path has neither: one stream, no ranges, and
// nothing to resume from, so the only thing it can do is ask again later --
// which makes the give-up bound the important part. Retrying a single refused
// stream forever would look exactly like a hung download.
func awaitStream(ctx context.Context, client *http.Client, url string, stat *workerStat, stall time.Duration) (*http.Response, error) {
	deadline := time.Now().Add(stall)
	for streak := 1; ; streak++ {
		stat.setState(stateConnecting)
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		response, err := client.Do(request)
		if err != nil {
			return nil, err
		}
		if response.StatusCode != http.StatusTooManyRequests {
			return response, nil
		}
		// Nothing in a 429 body is worth reading, and the next attempt wants a
		// clean connection rather than an undrained one.
		response.Body.Close()

		throttle := &throttleError{
			label: shortHost(url),
			hint:  parseRetryAfter(response.Header.Get("Retry-After"), time.Now()),
		}
		throttle.wait = throttleWait(throttle.hint, streak)
		// Stop before sleeping past the bound rather than after: waiting out a
		// pause we already know is pointless helps nobody.
		if time.Now().Add(throttle.wait).After(deadline) {
			stat.setState(stateFailed)
			stat.setNote(throttle)
			return nil, fmt.Errorf("rate limited with no progress for %s: %w", stall, throttle)
		}
		stat.setState(stateThrottled)
		stat.setNote(throttle)

		// Interruptible, for the same reason the ranged path is: a cancelled
		// download must not have to sit out the server's Retry-After first.
		timer := time.NewTimer(throttle.wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
