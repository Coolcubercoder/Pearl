package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
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
		if err := d.fetchSegment(ctx, id, seg); err != nil {
			// Hand the range back so its remaining bytes are still described by
			// the checkpoint and can be picked up on the next run.
			d.co.release(seg)
			if ctx.Err() != nil {
				stat.setState(stateIdle)
				return ctx.Err()
			}
			stat.setState(stateFailed)
			return err
		}
		d.co.release(seg)
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
	if response.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("expected 206, got %s", response.Status)
	}

	stat.setState(stateDownloading)
	_, err = d.pipe.copy(cancel, response.Body, func(buf []byte) (int, error) {
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
