package main

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseSize(t *testing.T) {
	good := map[string]int64{
		"512":     512,
		"512K":    512 << 10,
		"512k":    512 << 10,
		"4M":      4 << 20,
		"4MB":     4 << 20,
		"4MiB":    4 << 20,
		"1G":      1 << 30,
		" 2M ":    2 << 20,
		"1048576": 1 << 20,
	}
	for text, want := range good {
		got, err := parseSize(text)
		if err != nil {
			t.Fatalf("parseSize(%q): %v", text, err)
		}
		if got != want {
			t.Fatalf("parseSize(%q) = %d, want %d", text, got, want)
		}
	}

	for _, text := range []string{"", "B", "abc", "-1", "0", "4X", "M"} {
		if got, err := parseSize(text); err == nil {
			t.Fatalf("parseSize(%q) = %d, want an error", text, got)
		}
	}
}

// Buffers must come back with their full length, not the truncated length of
// whatever the last read happened to fill. A pool that handed back short
// buffers would silently shrink every subsequent read.
func TestBufferPoolReturnsFullLengthBuffers(t *testing.T) {
	pool := newBufferPool(defaultBufferSize)
	for i := 0; i < 32; i++ {
		buf := pool.get()
		if len(buf) != defaultBufferSize {
			t.Fatalf("got a %d-byte buffer, want %d", len(buf), defaultBufferSize)
		}
		pool.put(buf)
	}
}

// The pool is the only thing standing between a long download and a lot of
// garbage: every segment, retry and steal runs another copy.
func TestPipelineReusesBuffersAcrossCopies(t *testing.T) {
	pipe := newPipeline(minBufferSize, defaultPipelineDepth)
	source := make([]byte, minBufferSize*4)
	rand.New(rand.NewSource(5)).Read(source)

	allocations := testing.AllocsPerRun(20, func() {
		var sink bytes.Buffer
		_, err := pipe.copy(func() {}, bytes.NewReader(source), func(buf []byte) (int, error) {
			return sink.Write(buf)
		})
		if err != nil {
			t.Fatalf("copy: %v", err)
		}
	})
	// Without pooling this would allocate at least pipelineDepth buffers of
	// minBufferSize each per run; the exact count varies with the channels and
	// goroutines, so just assert we are nowhere near that.
	if allocations > 40 {
		t.Fatalf("%.0f allocations per copy suggests buffers are not being reused", allocations)
	}
}

func TestPipelineCopyRoundTripsData(t *testing.T) {
	for _, depth := range []int{2, 3, 8} {
		pipe := newPipeline(minBufferSize, depth)
		source := make([]byte, minBufferSize*3+1234)
		rand.New(rand.NewSource(int64(depth))).Read(source)

		var sink bytes.Buffer
		n, err := pipe.copy(func() {}, bytes.NewReader(source), func(buf []byte) (int, error) {
			return sink.Write(buf)
		})
		if err != nil {
			t.Fatalf("depth %d: copy: %v", depth, err)
		}
		if n != int64(len(source)) {
			t.Fatalf("depth %d: copied %d bytes, want %d", depth, n, len(source))
		}
		if !bytes.Equal(sink.Bytes(), source) {
			t.Fatalf("depth %d: copied data does not match", depth)
		}
	}
}

func TestPipelineCopyReportsWriteErrors(t *testing.T) {
	pipe := newPipeline(minBufferSize, defaultPipelineDepth)
	source := make([]byte, minBufferSize*4)
	boom := errors.New("disk on fire")

	cancelled := false
	_, err := pipe.copy(func() { cancelled = true }, bytes.NewReader(source),
		func(buf []byte) (int, error) { return 0, boom })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if !cancelled {
		t.Fatal("a write failure must cancel the request so the reader unblocks")
	}
}

func TestPipelineCopyPropagatesReadErrors(t *testing.T) {
	pipe := newPipeline(minBufferSize, defaultPipelineDepth)
	boom := errors.New("connection reset")
	src := io.MultiReader(bytes.NewReader(make([]byte, minBufferSize)), errReader{boom})

	written, err := pipe.copy(func() {}, src, func(buf []byte) (int, error) { return len(buf), nil })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	// The bytes that did arrive before the failure still count: they are on
	// disk, and the checkpoint has to know about them.
	if written != minBufferSize {
		t.Fatalf("written = %d, want %d", written, minBufferSize)
	}
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// Truncate alone leaves a sparse file, so the filesystem allocates blocks
// during the download instead of before it. Where the platform supports
// preallocation, the blocks should already be reserved.
func TestPreallocateReservesBlocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer file.Close()

	const size = 8 << 20
	if err := preallocate(file, size); err != nil {
		t.Skipf("preallocation unsupported here: %v", err)
	}
	if err := file.Truncate(size); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	info, err := file.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != size {
		t.Fatalf("size = %d, want %d", info.Size(), size)
	}
	// The file must still read back as zeroes: preallocation reserves space, it
	// does not put anything in it.
	if _, err := file.WriteAt([]byte("pearl"), size-5); err != nil {
		t.Fatalf("write at end: %v", err)
	}
	head := make([]byte, 16)
	if _, err := file.ReadAt(head, 0); err != nil {
		t.Fatalf("read head: %v", err)
	}
	if !bytes.Equal(head, make([]byte, 16)) {
		t.Fatal("preallocated space should read back as zeroes")
	}
}

func TestOpenOutputPreallocatesAndResumesWithoutTruncating(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")

	const size = 1 << 20
	file, err := openOutput(path, size, false)
	if err != nil {
		t.Fatalf("openOutput: %v", err)
	}
	if _, err := file.WriteAt([]byte("important"), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	file.Close()

	// Reopening for a resume must never discard what is already on disk.
	reopened, err := openOutput(path, size, true)
	if err != nil {
		t.Fatalf("openOutput resume: %v", err)
	}
	defer reopened.Close()

	got := make([]byte, 9)
	if _, err := reopened.ReadAt(got, 0); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != "important" {
		t.Fatalf("resume clobbered existing data, read %q", got)
	}
	info, err := reopened.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != size {
		t.Fatalf("size = %d, want %d", info.Size(), size)
	}
}

// checkpointFixture builds the minimum downloader the checkpoint loop needs:
// a real file to fsync, a coordinator to snapshot, and a meter to watch.
func checkpointFixture(t *testing.T) (*downloader, string) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "out.bin")
	file, err := openOutput(out, 1<<20, false)
	if err != nil {
		t.Fatalf("openOutput: %v", err)
	}
	t.Cleanup(func() { file.Close() })

	return &downloader{
		outputPath: out,
		statePath:  out + ".pearl",
		file:       file,
		totalSize:  1 << 20,
		sources:    newSourcePool(mirrorsFor("http://a.test/f"), 1),
		co:         newCoordinator(splitEvenly(1<<20, 2)),
		meter:      &atomic.Int64{},
	}, out + ".pearl"
}

func waitForFile(path string, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// An fsync costs in proportion to the dirty pages it flushes, so a fast
// transfer has to checkpoint on volume rather than waiting out the clock.
func TestCheckpointFiresOnBytesBeforeTheTimeCap(t *testing.T) {
	d, statePath := checkpointFixture(t)

	done := make(chan struct{})
	go d.checkpointLoop(done)
	defer close(done)

	// Let the loop take its baseline reading, but stay well inside the first
	// poll so no checkpoint has had a chance to fire yet.
	time.Sleep(checkpointPoll / 5)
	d.meter.Store(checkpointBytes + 1)

	// A time-paced loop could not possibly have checkpointed this early, so
	// seeing the file at all proves the byte trigger fired.
	if !waitForFile(statePath, checkpointInterval-(checkpointInterval/4)) {
		t.Fatal("enough bytes were written to warrant a checkpoint, but none was written")
	}
}

// A trickle still has to be checkpointed eventually, or a slow download that
// dies loses everything since it started.
func TestCheckpointStillFiresOnTimeWhenSlow(t *testing.T) {
	d, statePath := checkpointFixture(t)
	d.meter.Store(1024) // nowhere near the byte threshold

	done := make(chan struct{})
	go d.checkpointLoop(done)
	defer close(done)

	if waitForFile(statePath, checkpointInterval/2) {
		t.Fatal("checkpointed early: a trickle should wait for the time cap")
	}
	if !waitForFile(statePath, 2*checkpointInterval) {
		t.Fatal("a slow transfer was never checkpointed")
	}
}

func TestPipelineInFlightReportsTotalWindow(t *testing.T) {
	pipe := newPipeline(2<<20, 4)
	if got := pipe.inFlight(); got != 8<<20 {
		t.Fatalf("inFlight = %d, want %d", got, 8<<20)
	}
}
