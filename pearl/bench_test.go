package main

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// benchServer serves a body over loopback with no throttling at all, so the
// benchmark measures what pearl itself costs rather than what a network costs.
func benchServer(tb testing.TB, body []byte) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "f.bin", time.Time{}, bytes.NewReader(body))
	}))
	tb.Cleanup(server.Close)
	return server
}

type benchOptions struct {
	workers    int
	bufferSize int
	depth      int
	checkpoint bool
}

func runBenchDownload(b *testing.B, dir, url string, size int64, opt benchOptions) {
	b.Helper()
	b.StopTimer()
	out := filepath.Join(dir, "out.bin")
	file, err := openOutput(out, size, false)
	if err != nil {
		b.Fatalf("openOutput: %v", err)
	}
	defer file.Close()

	d := &downloader{
		clients:    newTCPClientSet(opt.workers, opt.bufferSize),
		outputPath: out,
		statePath:  out + ".pearl",
		file:       file,
		totalSize:  size,
		retries:    2,
		sources:    newSourcePool(mirrorsFor(url), opt.workers),
		pipe:       newPipeline(opt.bufferSize, opt.depth),
		co:         newCoordinator(splitEvenly(size, opt.workers)),
		meter:      &atomic.Int64{},
	}
	for i := 0; i < opt.workers; i++ {
		stat := &workerStat{}
		stat.source.Store(-1)
		d.stats = append(d.stats, stat)
	}

	done := make(chan struct{})
	if opt.checkpoint {
		go d.checkpointLoop(done)
	}

	b.StartTimer()
	err = d.run(context.Background())
	close(done)
	b.StopTimer()

	if err != nil {
		b.Fatalf("download: %v", err)
	}
	if left := d.co.remaining(); left != 0 {
		b.Fatalf("%d bytes unfetched", left)
	}
	b.StartTimer()
}

func BenchmarkDownload(b *testing.B) {
	const size = 128 << 20
	body := make([]byte, size)
	rand.New(rand.NewSource(1)).Read(body)
	server := benchServer(b, body)
	dir := b.TempDir()

	for _, workers := range []int{4, 8, 16} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.SetBytes(size)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				runBenchDownload(b, dir, server.URL, size, benchOptions{
					workers:    workers,
					bufferSize: defaultBufferSize,
					depth:      defaultPipelineDepth,
				})
			}
		})
	}
}

// BenchmarkCheckpointCost isolates what the every-two-seconds fsync costs a
// fast transfer. Each checkpoint flushes every dirty page written since the
// last one, so on a quick download it is pure overhead against throughput.
func BenchmarkCheckpointCost(b *testing.B) {
	const size = 128 << 20
	body := make([]byte, size)
	rand.New(rand.NewSource(2)).Read(body)
	server := benchServer(b, body)
	dir := b.TempDir()

	for _, checkpoint := range []bool{false, true} {
		name := "checkpoint=off"
		if checkpoint {
			name = "checkpoint=on"
		}
		b.Run(name, func(b *testing.B) {
			b.SetBytes(size)
			for i := 0; i < b.N; i++ {
				runBenchDownload(b, dir, server.URL, size, benchOptions{
					workers:    8,
					bufferSize: defaultBufferSize,
					depth:      defaultPipelineDepth,
					checkpoint: checkpoint,
				})
			}
		})
	}
}

// BenchmarkCheckpointSync measures what one checkpoint's fsync actually costs
// while workers are writing, which is the thing a short benchmark download can
// never show: at 50 MB/s a two-second interval means each Sync flushes roughly
// 100 MB of dirty pages, and it does so on the same inode the workers are
// still writing to.
func BenchmarkCheckpointSync(b *testing.B) {
	const (
		perCheckpoint = 100 << 20 // bytes written between checkpoints at ~50 MB/s
		workers       = 8
		chunk         = 512 << 10
	)
	dir := b.TempDir()
	payload := make([]byte, chunk)

	writeAll := func(file *os.File) {
		var wg sync.WaitGroup
		span := int64(perCheckpoint / workers)
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				base := int64(w) * span
				for off := int64(0); off < span; off += chunk {
					file.WriteAt(payload, base+off)
				}
			}(w)
		}
		wg.Wait()
	}

	b.Run("write-only", func(b *testing.B) {
		b.SetBytes(perCheckpoint)
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			file, err := openOutput(filepath.Join(dir, "a.bin"), perCheckpoint, false)
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			writeAll(file)
			b.StopTimer()
			file.Close()
			b.StartTimer()
		}
	})

	b.Run("write+sync", func(b *testing.B) {
		b.SetBytes(perCheckpoint)
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			file, err := openOutput(filepath.Join(dir, "b.bin"), perCheckpoint, false)
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			writeAll(file)
			if err := file.Sync(); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			file.Close()
			b.StartTimer()
		}
	})
}

// BenchmarkBufferSize checks whether the chunk size actually moves throughput,
// rather than assuming that bigger is better.
func BenchmarkBufferSize(b *testing.B) {
	const size = 128 << 20
	body := make([]byte, size)
	rand.New(rand.NewSource(3)).Read(body)
	server := benchServer(b, body)
	dir := b.TempDir()

	for _, buf := range []int{128 << 10, 512 << 10, 2 << 20, 8 << 20} {
		b.Run(humanBytes(int64(buf)), func(b *testing.B) {
			b.SetBytes(size)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				runBenchDownload(b, dir, server.URL, size, benchOptions{
					workers:    8,
					bufferSize: buf,
					depth:      defaultPipelineDepth,
				})
			}
		})
	}
}
