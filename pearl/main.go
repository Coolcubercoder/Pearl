package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	bufferSize = 512 * 1024
	// pipelineDepth buffers are kept in flight per worker so the network read
	// for chunk N+1 can proceed while chunk N is still being written to disk,
	// instead of the two alternating serially.
	pipelineDepth      = 3
	progressUpdateSize = 1 * 1024 * 1024
)

type progress struct {
	bytes atomic.Int64
	total int64
}

func (p *progress) run(done <-chan struct{}, tickerInterval time.Duration) {
	ticker := time.NewTicker(tickerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.print(false)
		case <-done:
			return
		}
	}
}

func (p *progress) print(final bool) {
	current := p.bytes.Load()
	percentage := float64(0)
	if p.total > 0 {
		percentage = float64(current) * 100 / float64(p.total)
	}
	if final {
		if p.total > 0 {
			fmt.Printf("\rDownloaded %d / %d bytes (%.1f%%)\n", current, p.total, percentage)
		} else {
			fmt.Printf("\rDownloaded %d bytes\n", current)
		}
		return
	}
	fmt.Printf("\rDownloaded %d / %d bytes (%.1f%%)", current, p.total, percentage)
}

func main() {
	var rawURL, outputPath string
	var concurrency int

	flag.StringVar(&rawURL, "u", "", "URL to download")
	flag.IntVar(&concurrency, "c", 8, "number of concurrent ranged downloads")
	flag.StringVar(&outputPath, "o", "", "output file path")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: pearl -u URL [-c concurrency] [-o output]\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if rawURL == "" {
		flag.Usage()
		os.Exit(2)
	}
	if concurrency < 1 {
		fmt.Fprintln(os.Stderr, "pearl: concurrency must be at least 1")
		os.Exit(2)
	}

	downloadURL, err := url.ParseRequestURI(rawURL)
	if err != nil || downloadURL.Scheme == "" || downloadURL.Host == "" {
		fmt.Fprintf(os.Stderr, "pearl: invalid URL %q\n", rawURL)
		os.Exit(2)
	}
	if outputPath == "" {
		outputPath = path.Base(downloadURL.Path)
		if outputPath == "." || outputPath == "/" || outputPath == "" {
			outputPath = "download"
		}
	}

	if err := download(downloadURL.String(), outputPath, concurrency); err != nil {
		fmt.Fprintf(os.Stderr, "pearl: %v\n", err)
		os.Exit(1)
	}
}

func download(rawURL, outputPath string, concurrency int) error {
	client := newHTTPClient(concurrency)
	response, err := client.Head(rawURL)
	if err != nil {
		return fmt.Errorf("pre-flight HEAD request: %w", err)
	}
	response.Body.Close()

	totalSize := response.ContentLength
	rangesSupported := strings.EqualFold(strings.TrimSpace(response.Header.Get("Accept-Ranges")), "bytes")
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("pre-flight HEAD request returned %s", response.Status)
	}

	started := time.Now()
	if totalSize <= 0 || !rangesSupported {
		progressState := &progress{total: totalSize}
		done := make(chan struct{})
		var progressWait sync.WaitGroup
		progressWait.Add(1)
		go func() {
			defer progressWait.Done()
			progressState.run(done, 250*time.Millisecond)
		}()
		err := sequentialDownload(client, rawURL, outputPath, totalSize, progressState)
		close(done)
		progressWait.Wait()
		progressState.print(true)
		if err != nil {
			return err
		}
		return reportSuccess(outputPath, totalSize, started)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer file.Close()
	if err := file.Truncate(totalSize); err != nil {
		return fmt.Errorf("pre-allocate output: %w", err)
	}

	workerCount := concurrency
	if int64(workerCount) > totalSize {
		workerCount = int(totalSize)
	}
	progressState := &progress{total: totalSize}
	done := make(chan struct{})
	var progressWait sync.WaitGroup
	progressWait.Add(1)
	go func() {
		defer progressWait.Done()
		progressState.run(done, 250*time.Millisecond)
	}()

	err = rangedDownload(client, file, rawURL, totalSize, workerCount, progressState)
	close(done)
	progressWait.Wait()
	progressState.print(true)
	if err != nil {
		return err
	}
	return reportSuccess(outputPath, totalSize, started)
}

func newHTTPClient(concurrency int) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			// HTTP/2 multiplexes all requests to a host over one TCP connection,
			// which would serialize our "concurrent" range requests onto a single
			// congestion window. Disabling it forces one independent connection
			// per worker, so concurrency actually parallelizes throughput.
			TLSNextProto:          map[string]func(string, *tls.Conn) http.RoundTripper{},
			MaxIdleConns:          concurrency,
			MaxIdleConnsPerHost:   concurrency,
			MaxConnsPerHost:       concurrency,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   15 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			DisableCompression:    true,
			// Match the transport's read buffering to our copy buffer size so
			// large transfers need fewer read syscalls per connection.
			ReadBufferSize: bufferSize,
		},
	}
}

// chunk is a filled read buffer handed from the reader goroutine to the
// writer goroutine inside pipelinedCopy.
type chunk struct {
	buf []byte
	n   int
}

// pipelinedCopy streams src through writeChunk, overlapping network reads
// with the (potentially slow) writeChunk call by running the reader and
// writer on separate goroutines connected by a small buffer pool. writeChunk
// is always called from a single goroutine, in order, so it is safe for it
// to mutate captured state (e.g. advancing a file offset) without locking.
// On a writeChunk error, cancel is invoked so a context-aware src (such as an
// HTTP response body) unblocks promptly instead of reading to completion.
func pipelinedCopy(cancel context.CancelFunc, src io.Reader, writeChunk func(buf []byte) error, progressState *progress) (int64, error) {
	free := make(chan []byte, pipelineDepth)
	for i := 0; i < pipelineDepth; i++ {
		free <- make([]byte, bufferSize)
	}
	filled := make(chan chunk, pipelineDepth)
	writeErrCh := make(chan error, 1)

	go func() {
		pendingProgress := int64(0)
		for c := range filled {
			if err := writeChunk(c.buf[:c.n]); err != nil {
				cancel()
				// Keep recycling buffers back to free so the reader (which may
				// be blocked waiting for one) can observe the cancellation via
				// its next Read and unwind instead of deadlocking.
				for wc := range filled {
					free <- wc.buf
				}
				writeErrCh <- err
				return
			}
			pendingProgress += int64(c.n)
			if pendingProgress >= progressUpdateSize {
				progressState.bytes.Add(pendingProgress)
				pendingProgress = 0
			}
			free <- c.buf
		}
		if pendingProgress > 0 {
			progressState.bytes.Add(pendingProgress)
		}
		writeErrCh <- nil
	}()

	var total int64
	var readErr error
	for {
		buf := <-free
		read, err := src.Read(buf)
		if read > 0 {
			total += int64(read)
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

	writeErr := <-writeErrCh
	if readErr != nil {
		return total, readErr
	}
	return total, writeErr
}

func rangedDownload(client *http.Client, file *os.File, rawURL string, totalSize int64, workerCount int, progressState *progress) error {
	var workers sync.WaitGroup
	errorsFound := make(chan error, workerCount)
	baseSize := totalSize / int64(workerCount)
	extra := totalSize % int64(workerCount)
	start := int64(0)

	for worker := 0; worker < workerCount; worker++ {
		chunkSize := baseSize
		if int64(worker) < extra {
			chunkSize++
		}
		chunkStart := start
		chunkEnd := chunkStart + chunkSize - 1
		start += chunkSize

		workers.Add(1)
		go func(start, end int64) {
			defer workers.Done()
			if err := downloadRange(client, file, rawURL, start, end, progressState); err != nil {
				errorsFound <- err
			}
		}(chunkStart, chunkEnd)
	}

	workers.Wait()
	select {
	case err := <-errorsFound:
		return err
	default:
		return nil
	}
}

func downloadRange(client *http.Client, file *os.File, rawURL string, start, end int64, progressState *progress) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Range", "bytes="+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10))
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("range %d-%d: %w", start, end, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("range %d-%d returned %s", start, end, response.Status)
	}

	position := start
	total, err := pipelinedCopy(cancel, response.Body, func(buf []byte) error {
		_, writeErr := file.WriteAt(buf, position)
		position += int64(len(buf))
		return writeErr
	}, progressState)
	if err != nil {
		return fmt.Errorf("range %d-%d: %w", start, end, err)
	}
	if expected := end - start + 1; total != expected {
		return fmt.Errorf("range %d-%d: downloaded %d bytes, expected %d", start, end, total, expected)
	}
	return nil
}

func sequentialDownload(client *http.Client, rawURL, outputPath string, expectedSize int64, progressState *progress) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return fmt.Errorf("download: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("download: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("download returned %s", response.Status)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer file.Close()
	if expectedSize > 0 {
		if err := file.Truncate(expectedSize); err != nil {
			return fmt.Errorf("pre-allocate output: %w", err)
		}
	}

	downloaded, err := pipelinedCopy(cancel, response.Body, func(buf []byte) error {
		_, writeErr := file.Write(buf)
		return writeErr
	}, progressState)
	if err != nil {
		return fmt.Errorf("download: %w", err)
	}
	if err := file.Truncate(downloaded); err != nil {
		return fmt.Errorf("finalize output: %w", err)
	}
	if expectedSize > 0 && downloaded != expectedSize {
		return fmt.Errorf("downloaded %d bytes, expected %d", downloaded, expectedSize)
	}
	return nil
}

func reportSuccess(outputPath string, totalSize int64, started time.Time) error {
	if totalSize <= 0 {
		info, err := os.Stat(outputPath)
		if err != nil {
			return fmt.Errorf("stat output: %w", err)
		}
		totalSize = info.Size()
	}
	elapsed := time.Since(started)
	megabytesPerSecond := float64(totalSize) / elapsed.Seconds() / (1024 * 1024)
	fmt.Printf("Saved %s (%d bytes) in %s, average %.2f MB/s\n", outputPath, totalSize, elapsed.Round(time.Millisecond), megabytesPerSecond)
	return nil
}
