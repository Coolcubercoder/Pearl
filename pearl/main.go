package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type config struct {
	urls          urlList
	output        string
	concurrency   int
	retries       int
	fresh         bool
	plain         bool
	http3         bool
	bufferSize    int
	pipelineDepth int
}

func main() {
	var cfg config

	flag.Var(&cfg.urls, "u", "URL to download; repeat or comma-separate for mirrors")
	flag.IntVar(&cfg.concurrency, "c", 8, "number of concurrent ranged connections")
	flag.StringVar(&cfg.output, "o", "", "output file path")
	flag.IntVar(&cfg.retries, "r", 5, "retry attempts per connection before giving up")
	flag.BoolVar(&cfg.fresh, "fresh", false, "ignore any saved .pearl state and restart from zero")
	flag.BoolVar(&cfg.plain, "plain", false, "print plain progress lines instead of the live matrix")
	flag.BoolVar(&cfg.http3, "http3", false, "try HTTP/3 over QUIC, falling back to TCP if it does not work")
	bufText := flag.String("buf", "512K", "read/write chunk size per connection (e.g. 512K, 4M)")
	flag.IntVar(&cfg.pipelineDepth, "pipeline", defaultPipelineDepth, "chunks kept in flight per connection")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: pearl -u URL [-u MIRROR ...] [-c concurrency] [-o output]\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if len(cfg.urls) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	if cfg.concurrency < 1 {
		fmt.Fprintln(os.Stderr, "pearl: concurrency must be at least 1")
		os.Exit(2)
	}
	if cfg.retries < 0 {
		fmt.Fprintln(os.Stderr, "pearl: retries cannot be negative")
		os.Exit(2)
	}

	size, err := parseSize(*bufText)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pearl: %v\n", err)
		os.Exit(2)
	}
	// Below the floor the syscall overhead per byte starts to dominate; above
	// the ceiling a single connection is holding more memory than any plausible
	// bandwidth-delay product needs.
	if size < minBufferSize || size > maxBufferSize {
		fmt.Fprintf(os.Stderr, "pearl: -buf must be between %s and %s\n",
			humanBytes(minBufferSize), humanBytes(maxBufferSize))
		os.Exit(2)
	}
	if cfg.pipelineDepth < 2 || cfg.pipelineDepth > 16 {
		fmt.Fprintln(os.Stderr, "pearl: -pipeline must be between 2 and 16")
		os.Exit(2)
	}
	cfg.bufferSize = int(size)

	// Buffers are per connection, so the three flags multiply. Say so rather
	// than quietly reserving a surprising amount of memory.
	if total := size * int64(cfg.pipelineDepth) * int64(cfg.concurrency); total > 1<<30 {
		fmt.Fprintf(os.Stderr, "pearl: warning: -buf x -pipeline x -c reserves up to %s of buffers\n",
			humanBytes(total))
	}

	seen := map[string]bool{}
	unique := cfg.urls[:0]
	for _, raw := range cfg.urls {
		parsed, err := url.ParseRequestURI(raw)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			fmt.Fprintf(os.Stderr, "pearl: invalid URL %q\n", raw)
			os.Exit(2)
		}
		// Duplicate mirrors would double a server's share of the connections
		// while pretending to spread the load, which is the opposite of the point.
		if normalized := parsed.String(); !seen[normalized] {
			seen[normalized] = true
			unique = append(unique, normalized)
		}
	}
	cfg.urls = unique

	// Interrupts cancel the workers rather than killing the process, so the
	// checkpoint on the way out describes exactly what reached the disk.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch err := run(ctx, cfg); {
	case errors.Is(err, errInterrupted):
		os.Exit(130)
	case err != nil:
		fmt.Fprintf(os.Stderr, "pearl: %v\n", err)
		os.Exit(1)
	}
}

// probeResult is what the pre-flight requests told us about the target.
type probeResult struct {
	url       string // after redirects, so range requests skip the hops
	size      int64
	ranges    bool
	validator validator
}

func run(ctx context.Context, cfg config) error {
	clients, sources, info, err := connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer clients.Close()

	if cfg.output == "" {
		cfg.output = defaultOutputName(info.url)
	}
	if info.ranges && info.size > 0 {
		return rangedRun(ctx, clients, cfg, info, sources)
	}
	return sequentialRun(ctx, clients, cfg, info)
}

// connect builds the transport and probes the mirrors with it. HTTP/3 is
// attempted only when asked for, and never at the cost of the download: UDP
// is blocked or throttled on plenty of networks, and a server advertising
// HTTP/3 is no guarantee it will carry a bulk transfer well. A failed probe
// therefore falls back to TCP rather than failing.
func connect(ctx context.Context, cfg config) (*clientSet, []source, probeResult, error) {
	if cfg.http3 {
		clients := newHTTP3ClientSet(cfg.concurrency, nil)
		// Decide on the transport before probing the mirrors, using whether QUIC
		// can be established at all. Falling back on any probe failure instead
		// would retry genuine HTTP errors -- a 404, a dead host -- pointlessly
		// over TCP and report every one of them twice.
		if err := quicReachable(ctx, clients.probeClient(), cfg.urls[0]); err != nil {
			clients.Close()
			fmt.Fprintf(os.Stderr, "pearl: HTTP/3 unavailable (%v), falling back to TCP\n", err)
		} else {
			sources, info, err := probeSources(ctx, clients.probeClient(), cfg.urls)
			if err != nil {
				clients.Close()
				return nil, nil, probeResult{}, err
			}
			return clients, sources, info, nil
		}
	}

	clients := newTCPClientSet(cfg.concurrency, cfg.bufferSize)
	sources, info, err := probeSources(ctx, clients.probeClient(), cfg.urls)
	if err != nil {
		clients.Close()
		return nil, nil, probeResult{}, err
	}
	return clients, sources, info, nil
}

// parseSize parses a byte count with an optional binary suffix, accepting the
// spellings people actually type: 512K, 4M, 4MB, 4MiB, or a plain byte count.
func parseSize(text string) (int64, error) {
	upper := strings.ToUpper(strings.TrimSpace(text))
	upper = strings.TrimSuffix(upper, "B")
	upper = strings.TrimSuffix(upper, "I")

	multiplier := int64(1)
	if upper != "" {
		switch upper[len(upper)-1] {
		case 'K':
			multiplier, upper = 1<<10, upper[:len(upper)-1]
		case 'M':
			multiplier, upper = 1<<20, upper[:len(upper)-1]
		case 'G':
			multiplier, upper = 1<<30, upper[:len(upper)-1]
		}
	}
	value, err := strconv.ParseInt(strings.TrimSpace(upper), 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("invalid size %q", text)
	}
	return value * multiplier, nil
}

func defaultOutputName(rawURL string) string {
	name := "download"
	if parsed, err := url.Parse(rawURL); err == nil {
		if base := path.Base(parsed.Path); base != "." && base != "/" && base != "" {
			name = base
		}
	}
	return name
}

// probe asks the server how big the file is and whether it will serve byte
// ranges. It prefers HEAD, but falls back to a one-byte ranged GET: plenty of
// servers either reject HEAD or fail to advertise Accept-Ranges while
// happily honouring Range.
func probe(ctx context.Context, client *http.Client, rawURL string) (probeResult, error) {
	head, headErr := headProbe(ctx, client, rawURL)
	if headErr == nil && head.ranges && head.size > 0 {
		return head, nil
	}
	ranged, rangeErr := rangeProbe(ctx, client, rawURL)
	if rangeErr == nil && ranged.ranges && ranged.size > 0 {
		return ranged, nil
	}
	if headErr == nil {
		return head, nil
	}
	if rangeErr == nil {
		return ranged, nil
	}
	return probeResult{}, fmt.Errorf("pre-flight request: %w", headErr)
}

func headProbe(ctx context.Context, client *http.Client, rawURL string) (probeResult, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodHead, rawURL, nil)
	if err != nil {
		return probeResult{}, err
	}
	response, err := client.Do(request)
	if err != nil {
		return probeResult{}, err
	}
	response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return probeResult{}, fmt.Errorf("HEAD returned %s", response.Status)
	}
	return probeResult{
		url:       response.Request.URL.String(),
		size:      response.ContentLength,
		ranges:    strings.EqualFold(strings.TrimSpace(response.Header.Get("Accept-Ranges")), "bytes"),
		validator: validatorOf(response),
	}, nil
}

func rangeProbe(ctx context.Context, client *http.Client, rawURL string) (probeResult, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return probeResult{}, err
	}
	request.Header.Set("Range", "bytes=0-0")
	response, err := client.Do(request)
	if err != nil {
		return probeResult{}, err
	}
	defer response.Body.Close()

	result := probeResult{url: response.Request.URL.String(), validator: validatorOf(response)}
	if response.StatusCode == http.StatusPartialContent {
		size, ok := totalFromContentRange(response.Header.Get("Content-Range"))
		result.ranges, result.size = ok, size
		return result, nil
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return probeResult{}, fmt.Errorf("GET returned %s", response.Status)
	}
	result.size = response.ContentLength
	return result, nil
}

func validatorOf(response *http.Response) validator {
	return validator{
		etag:         strings.TrimSpace(response.Header.Get("ETag")),
		lastModified: strings.TrimSpace(response.Header.Get("Last-Modified")),
	}
}

// totalFromContentRange pulls the instance length out of "bytes 0-0/1234".
func totalFromContentRange(header string) (int64, bool) {
	slash := strings.LastIndex(header, "/")
	if slash < 0 {
		return 0, false
	}
	size, err := strconv.ParseInt(strings.TrimSpace(header[slash+1:]), 10, 64)
	if err != nil || size <= 0 {
		return 0, false
	}
	return size, true
}

func rangedRun(ctx context.Context, clients *clientSet, cfg config, info probeResult, sources []source) error {
	statePath := statePathFor(cfg.output)
	views, resumed, err := planSegments(cfg, info, sources, statePath)
	if err != nil {
		return err
	}

	file, err := openOutput(cfg.output, info.size, resumed)
	if err != nil {
		return err
	}
	defer file.Close()

	meter := &atomic.Int64{}
	d := &downloader{
		clients:    clients,
		outputPath: cfg.output,
		statePath:  statePath,
		file:       file,
		totalSize:  info.size,
		retries:    cfg.retries,
		sources:    newSourcePool(sources, cfg.concurrency),
		pipe:       newPipeline(cfg.bufferSize, cfg.pipelineDepth),
		co:         newCoordinator(views),
		meter:      meter,
	}
	for i := 0; i < cfg.concurrency; i++ {
		stat := &workerStat{}
		stat.source.Store(-1) // no mirror chosen until the first attempt
		d.stats = append(d.stats, stat)
	}
	meter.Store(info.size - d.co.remaining())

	started := time.Now()
	startBytes := meter.Load()

	view := newDisplay(os.Stdout, filepath.Base(cfg.output), info.size, resumed, d.co, d.sources, d.stats, meter, cfg.plain)
	done := make(chan struct{})
	var background sync.WaitGroup
	background.Add(2)
	go func() { defer background.Done(); view.run(done) }()
	go func() { defer background.Done(); d.checkpointLoop(done) }()

	runErr := d.run(ctx)

	close(done)
	background.Wait()
	view.finish()

	if runErr != nil {
		if err := d.syncAndCheckpoint(); err != nil {
			fmt.Fprintf(os.Stderr, "pearl: could not save resume state: %v\n", err)
		} else {
			fmt.Printf("\nProgress saved to %s — rerun the same command to resume (%s of %s done).\n",
				statePath, humanBytes(meter.Load()), humanBytes(info.size))
		}
		return runErr
	}

	if left := d.co.remaining(); left > 0 {
		d.syncAndCheckpoint()
		return fmt.Errorf("finished with %s unaccounted for", humanBytes(left))
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("flush output: %w", err)
	}
	os.Remove(statePath)

	_, steals := d.co.snapshot(nil)
	return reportSuccess(cfg.output, info.size, meter.Load()-startBytes, started, steals)
}

// planSegments decides whether this is a resume or a fresh start, and lays
// out the byte ranges accordingly.
func planSegments(cfg config, info probeResult, sources []source, statePath string) ([]segView, bool, error) {
	if cfg.fresh {
		os.Remove(statePath)
		return splitEvenly(info.size, cfg.concurrency), false, nil
	}

	saved, err := loadState(statePath)
	if err != nil {
		if !os.IsNotExist(err) {
			// A corrupt or unreadable sidecar should not block the download; it
			// only costs us the resume.
			fmt.Fprintf(os.Stderr, "pearl: ignoring %s (%v)\n", statePath, err)
		}
		return splitEvenly(info.size, cfg.concurrency), false, nil
	}
	if !saved.resumable(info.size, sources) {
		fmt.Fprintf(os.Stderr, "pearl: remote file changed since %s was written, starting over\n", statePath)
		os.Remove(statePath)
		return splitEvenly(info.size, cfg.concurrency), false, nil
	}
	if _, err := os.Stat(cfg.output); err != nil {
		fmt.Fprintf(os.Stderr, "pearl: %s is missing, starting over\n", cfg.output)
		os.Remove(statePath)
		return splitEvenly(info.size, cfg.concurrency), false, nil
	}
	return saved.views(), true, nil
}

// openOutput creates and pre-allocates the file, or reopens the partial one
// when resuming. Resuming must never truncate: the bytes already on disk are
// the whole point.
func openOutput(outputPath string, size int64, resumed bool) (*os.File, error) {
	if resumed {
		file, err := os.OpenFile(outputPath, os.O_RDWR, 0o644)
		if err != nil {
			return nil, fmt.Errorf("reopen output: %w", err)
		}
		info, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("stat output: %w", err)
		}
		if info.Size() != size {
			if err := file.Truncate(size); err != nil {
				file.Close()
				return nil, fmt.Errorf("resize output: %w", err)
			}
		}
		return file, nil
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return nil, fmt.Errorf("create output: %w", err)
	}
	// Reserve the blocks before setting the length. Truncate on its own leaves
	// a sparse file, which pushes block allocation onto the write path where
	// it happens repeatedly, mid-transfer, scattered across the whole file.
	// Preallocation is best-effort: filesystems that cannot do it just get the
	// old lazy behaviour, so a failure here is not worth failing the download.
	_ = preallocate(file, size)
	if err := file.Truncate(size); err != nil {
		file.Close()
		return nil, fmt.Errorf("size output: %w", err)
	}
	return file, nil
}

// sequentialRun handles servers that will not serve ranges, or that hide the
// content length. There is one connection, no work to steal, and nothing
// safe to resume, so it just streams.
func sequentialRun(ctx context.Context, clients *clientSet, cfg config, info probeResult) error {
	meter := &atomic.Int64{}
	stats := []*workerStat{{}}
	view := newDisplay(os.Stdout, filepath.Base(cfg.output), info.size, false, nil, nil, stats, meter, cfg.plain)
	done := make(chan struct{})
	var background sync.WaitGroup
	background.Add(1)
	go func() { defer background.Done(); view.run(done) }()

	started := time.Now()
	err := streamToFile(ctx, clients.probeClient(), cfg.output, info, newPipeline(cfg.bufferSize, cfg.pipelineDepth), stats[0], meter)

	close(done)
	background.Wait()
	view.finish()

	if err != nil {
		return err
	}
	return reportSuccess(cfg.output, meter.Load(), meter.Load(), started, 0)
}

func streamToFile(ctx context.Context, client *http.Client, outputPath string, info probeResult, pipe *pipeline, stat *workerStat, meter *atomic.Int64) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stat.setState(stateConnecting)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, info.url, nil)
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
	if info.size > 0 {
		if err := file.Truncate(info.size); err != nil {
			return fmt.Errorf("pre-allocate output: %w", err)
		}
	}

	stat.setState(stateDownloading)
	downloaded, err := pipe.copy(cancel, response.Body, func(buf []byte) (int, error) {
		n, err := file.Write(buf)
		stat.bytes.Add(int64(n))
		meter.Add(int64(n))
		return n, err
	})
	if err != nil {
		stat.setState(stateFailed)
		stat.setNote(err)
		return fmt.Errorf("download: %w", err)
	}
	stat.setState(stateDone)
	if err := file.Truncate(downloaded); err != nil {
		return fmt.Errorf("finalize output: %w", err)
	}
	if info.size > 0 && downloaded != info.size {
		return fmt.Errorf("downloaded %d bytes, expected %d", downloaded, info.size)
	}
	return file.Sync()
}

func reportSuccess(outputPath string, totalSize, sessionBytes int64, started time.Time, steals int) error {
	if totalSize <= 0 {
		info, err := os.Stat(outputPath)
		if err != nil {
			return fmt.Errorf("stat output: %w", err)
		}
		totalSize = info.Size()
	}
	elapsed := time.Since(started)
	rate := float64(sessionBytes) / elapsed.Seconds()
	fmt.Printf("\nSaved %s (%s) in %s, average %s",
		outputPath, humanBytes(totalSize), elapsed.Round(time.Millisecond), humanRate(rate))
	if steals > 0 {
		fmt.Printf(", %d range%s rebalanced", steals, plural(steals))
	}
	fmt.Println()
	return nil
}
