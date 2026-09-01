package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
)

// maxSourceFailures is how many consecutive failures retire a mirror for the
// rest of the run. A mirror that is refusing connections is worse than no
// mirror at all: every worker that lands on it burns a retry budget before
// making progress, so once it has proved itself broken we stop sending work
// there. The last surviving mirror is never retired -- with nowhere else to
// send the bytes, the ordinary retry path is all we have.
const maxSourceFailures = 3

// verifyWindow is how many bytes are read from the middle of each candidate
// mirror and compared against the primary before it is allowed to serve any
// real ranges. Matching content lengths are necessary but nowhere near
// sufficient: two different builds of the same image are routinely the same
// size, and splicing them together would produce a file that is corrupt in a
// way no amount of retrying can fix.
const verifyWindow = 64 << 10

// source is one mirror serving the same bytes. Each carries its own
// validator, because an ETag is only meaningful to the server that issued it
// -- sending mirror A's ETag to mirror B in an If-Range is not a weaker
// check, it is a meaningless one.
type source struct {
	url       string
	label     string
	validator validator
}

// urlList collects repeated -u flags, and splits comma-separated values, so
// both "-u A -u B" and "-u A,B" name the same two mirrors.
type urlList []string

func (u *urlList) String() string { return strings.Join(*u, ",") }

func (u *urlList) Set(value string) error {
	for _, part := range strings.Split(value, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*u = append(*u, part)
		}
	}
	return nil
}

// sourcePool decides which mirror each worker talks to. Workers are spread
// round-robin at the start; a worker whose mirror is retired migrates to the
// least loaded survivor, so losing a mirror costs its share of the
// connections rather than the download.
//
// The pool deliberately knows nothing about byte ranges: the coordinator owns
// those, and keeping the two apart is what lets a range be started on one
// mirror and finished on another without any special handling.
type sourcePool struct {
	mu       sync.Mutex
	sources  []source
	failures []int
	dead     []bool
	load     []int // workers currently assigned to each mirror
	assign   []int // worker id -> mirror index
}

func newSourcePool(sources []source, workers int) *sourcePool {
	p := &sourcePool{
		sources:  sources,
		failures: make([]int, len(sources)),
		dead:     make([]bool, len(sources)),
		load:     make([]int, len(sources)),
		assign:   make([]int, workers),
	}
	for w := 0; w < workers; w++ {
		idx := w % len(sources)
		p.assign[w] = idx
		p.load[idx]++
	}
	return p
}

func (p *sourcePool) count() int { return len(p.sources) }

func (p *sourcePool) at(idx int) source { return p.sources[idx] }

// pick reports the mirror this worker should use for its next attempt. It
// returns false only when every mirror has been retired, which is the one
// case where the download genuinely cannot continue.
func (p *sourcePool) pick(worker int) (source, int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	idx := p.assign[worker]
	if !p.dead[idx] {
		return p.sources[idx], idx, true
	}

	best := -1
	for i := range p.sources {
		if p.dead[i] {
			continue
		}
		if best == -1 || p.load[i] < p.load[best] {
			best = i
		}
	}
	if best == -1 {
		return source{}, -1, false
	}
	p.load[idx]--
	p.load[best]++
	p.assign[worker] = best
	return p.sources[best], best, true
}

// succeed clears a mirror's failure streak. Only *consecutive* failures
// retire a mirror, so an otherwise healthy server that drops the occasional
// connection is never written off.
func (p *sourcePool) succeed(idx int) {
	if idx < 0 {
		return
	}
	p.mu.Lock()
	p.failures[idx] = 0
	p.mu.Unlock()
}

// fail records a failed transfer and reports whether it retired the mirror.
func (p *sourcePool) fail(idx int) bool {
	if idx < 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	p.failures[idx]++
	if p.dead[idx] || p.failures[idx] < maxSourceFailures {
		return false
	}
	live := 0
	for i := range p.dead {
		if !p.dead[i] {
			live++
		}
	}
	if live <= 1 {
		return false
	}
	p.dead[idx] = true
	return true
}

func (p *sourcePool) alive() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	live := 0
	for i := range p.dead {
		if !p.dead[i] {
			live++
		}
	}
	return live
}

// isDead reports whether a mirror has been retired, for the display.
func (p *sourcePool) isDead(idx int) bool {
	if idx < 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dead[idx]
}

// maxLabel is the widest mirror label the matrix will show. Labels are capped
// here rather than at render time so that uniqueness is checked against what
// actually reaches the screen: two labels that differ only past the cut would
// arrive at the terminal identical.
const maxLabel = 12

// shortHost is the leading label of the hostname: "cdimage.ubuntu.com" ->
// "cdimage". It is the most readable form and usually distinguishing.
func shortHost(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Hostname() == "" {
		return "source"
	}
	host := parsed.Hostname()
	if first, _, ok := strings.Cut(host, "."); ok && len(first) > 2 {
		return first
	}
	return host
}

func fullHost(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Hostname() == "" {
		return "source"
	}
	return parsed.Hostname()
}

func hostPort(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" {
		return "source"
	}
	return parsed.Host
}

func trimTo(text string, width int) string {
	if runes := []rune(text); len(runes) > width {
		return string(runes[:width])
	}
	return text
}

// assignLabels gives every mirror the shortest name that still tells it apart
// from the others *after truncation*. Mirrors that remain indistinguishable
// by name -- several ports on one host, say -- fall back to a numeric tag,
// because a column that shows the same text on every row is just wasted width.
func assignLabels(sources []source) {
	for _, form := range []func(string) string{shortHost, fullHost, hostPort} {
		candidates := make([]string, len(sources))
		seen := map[string]int{}
		for i := range sources {
			candidates[i] = trimTo(form(sources[i].url), maxLabel)
			seen[candidates[i]]++
		}
		unique := true
		for _, n := range seen {
			if n > 1 {
				unique = false
				break
			}
		}
		if unique {
			for i := range sources {
				sources[i].label = candidates[i]
			}
			return
		}
	}
	for i := range sources {
		tag := fmt.Sprintf("#%d", i+1)
		sources[i].label = trimTo(shortHost(sources[i].url), maxLabel-len(tag)) + tag
	}
}

// probeSources probes every URL and returns the mirrors that are safe to use
// together, plus the probe result for the primary. The first URL that answers
// becomes the primary and defines the file; the rest have to prove they are
// serving the same bytes before they are allowed to contribute any.
func probeSources(ctx context.Context, client *http.Client, urls []string) ([]source, probeResult, error) {
	var primary probeResult
	var primaryIndex = -1
	var firstErr error
	for i, raw := range urls {
		result, err := probe(ctx, client, raw)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			fmt.Fprintf(os.Stderr, "pearl: skipping %s (%v)\n", raw, err)
			continue
		}
		primary, primaryIndex = result, i
		break
	}
	if primaryIndex == -1 {
		return nil, probeResult{}, firstErr
	}

	sources := []source{{url: primary.url, validator: primary.validator}}

	// Mirroring needs ranges: without them there is only one stream to be had
	// from each server and nothing to split between them.
	if !primary.ranges || primary.size <= 0 {
		if len(urls) > 1 {
			fmt.Fprintf(os.Stderr, "pearl: %s does not support range requests, ignoring the other mirrors\n", primary.url)
		}
		assignLabels(sources)
		return sources, primary, nil
	}

	var reference []byte
	start, length := verifyRange(primary.size)
	for i, raw := range urls {
		if i == primaryIndex {
			continue
		}
		candidate, err := probe(ctx, client, raw)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pearl: skipping mirror %s (%v)\n", raw, err)
			continue
		}
		if !candidate.ranges || candidate.size != primary.size {
			fmt.Fprintf(os.Stderr, "pearl: skipping mirror %s (serves %s, primary serves %s)\n",
				raw, humanBytes(candidate.size), humanBytes(primary.size))
			continue
		}
		if reference == nil {
			reference, err = fetchWindow(ctx, client, primary.url, start, length)
			if err != nil {
				fmt.Fprintf(os.Stderr, "pearl: cannot verify mirrors against %s (%v), continuing single-source\n", primary.url, err)
				break
			}
		}
		sample, err := fetchWindow(ctx, client, candidate.url, start, length)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pearl: skipping mirror %s (%v)\n", raw, err)
			continue
		}
		if !bytes.Equal(reference, sample) {
			fmt.Fprintf(os.Stderr, "pearl: skipping mirror %s (different content at offset %d)\n", raw, start)
			continue
		}
		sources = append(sources, source{url: candidate.url, validator: candidate.validator})
	}

	assignLabels(sources)
	return sources, primary, nil
}

// verifyRange picks the window compared across mirrors. It is taken from the
// middle rather than the start because file headers are the least
// distinguishing part of a file: two different releases of the same image
// often share their first few kilobytes exactly.
func verifyRange(size int64) (start, length int64) {
	length = int64(verifyWindow)
	if length > size {
		length = size
	}
	start = size/2 - length/2
	if start < 0 {
		start = 0
	}
	if start+length > size {
		start = size - length
	}
	return start, length
}

func fetchWindow(ctx context.Context, client *http.Client, rawURL string, start, length int64) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, start+length-1))
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("expected 206, got %s", response.Status)
	}
	return io.ReadAll(io.LimitReader(response.Body, length))
}
