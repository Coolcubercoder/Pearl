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

func mirrorsFor(urls ...string) []source {
	sources := make([]source, 0, len(urls))
	for _, u := range urls {
		sources = append(sources, source{url: u})
	}
	assignLabels(sources)
	return sources
}

func TestURLListAcceptsRepeatedAndCommaSeparated(t *testing.T) {
	var list urlList
	if err := list.Set("http://a.test/f, http://b.test/f"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := list.Set("http://c.test/f"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	want := []string{"http://a.test/f", "http://b.test/f", "http://c.test/f"}
	if len(list) != len(want) {
		t.Fatalf("got %v, want %v", list, want)
	}
	for i := range want {
		if list[i] != want[i] {
			t.Fatalf("got %v, want %v", list, want)
		}
	}
}

func TestLabelsDisambiguateCollidingHosts(t *testing.T) {
	sources := mirrorsFor("http://mirror.a.test/f", "http://mirror.b.test/f")
	if sources[0].label == sources[1].label {
		t.Fatalf("colliding hosts must not share a label, both are %q", sources[0].label)
	}

	distinct := mirrorsFor("http://cdimage.ubuntu.com/f", "http://mirrors.kernel.org/f")
	if distinct[0].label != "cdimage" || distinct[1].label != "mirrors" {
		t.Fatalf("unexpected labels %q, %q", distinct[0].label, distinct[1].label)
	}
}

// Labels must stay distinct after the width cap, or the column tells the user
// nothing. Same host, different ports is the case that exposes this: every
// name-based form is identical until well past the cut.
func TestLabelsStayDistinctAfterTruncation(t *testing.T) {
	cases := [][]string{
		{"http://127.0.0.1:18081/f", "http://127.0.0.1:18082/f"},
		{"http://a.very-long-mirror-hostname.test/f", "http://b.very-long-mirror-hostname.test/f"},
		{"http://mirror.a.test/f", "http://mirror.b.test/f", "http://mirror.c.test/f"},
	}
	for _, urls := range cases {
		sources := mirrorsFor(urls...)
		seen := map[string]bool{}
		for _, s := range sources {
			if len([]rune(s.label)) > maxLabel {
				t.Fatalf("label %q exceeds the %d-column cap", s.label, maxLabel)
			}
			if seen[s.label] {
				t.Fatalf("%v produced duplicate label %q", urls, s.label)
			}
			seen[s.label] = true
		}
	}
}

func TestSourcePoolSpreadsWorkersAcrossMirrors(t *testing.T) {
	pool := newSourcePool(mirrorsFor("http://a.test/f", "http://b.test/f", "http://c.test/f"), 7)
	counts := map[int]int{}
	for worker := 0; worker < 7; worker++ {
		_, idx, ok := pool.pick(worker)
		if !ok {
			t.Fatal("pick failed with every mirror healthy")
		}
		counts[idx]++
	}
	// 7 workers over 3 mirrors: nobody should be carrying more than one extra.
	for idx, n := range counts {
		if n < 2 || n > 3 {
			t.Fatalf("mirror %d got %d workers, want 2 or 3", idx, n)
		}
	}
}

func TestSourcePoolRetiresFailingMirrorAndMigratesWorkers(t *testing.T) {
	pool := newSourcePool(mirrorsFor("http://a.test/f", "http://b.test/f"), 4)

	_, bad, _ := pool.pick(0)
	for i := 0; i < maxSourceFailures-1; i++ {
		if pool.fail(bad) {
			t.Fatalf("mirror retired after %d failures, want %d", i+1, maxSourceFailures)
		}
	}
	if !pool.fail(bad) {
		t.Fatalf("mirror should retire on failure %d", maxSourceFailures)
	}
	if pool.alive() != 1 {
		t.Fatalf("alive = %d, want 1", pool.alive())
	}

	// Every worker, including those assigned to the dead mirror, must now be
	// handed the survivor.
	for worker := 0; worker < 4; worker++ {
		_, idx, ok := pool.pick(worker)
		if !ok {
			t.Fatalf("worker %d got no mirror while one is still healthy", worker)
		}
		if idx == bad {
			t.Fatalf("worker %d was handed the retired mirror", worker)
		}
	}
}

func TestSourcePoolSuccessClearsFailureStreak(t *testing.T) {
	pool := newSourcePool(mirrorsFor("http://a.test/f", "http://b.test/f"), 2)
	for i := 0; i < maxSourceFailures*3; i++ {
		pool.fail(0)
		pool.succeed(0)
	}
	if pool.alive() != 2 {
		t.Fatal("intermittent failures must not retire a mirror")
	}
}

func TestSourcePoolNeverRetiresTheLastMirror(t *testing.T) {
	pool := newSourcePool(mirrorsFor("http://a.test/f"), 2)
	for i := 0; i < maxSourceFailures*3; i++ {
		pool.fail(0)
	}
	if _, _, ok := pool.pick(0); !ok {
		t.Fatal("the only mirror was retired, leaving nowhere to send work")
	}
}

func TestVerifyRangeStaysInsideTheFile(t *testing.T) {
	for _, size := range []int64{1, 1024, verifyWindow - 1, verifyWindow, 5 << 30} {
		start, length := verifyRange(size)
		if start < 0 || length <= 0 || start+length > size {
			t.Fatalf("size %d: window [%d,%d) is outside the file", size, start, start+length)
		}
		if size >= verifyWindow && length != verifyWindow {
			t.Fatalf("size %d: window length %d, want %d", size, length, verifyWindow)
		}
	}
}

// rangeServer serves body with full Range support via http.ServeContent.
func rangeServer(t *testing.T, body []byte) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "f", time.Time{}, bytes.NewReader(body))
	}))
	t.Cleanup(server.Close)
	return server
}

func TestProbeSourcesDropsMirrorServingDifferentBytes(t *testing.T) {
	size := 256 << 10
	good := make([]byte, size)
	rand.New(rand.NewSource(1)).Read(good)

	// Same length, same first 64 KiB, different in the middle -- exactly the
	// case a content-length check cannot catch.
	impostor := append([]byte(nil), good...)
	impostor[size/2] ^= 0xff

	primary := rangeServer(t, good)
	twin := rangeServer(t, append([]byte(nil), good...))
	fake := rangeServer(t, impostor)

	client := newHTTPClient(4, defaultBufferSize)
	sources, info, err := probeSources(context.Background(), client, []string{primary.URL, twin.URL, fake.URL})
	if err != nil {
		t.Fatalf("probeSources: %v", err)
	}
	if info.size != int64(size) {
		t.Fatalf("size = %d, want %d", info.size, size)
	}
	if len(sources) != 2 {
		t.Fatalf("got %d mirrors, want 2 (the impostor must be dropped)", len(sources))
	}
	for _, s := range sources {
		if s.url == fake.URL {
			t.Fatal("a mirror serving different bytes was accepted")
		}
	}
}

// runMultiSource wires up a downloader across the given mirrors and returns
// the bytes it wrote.
func runMultiSource(t *testing.T, urls []string, size int64, workers int) ([]byte, error) {
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

	sources := mirrorsFor(urls...)
	d := &downloader{
		clients:    newTCPClientSet(workers, defaultBufferSize),
		outputPath: out,
		statePath:  out + ".pearl",
		file:       file,
		totalSize:  size,
		retries:    5,
		sources:    newSourcePool(sources, workers),
		pipe:       newPipeline(defaultBufferSize, defaultPipelineDepth),
		co:         newCoordinator(splitEvenly(size, workers)),
		meter:      &atomic.Int64{},
	}
	for i := 0; i < workers; i++ {
		stat := &workerStat{}
		stat.source.Store(-1)
		d.stats = append(d.stats, stat)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.run(ctx); err != nil {
		return nil, err
	}
	if left := d.co.remaining(); left != 0 {
		t.Fatalf("%d bytes left unfetched", left)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	return got, nil
}

func TestMultiSourceDownloadAssemblesTheFileCorrectly(t *testing.T) {
	content := make([]byte, 3<<20)
	rand.New(rand.NewSource(7)).Read(content)

	a := rangeServer(t, content)
	b := rangeServer(t, append([]byte(nil), content...))
	c := rangeServer(t, append([]byte(nil), content...))

	got, err := runMultiSource(t, []string{a.URL, b.URL, c.URL}, int64(len(content)), 6)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("assembled file does not match the source content")
	}
}

// The point of mirroring: one server falling over should cost its share of
// the connections, not the download.
func TestDownloadSurvivesAMirrorThatIsAlwaysBroken(t *testing.T) {
	content := make([]byte, 2<<20)
	rand.New(rand.NewSource(11)).Read(content)

	good := rangeServer(t, content)
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer broken.Close()

	got, err := runMultiSource(t, []string{good.URL, broken.URL}, int64(len(content)), 6)
	if err != nil {
		t.Fatalf("download should have failed over to the healthy mirror: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("assembled file does not match the source content")
	}
}
