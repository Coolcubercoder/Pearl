package main

import (
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

// interval is a byte range some worker claimed to have written.
type interval struct{ start, end int64 }

// assertTiles checks that the intervals cover [0, size) exactly once: no
// gaps (missing bytes in the output) and no overlaps (two connections
// writing the same offset, which is how a work-stealing downloader
// corrupts a file).
func assertTiles(t *testing.T, intervals []interval, size int64) {
	t.Helper()
	sort.Slice(intervals, func(i, j int) bool { return intervals[i].start < intervals[j].start })
	var next int64
	for _, iv := range intervals {
		if iv.start != next {
			if iv.start > next {
				t.Fatalf("gap: bytes %d-%d never written", next, iv.start-1)
			}
			t.Fatalf("overlap: byte %d written twice (interval starts at %d)", iv.start, iv.start)
		}
		next = iv.end + 1
	}
	if next != size {
		t.Fatalf("covered %d bytes, want %d", next, size)
	}
}

func TestSplitEvenlyTilesFile(t *testing.T) {
	for _, size := range []int64{1, 7, 1024, 1<<20 + 3} {
		for _, parts := range []int{1, 3, 8, 64} {
			views := splitEvenly(size, parts)
			intervals := make([]interval, 0, len(views))
			for _, v := range views {
				if v.pos != v.start {
					t.Fatalf("fresh segment should start unread: %+v", v)
				}
				intervals = append(intervals, interval{v.start, v.end})
			}
			assertTiles(t, intervals, size)
		}
	}
}

// TestStealingPreservesCoverage runs many workers against one coordinator,
// letting them steal from each other while they "download", and verifies
// that the bytes they collectively wrote tile the file exactly once.
func TestStealingPreservesCoverage(t *testing.T) {
	const size = 512 << 20
	co := newCoordinator(splitEvenly(size, 4))

	var mu sync.Mutex
	var written []interval

	var wg sync.WaitGroup
	// More workers than initial segments, so the extras have nothing to
	// claim and must steal to make progress.
	for id := 0; id < 16; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id) + 1))
			for {
				seg := co.next(id)
				if seg == nil {
					return
				}
				for {
					pos, allowed := co.begin(seg, int64(rng.Intn(4<<20)+1))
					if allowed <= 0 {
						break
					}
					mu.Lock()
					written = append(written, interval{pos, pos + allowed - 1})
					mu.Unlock()
					if co.commit(seg, allowed) {
						break
					}
				}
				co.release(seg)
			}
		}(id)
	}
	wg.Wait()

	if remaining := co.remaining(); remaining != 0 {
		t.Fatalf("coordinator still reports %d bytes outstanding", remaining)
	}
	views, steals := co.snapshot(nil)
	if steals == 0 {
		t.Fatal("no steals occurred; the test did not exercise rebalancing")
	}
	segIntervals := make([]interval, 0, len(views))
	for _, v := range views {
		if v.pos != v.end+1 {
			t.Fatalf("segment %d left incomplete: pos=%d end=%d", v.id, v.pos, v.end)
		}
		segIntervals = append(segIntervals, interval{v.start, v.end})
	}
	// Both the segment table (what a checkpoint would record) and the actual
	// writes must tile the file.
	assertTiles(t, segIntervals, size)
	assertTiles(t, written, size)
}

// TestStealLeavesVictimUsableWork guards the split arithmetic: a steal must
// never hand the victim an empty or backwards range, and must never take
// bytes the victim already downloaded.
func TestStealNeverStrandsVictim(t *testing.T) {
	co := newCoordinator(splitEvenly(64<<20, 1))
	victim := co.next(0)
	pos, allowed := co.begin(victim, 10<<20)
	if pos != 0 || allowed != 10<<20 {
		t.Fatalf("begin returned (%d, %d), want (0, %d)", pos, allowed, int64(10<<20))
	}
	co.commit(victim, allowed)

	thief := co.next(1)
	if thief == nil {
		t.Fatal("expected a steal from a 54 MiB segment")
	}
	vpos, vend := co.window(victim)
	if vend < vpos {
		t.Fatalf("victim left with an empty range: pos=%d end=%d", vpos, vend)
	}
	if thief.start <= vpos {
		t.Fatalf("steal took bytes at/behind the victim's position: stole from %d, victim at %d", thief.start, vpos)
	}
	if thief.start != vend+1 {
		t.Fatalf("steal is not contiguous with the victim: victim ends %d, thief starts %d", vend, thief.start)
	}
}

// TestStealNeverLandsInsideInFlightWrite is the regression guard for the
// hazard that a naive read-end-then-write loop hits: if a steal may split
// inside the chunk a worker is currently writing, two connections end up
// owning the same offsets and the output silently corrupts.
func TestStealNeverLandsInsideInFlightWrite(t *testing.T) {
	co := newCoordinator(splitEvenly(64<<20, 1))
	victim := co.next(0)

	// A large chunk is in flight, covering the first half of the segment.
	pos, allowed := co.begin(victim, 32<<20)
	if allowed != 32<<20 {
		t.Fatalf("reserved %d bytes, want %d", allowed, int64(32<<20))
	}

	thief := co.next(1)
	if thief == nil {
		t.Fatal("expected a steal from the 32 MiB still available")
	}
	if thief.start < pos+allowed {
		t.Fatalf("steal at %d landed inside the in-flight write [%d, %d)", thief.start, pos, pos+allowed)
	}

	if exhausted := co.commit(victim, allowed); exhausted {
		t.Fatal("victim should still have work after committing its in-flight chunk")
	}
	vpos, vend := co.window(victim)
	if vpos > vend+1 {
		t.Fatalf("victim position %d overshot its end %d", vpos, vend)
	}
}

// TestNoStealBelowThreshold keeps a nearly-finished download from shredding
// itself into a swarm of tiny ranges.
func TestNoStealBelowThreshold(t *testing.T) {
	co := newCoordinator([]segView{{start: 0, pos: 0, end: minStealBytes - 2}})
	if seg := co.next(0); seg == nil {
		t.Fatal("first worker should get the only segment")
	}
	if seg := co.next(1); seg != nil {
		t.Fatalf("stole from a segment below the threshold: %+v", seg)
	}
}

func TestStateRoundTripAndResumeGuards(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin.pearl")
	original := &downloadState{
		Output:    "out.bin",
		TotalSize: 1000,
		Sources:   []stateSource{{URL: "http://a.test/f", ETag: `"v1"`}},
		Segments:  []stateSegment{{Start: 0, Pos: 400, End: 499}, {Start: 500, Pos: 500, End: 999}},
	}
	if err := original.save(path); err != nil {
		t.Fatalf("save: %v", err)
	}
	loaded, err := loadState(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := loaded.completed(); got != 400 {
		t.Fatalf("completed = %d, want 400", got)
	}

	mirrors := func(rawURL, etag string) []source {
		return []source{{url: rawURL, validator: validator{etag: etag}}}
	}

	if !loaded.resumable(1000, mirrors("http://a.test/f", `"v1"`)) {
		t.Fatal("should resume when the ETag still matches")
	}
	if loaded.resumable(1000, mirrors("http://a.test/f", `"v2"`)) {
		t.Fatal("must not resume onto a different ETag")
	}
	if loaded.resumable(2000, mirrors("http://a.test/f", `"v1"`)) {
		t.Fatal("must not resume when the size changed")
	}
	if loaded.resumable(1000, mirrors("http://a.test/f", "")) {
		t.Fatal("must not resume when the server stopped sending a validator")
	}
	// Nothing ties the bytes on disk to a mirror set we have never seen.
	if loaded.resumable(1000, mirrors("http://b.test/f", `"v1"`)) {
		t.Fatal("must not resume against an entirely new set of mirrors")
	}
	// A mirror added since the checkpoint is fine as long as one still overlaps.
	added := []source{
		{url: "http://a.test/f", validator: validator{etag: `"v1"`}},
		{url: "http://b.test/f", validator: validator{etag: `"other"`}},
	}
	if !loaded.resumable(1000, added) {
		t.Fatal("adding a mirror should not invalidate the checkpoint")
	}

	// A state file with no validator at all falls back to size alone.
	sizeOnly := &downloadState{
		TotalSize: 1000,
		Sources:   []stateSource{{URL: "http://a.test/f"}},
		Segments:  original.Segments,
	}
	if !sizeOnly.resumable(1000, mirrors("http://a.test/f", "")) {
		t.Fatal("size-only state should resume against a size-only server")
	}

	if _, err := loadState(filepath.Join(dir, "missing.pearl")); !os.IsNotExist(err) {
		t.Fatalf("missing state should report NotExist, got %v", err)
	}
}

func TestTotalFromContentRange(t *testing.T) {
	cases := map[string]struct {
		size int64
		ok   bool
	}{
		"bytes 0-0/1234": {1234, true},
		"bytes 0-99/100": {100, true},
		"bytes 0-0/*":    {0, false},
		"garbage":        {0, false},
		"":               {0, false},
	}
	for header, want := range cases {
		size, ok := totalFromContentRange(header)
		if size != want.size || ok != want.ok {
			t.Errorf("%q: got (%d, %v), want (%d, %v)", header, size, ok, want.size, want.ok)
		}
	}
}
