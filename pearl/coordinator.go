package main

import "sync"

// minStealBytes is the smallest amount of outstanding work worth splitting.
// Stealing from a segment with less than this left would hand the thief a
// range too small to amortize a fresh TCP connection and TLS handshake, and
// would let a nearly-finished download shred itself into thousands of
// segments.
const minStealBytes = 4 << 20

// segment is a half-open-at-the-end byte range [start, end] of the output
// file, plus the position of the next byte still needed. Every field is
// guarded by the owning coordinator's mutex.
type segment struct {
	id    int
	start int64
	pos   int64 // next byte to fetch; pos > end means the segment is complete
	end   int64 // inclusive; shrinks when another worker steals the tail
	owner int   // worker id currently fetching this segment, or -1
	// writing is the size of the chunk the owner is writing to disk right
	// now. Those bytes are off limits to thieves: a steal that landed inside
	// an in-flight write would hand the same offsets to two connections.
	writing int64
}

func (s *segment) remaining() int64 {
	if s.pos > s.end {
		return 0
	}
	return s.end - s.pos + 1
}

// segView is a lock-free copy of a segment, handed to the display and the
// state writer so neither has to hold the coordinator lock while formatting
// or doing I/O.
type segView struct {
	id, owner       int
	start, pos, end int64
}

// coordinator owns every byte range in the download. Workers do not own
// their ranges: they borrow one, and any range still in flight can have its
// tail taken away by a worker that has run out of work. A single mutex
// guards everything; it is taken once per 512 KiB written, which is far too
// infrequent to contend.
type coordinator struct {
	mu       sync.Mutex
	segments []*segment
	steals   int
}

func newCoordinator(views []segView) *coordinator {
	c := &coordinator{segments: make([]*segment, 0, len(views))}
	for i, v := range views {
		c.segments = append(c.segments, &segment{
			id:    i,
			start: v.start,
			pos:   v.pos,
			end:   v.end,
			owner: -1,
		})
	}
	return c
}

// splitEvenly lays out size bytes as parts contiguous ranges.
func splitEvenly(size int64, parts int) []segView {
	if parts < 1 {
		parts = 1
	}
	if int64(parts) > size {
		parts = int(size)
	}
	views := make([]segView, 0, parts)
	base, extra, start := size/int64(parts), size%int64(parts), int64(0)
	for i := 0; i < parts; i++ {
		length := base
		if int64(i) < extra {
			length++
		}
		views = append(views, segView{start: start, pos: start, end: start + length - 1})
		start += length
	}
	return views
}

// next hands worker a range to fetch. It prefers a segment nobody has
// claimed yet; when there are none it steals the back half of whichever
// in-flight segment has the most bytes left, which is by definition the one
// making the least progress. Returns nil when there is nothing left to do.
func (c *coordinator) next(worker int) *segment {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, s := range c.segments {
		if s.owner == -1 && s.remaining() > 0 {
			s.owner = worker
			return s
		}
	}

	// Steal from whichever connection has the most bytes still to fetch --
	// by definition the one making the least progress. Only the bytes past
	// its in-flight write are up for grabs.
	var victim *segment
	var most int64
	for _, s := range c.segments {
		if s.owner == -1 {
			continue
		}
		if available := s.end - (s.pos + s.writing) + 1; available > most {
			most, victim = available, s
		}
	}
	if victim == nil || most < minStealBytes {
		return nil
	}

	// Split at the midpoint of what the victim has *left*, not of its whole
	// range: the bytes before victim.pos are already on disk. Taking the back
	// half means the victim keeps a contiguous prefix it is already streaming,
	// so its in-flight connection stays useful.
	split := victim.pos + victim.writing + most/2
	stolen := &segment{
		id:    len(c.segments),
		start: split,
		pos:   split,
		end:   victim.end,
		owner: worker,
	}
	victim.end = split - 1
	c.segments = append(c.segments, stolen)
	c.steals++
	return stolen
}

// window reports the segment's current position and end. Callers use it to
// decide what to request next; it must not be used to size a write, because
// the end can move between the read and the write (see begin).
func (c *coordinator) window(s *segment) (pos, end int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return s.pos, s.end
}

// begin reserves up to n bytes at the segment's write position and reports
// where to write them. Deciding the size and marking the bytes in flight
// happen under one lock, so a steal can neither shrink the segment inside
// the chunk being written nor push pos past end. allowed is 0 when the
// segment has nothing left to write, which is how a worker learns its tail
// was taken.
func (c *coordinator) begin(s *segment, n int64) (pos, allowed int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s.writing = 0
	allowed = s.end - s.pos + 1
	if allowed <= 0 {
		return 0, 0
	}
	if n < allowed {
		allowed = n
	}
	s.writing = allowed
	return s.pos, allowed
}

// commit records a reserved chunk as written and releases the reservation.
// It reports whether the segment is now exhausted -- either fully downloaded
// or shortened to nothing by a thief -- so the caller can drop the
// connection without a second lock acquisition.
func (c *coordinator) commit(s *segment, n int64) (exhausted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s.pos += n
	s.writing = 0
	return s.pos > s.end
}

// abort drops a reservation whose write failed, leaving pos where it was so
// the bytes are retried rather than skipped.
func (c *coordinator) abort(s *segment) {
	c.mu.Lock()
	s.writing = 0
	c.mu.Unlock()
}

// release returns a segment to the pool after a failure so another worker
// can pick up whatever is left of it.
func (c *coordinator) release(s *segment) {
	c.mu.Lock()
	s.owner = -1
	c.mu.Unlock()
}

func (c *coordinator) snapshot(dst []segView) ([]segView, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	dst = dst[:0]
	for _, s := range c.segments {
		dst = append(dst, segView{id: s.id, owner: s.owner, start: s.start, pos: s.pos, end: s.end})
	}
	return dst, c.steals
}

// remaining is the number of bytes the whole download still needs.
func (c *coordinator) remaining() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var total int64
	for _, s := range c.segments {
		total += s.remaining()
	}
	return total
}
