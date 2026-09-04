package main

import (
	"fmt"
	"math"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// Worker states, ordered so the display can colour them by severity.
const (
	stateIdle int32 = iota
	stateConnecting
	stateDownloading
	stateStealing
	stateRetrying
	stateThrottled
	stateDone
	stateFailed
)

var stateNames = map[int32]string{
	stateIdle:        "idle",
	stateConnecting:  "connecting",
	stateDownloading: "downloading",
	stateStealing:    "stealing",
	stateRetrying:    "retrying",
	stateThrottled:   "throttled",
	stateDone:        "done",
	stateFailed:      "failed",
}

const (
	styleReset  = "\x1b[0m"
	styleDim    = "\x1b[2m"
	styleBold   = "\x1b[1m"
	styleCyan   = "\x1b[36m"
	styleGreen  = "\x1b[32m"
	styleYellow = "\x1b[33m"
	styleRed    = "\x1b[31m"
)

// workerStat is one row of the matrix. Workers write it with atomics; the
// display reads it without ever blocking a transfer.
type workerStat struct {
	bytes  atomic.Int64  // bytes this worker has written this session
	state  atomic.Int32  // one of the state* constants
	speed  atomic.Uint64 // float64 bits: smoothed bytes/sec
	source atomic.Int32  // index into the source pool, or -1 before the first attempt
	note   atomic.Value  // string: last error, shown when retrying or failed

	lastBytes int64 // display-owned; not shared
}

func (w *workerStat) setState(s int32) { w.state.Store(s) }

func (w *workerStat) setSource(idx int) { w.source.Store(int32(idx)) }

func (w *workerStat) setNote(err error) {
	if err != nil {
		w.note.Store(err.Error())
	}
}

func (w *workerStat) noteText() string {
	if v, ok := w.note.Load().(string); ok {
		return v
	}
	return ""
}

// span is a piece of a rendered line with its own style. Lines are assembled
// from spans so the display can clip to the terminal width by counting real
// characters, without ever slicing through an ANSI escape sequence.
type span struct {
	text  string
	style string
}

func renderSpans(spans []span, width int, color bool) string {
	var b strings.Builder
	used := 0
	for _, s := range spans {
		if used >= width {
			break
		}
		text := s.text
		if runes := []rune(text); len(runes) > width-used {
			text = string(runes[:width-used])
		}
		used += len([]rune(text))
		if color && s.style != "" {
			b.WriteString(s.style)
			b.WriteString(text)
			b.WriteString(styleReset)
		} else {
			b.WriteString(text)
		}
	}
	return b.String()
}

func bar(width int, fraction float64, color bool) []span {
	if width < 1 {
		width = 1
	}
	if math.IsNaN(fraction) || fraction < 0 {
		fraction = 0
	}
	if fraction > 1 {
		fraction = 1
	}
	filled := int(fraction * float64(width))
	if filled > width {
		filled = width
	}
	return []span{
		{text: strings.Repeat("█", filled), style: styleCyan},
		{text: strings.Repeat("░", width-filled), style: styleDim},
	}
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit && exp < 4; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(n)/float64(div), "KMGTP"[exp])
}

// compactBytes is humanBytes with a fixed, narrow width, for the segment
// boundary columns where space is tight.
func compactBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit && exp < 4; m /= unit {
		div *= unit
		exp++
	}
	value := float64(n) / float64(div)
	if value >= 100 {
		return fmt.Sprintf("%.0f%c", value, "KMGTP"[exp])
	}
	return fmt.Sprintf("%.1f%c", value, "KMGTP"[exp])
}

func humanRate(bytesPerSecond float64) string {
	if bytesPerSecond <= 0 || math.IsNaN(bytesPerSecond) || math.IsInf(bytesPerSecond, 0) {
		return "—"
	}
	return humanBytes(int64(bytesPerSecond)) + "/s"
}

func humanDuration(d time.Duration) string {
	if d < 0 || d > 99*time.Hour {
		return "--:--"
	}
	total := int(d.Seconds())
	hours, minutes, seconds := total/3600, (total/60)%60, total%60
	if hours > 0 {
		return fmt.Sprintf("%d:%02d:%02d", hours, minutes, seconds)
	}
	return fmt.Sprintf("%02d:%02d", minutes, seconds)
}

// display renders the progress matrix. On a terminal it repaints a block of
// lines in place; anywhere else (a pipe, a log file, CI) it degrades to one
// plain line every couple of seconds.
type display struct {
	out     *os.File
	tty     bool
	color   bool
	plain   bool
	name    string
	total   int64
	resumed bool

	co      *coordinator
	sources *sourcePool
	stats   []*workerStat
	meter   *atomic.Int64
	started time.Time

	lines     int // lines painted by the previous frame
	views     []segView
	lastTime  time.Time
	lastTotal int64
	speed     float64
	lastPlain time.Time
}

func newDisplay(out *os.File, name string, total int64, resumed bool, co *coordinator, sources *sourcePool, stats []*workerStat, meter *atomic.Int64, plain bool) *display {
	_, _, tty := terminalSize(out)
	return &display{
		out:       out,
		tty:       tty && !plain,
		color:     tty && !plain && os.Getenv("NO_COLOR") == "",
		plain:     plain || !tty,
		name:      name,
		total:     total,
		resumed:   resumed,
		co:        co,
		sources:   sources,
		stats:     stats,
		meter:     meter,
		started:   time.Now(),
		lastTime:  time.Now(),
		lastTotal: meter.Load(),
	}
}

func (d *display) run(done <-chan struct{}) {
	if d.tty {
		fmt.Fprint(d.out, "\x1b[?25l") // hide cursor while we repaint
	}
	ticker := time.NewTicker(120 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			d.frame()
		case <-done:
			return
		}
	}
}

// finish paints one last frame and hands the terminal back. The final frame
// is left on screen so the user can see how the work was distributed.
func (d *display) finish() {
	d.frame()
	if d.tty {
		fmt.Fprint(d.out, "\x1b[?25h")
	}
}

// sample updates the smoothed global and per-worker rates. An exponential
// moving average keeps the numbers readable: raw per-tick deltas jump around
// far too much to spot a genuinely slow connection.
func (d *display) sample() {
	now := time.Now()
	elapsed := now.Sub(d.lastTime).Seconds()
	if elapsed < 0.05 {
		return
	}
	d.lastTime = now

	current := d.meter.Load()
	instant := float64(current-d.lastTotal) / elapsed
	d.lastTotal = current
	d.speed = smooth(d.speed, instant)

	for _, stat := range d.stats {
		bytes := stat.bytes.Load()
		rate := float64(bytes-stat.lastBytes) / elapsed
		stat.lastBytes = bytes
		previous := math.Float64frombits(stat.speed.Load())
		stat.speed.Store(math.Float64bits(smooth(previous, rate)))
	}
}

func smooth(previous, instant float64) float64 {
	if previous == 0 {
		return instant
	}
	return previous*0.7 + instant*0.3
}

func (d *display) frame() {
	d.sample()
	if !d.tty {
		d.plainFrame()
		return
	}

	width, height, _ := terminalSize(d.out)
	if width < 40 {
		// Unknown or absurdly narrow geometry: assume a classic 80 columns.
		width = 80
	}
	width--

	lines := d.compose(width, height)

	var b strings.Builder
	if d.lines > 0 {
		fmt.Fprintf(&b, "\x1b[%dA", d.lines) // rewind to the top of our block
	}
	for _, line := range lines {
		b.WriteString("\x1b[2K") // clear the whole line before repainting it
		b.WriteString(line)
		b.WriteString("\n")
	}
	// A shrinking block (workers finishing) would leave stale rows behind.
	for i := len(lines); i < d.lines; i++ {
		b.WriteString("\x1b[2K\n")
	}
	if d.lines > len(lines) {
		fmt.Fprintf(&b, "\x1b[%dA", d.lines-len(lines))
		d.lines = len(lines)
	} else {
		d.lines = len(lines)
	}
	fmt.Fprint(d.out, b.String())
}

func (d *display) compose(width, height int) []string {
	done := d.meter.Load()
	lines := []string{d.headerLine(width), "", d.summaryLine(width, done), ""}

	if d.co == nil {
		return lines
	}

	var steals int
	d.views, steals = d.co.snapshot(d.views)

	// Reserve room for the header block plus a trailing line, so the matrix
	// never scrolls the terminal and breaks our cursor arithmetic.
	maxRows := len(d.stats)
	if height > 0 && height-len(lines)-3 < maxRows {
		maxRows = height - len(lines) - 3
	}
	if maxRows < 1 {
		maxRows = 1
	}

	header := fmt.Sprintf("  %-3s ", "#")
	if w := d.sourceColumn(); w > 0 {
		header += padLabel("mirror", w)
	}
	header += fmt.Sprintf("%-13s %-*s %6s %11s  %s", "range", d.bars(width), "progress", "", "rate", "state")
	lines = append(lines, renderSpans([]span{{text: header, style: styleDim}}, width, d.color))

	shown := 0
	for i, stat := range d.stats {
		if shown >= maxRows {
			break
		}
		lines = append(lines, d.workerLine(width, i, stat))
		shown++
	}
	if hidden := len(d.stats) - shown; hidden > 0 {
		lines = append(lines, renderSpans([]span{
			{text: fmt.Sprintf("  … %d more connections", hidden), style: styleDim},
		}, width, d.color))
	}
	if steals > 0 {
		lines = append(lines, renderSpans([]span{
			{text: fmt.Sprintf("  %d range%s rebalanced from slow connections", steals, plural(steals)), style: styleDim},
		}, width, d.color))
	}
	return lines
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// sourceColumn is the width of the mirror column. It collapses to zero for a
// single-source download, where the column would repeat the same name on
// every row and buy nothing.
func (d *display) sourceColumn() int {
	if d.sources == nil || d.sources.count() < 2 {
		return 0
	}
	w := 0
	for i := 0; i < d.sources.count(); i++ {
		if n := len([]rune(d.sources.at(i).label)); n > w {
			w = n
		}
	}
	if w > maxLabel {
		w = maxLabel
	}
	return w
}

// bars is the progress bar width, after the mirror column has taken its share.
func (d *display) bars(width int) int { return barWidth(width - d.sourceColumn()) }

func padLabel(text string, w int) string {
	if runes := []rune(text); len(runes) > w {
		text = string(runes[:w])
	}
	return fmt.Sprintf("%-*s ", w, text)
}

func barWidth(width int) int {
	w := width - 58
	if w < 8 {
		w = 8
	}
	if w > 28 {
		w = 28
	}
	return w
}

func (d *display) headerLine(width int) string {
	parts := []span{
		{text: "pearl ", style: styleBold},
		{text: d.name, style: styleBold},
	}
	if d.total > 0 {
		parts = append(parts, span{text: "  " + humanBytes(d.total), style: styleDim})
	}
	parts = append(parts, span{text: fmt.Sprintf("  %d connections", len(d.stats)), style: styleDim})
	if d.sources != nil && d.sources.count() > 1 {
		live := d.sources.alive()
		text := fmt.Sprintf("  across %d mirrors", d.sources.count())
		style := styleDim
		if live < d.sources.count() {
			text = fmt.Sprintf("  across %d of %d mirrors", live, d.sources.count())
			style = styleYellow
		}
		parts = append(parts, span{text: text, style: style})
	}
	if d.resumed {
		parts = append(parts, span{text: "  resumed", style: styleYellow})
	}
	return renderSpans(parts, width, d.color)
}

func (d *display) summaryLine(width int, done int64) string {
	if d.total <= 0 {
		return renderSpans([]span{
			{text: fmt.Sprintf("  %s downloaded   %s   %s elapsed",
				humanBytes(done), humanRate(d.speed), humanDuration(time.Since(d.started)))},
		}, width, d.color)
	}

	fraction := float64(done) / float64(d.total)
	eta := "--:--"
	if d.speed > 0 && done < d.total {
		eta = humanDuration(time.Duration(float64(d.total-done)/d.speed) * time.Second)
	} else if done >= d.total {
		eta = "00:00"
	}

	parts := []span{{text: "  "}}
	parts = append(parts, bar(d.bars(width), fraction, d.color)...)
	parts = append(parts, span{text: fmt.Sprintf("  %5.1f%%", fraction*100), style: styleBold})
	parts = append(parts, span{text: fmt.Sprintf("  %s / %s", humanBytes(done), humanBytes(d.total))})
	parts = append(parts, span{text: fmt.Sprintf("  %11s", humanRate(d.speed)), style: styleGreen})
	parts = append(parts, span{text: "  ETA " + eta, style: styleDim})
	return renderSpans(parts, width, d.color)
}

func (d *display) workerLine(width, id int, stat *workerStat) string {
	state := stat.state.Load()
	view, active := d.viewFor(id)

	rangeText, fraction := "—", float64(0)
	if active {
		rangeText = compactBytes(view.start) + "→" + compactBytes(view.end+1)
		if size := view.end - view.start + 1; size > 0 {
			fraction = float64(view.pos-view.start) / float64(size)
		}
	} else if state == stateDone {
		fraction = 1
	}

	parts := []span{{text: fmt.Sprintf("  %-3d ", id), style: styleDim}}
	if w := d.sourceColumn(); w > 0 {
		label, style := "—", styleDim
		if idx := int(stat.source.Load()); idx >= 0 {
			label = d.sources.at(idx).label
			// A retired mirror stays on the row that last used it, in red, so
			// the reason a connection moved is visible rather than inferred.
			if d.sources.isDead(idx) {
				style = styleRed
			}
		}
		parts = append(parts, span{text: padLabel(label, w), style: style})
	}
	parts = append(parts, span{text: fmt.Sprintf("%-13s ", rangeText)})
	parts = append(parts, bar(d.bars(width), fraction, d.color)...)
	parts = append(parts, span{text: fmt.Sprintf(" %5.1f%%", fraction*100)})

	rate := math.Float64frombits(stat.speed.Load())
	if state != stateDownloading {
		rate = 0
	}
	parts = append(parts, span{text: fmt.Sprintf(" %11s  ", humanRate(rate))})
	parts = append(parts, span{text: stateNames[state], style: d.stateStyle(state)})

	// Only a stalled or broken connection gets to spend the rest of the line
	// explaining itself — that is exactly what the matrix is for.
	if note := stat.noteText(); note != "" && (state == stateRetrying || state == stateThrottled || state == stateFailed) {
		parts = append(parts, span{text: "  " + note, style: styleDim})
	}
	return renderSpans(parts, width, d.color)
}

func (d *display) stateStyle(state int32) string {
	switch state {
	case stateDownloading:
		return styleGreen
	case stateRetrying, stateThrottled:
		return styleYellow
	case stateFailed:
		return styleRed
	case stateDone:
		return styleDim
	default:
		return styleCyan
	}
}

func (d *display) viewFor(worker int) (segView, bool) {
	for _, v := range d.views {
		if v.owner == worker {
			return v, true
		}
	}
	return segView{}, false
}

// plainFrame is the non-terminal fallback: one appended line every couple of
// seconds, safe for logs and CI.
func (d *display) plainFrame() {
	if time.Since(d.lastPlain) < 2*time.Second {
		return
	}
	d.lastPlain = time.Now()
	done := d.meter.Load()
	if d.total > 0 {
		fmt.Fprintf(d.out, "%s / %s (%.1f%%) at %s\n",
			humanBytes(done), humanBytes(d.total), float64(done)*100/float64(d.total), humanRate(d.speed))
		return
	}
	fmt.Fprintf(d.out, "%s downloaded at %s\n", humanBytes(done), humanRate(d.speed))
}
