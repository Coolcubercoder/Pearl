# pearl

A fast command-line downloader written in Go.

pearl splits a download across many HTTP connections — optionally spread over
several mirrors — shows you exactly what each one is doing, moves work away
from connections that stall, and checkpoints its progress so an interrupted
transfer resumes instead of starting over.

```
pearl ubuntu-26.04.1-desktop-arm64.iso  5.62 GiB  6 connections  across 3 mirrors

  █████████████░░░░░░░░   45.3%  2.55 GiB / 5.62 GiB  112.4 MiB/s  ETA 00:28

  #   mirror   range         progress                     rate  state
  0   cdimage  0B→960M       ███████░░░░░░░░  35.2%  9.31 MiB/s  downloading
  1   mirrors  960M→1.9G     ████████████░░░  60.9%  24.1 MiB/s  downloading
  2   nl       1.9G→2.8G     ███████░░░░░░░░  35.2%  9.28 MiB/s  downloading
  3   cdimage  2.8G→3.8G     ██░░░░░░░░░░░░░   8.1%  1.02 MiB/s  retrying   unexpected EOF
  4   mirrors  3.8G→4.7G     ███████░░░░░░░░  35.2%  9.30 MiB/s  downloading
  5   nl       4.7G→5.6G     ██████████████░  70.3%  31.7 MiB/s  downloading
  2 ranges rebalanced from slow connections
```

The `mirror` column only appears when you give it more than one URL.

## Install

Requires Go 1.27+. One dependency, `github.com/quic-go/quic-go`, for the
optional HTTP/3 transport; `go build` fetches it.

```
git clone <repo> && cd pearl
go build -o pearl .
sudo cp pearl /usr/local/bin/pearl
```

## Usage

```
pearl -u URL [-u MIRROR ...] [-c concurrency] [-o output]
      [-r retries] [-fresh] [-plain] [-http3] [-buf size] [-pipeline n]
      [-max-speed rate]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-u` | URL to download; repeat or comma-separate to add mirrors (required) | — |
| `-c` | Number of concurrent ranged connections | `8` |
| `-o` | Output file path | basename of the URL path, or `download` |
| `-r` | Retry attempts per connection before giving up | `5` |
| `-fresh` | Ignore any saved `.pearl` state and restart from zero | `false` |
| `-plain` | Print plain progress lines instead of the live matrix | `false` |
| `-http3` | Try HTTP/3 over QUIC, falling back to TCP if it doesn't work | `false` |
| `-buf` | Read/write chunk size per connection (`512K`, `4M`, …), between 64K and 64M | `512K` |
| `-pipeline` | Chunks kept in flight per connection, between 2 and 16 | `3` |
| `-max-speed` | Cap the whole download's rate (`2M`, `500K`, …) | unlimited |

Exit codes: `0` success, `1` download error (checkpoint saved), `2` bad
usage, `130` interrupted with Ctrl-C (checkpoint saved).

### Examples

Download with the default 8 connections, named after the URL:

```
pearl -u https://cdimage.ubuntu.com/ubuntu/releases/26.04.1/release/ubuntu-26.04.1-desktop-arm64.iso
```

Sixteen connections, explicit output path:

```
pearl -u https://example.com/file.iso -o ~/Downloads/file.iso -c 16
```

Sixteen connections spread over three mirrors (repeated `-u`, or one
comma-separated `-u`, or any mix of the two):

```
pearl -c 16 -o ubuntu.iso \
      -u https://cdimage.ubuntu.com/.../ubuntu.iso \
      -u https://mirrors.kernel.org/.../ubuntu.iso \
      -u https://nl.releases.ubuntu.com/.../ubuntu.iso
```

Resume an interrupted download — the same command, unchanged:

```
pearl -u https://example.com/file.iso -o ~/Downloads/file.iso -c 16
```

Leave some bandwidth for everything else on the link:

```
pearl -u https://example.com/file.iso -max-speed 2M
```

Throw away a saved checkpoint and start over:

```
pearl -u https://example.com/file.iso -o ~/Downloads/file.iso -fresh
```

Log-friendly output for CI or a pipe:

```
pearl -u https://example.com/file.iso -plain
```

## Multi-source mirroring

Give pearl more than one URL for the same file and it spreads its connections
across them, round-robin. This is the one thing that actually beats a
per-connection or per-client rate limit: if a server caps you at 5 MB/s, more
connections to *that* server won't help, but three servers will give you
three times the cap.

It also makes a mirror going down survivable. Mirrors are picked per attempt,
not per range, so a mirror that fails three times in a row is retired for the
rest of the run and its connections migrate to the survivors — a range
started against a dead mirror finishes against a live one, keeping the bytes
it already wrote. The last remaining mirror is never retired.

```
pearl: dropping mirror nl after 3 consecutive failures (unexpected EOF)
```

### Mirrors have to prove they serve the same file

Assembling one file out of several servers is only safe if they are all
serving identical bytes, and matching content lengths do not establish that —
two builds of the same image are routinely the same size. Before a mirror is
allowed to contribute anything, pearl:

1. requires it to support range requests and report the **same total size**
   as the primary, then
2. reads a **64 KiB window from the middle** of both and requires them to
   match byte for byte.

The window is taken from the middle rather than the start because file
headers are the least distinguishing part of a file. A mirror that fails
either check is dropped with a message and the download continues without it:

```
pearl: skipping mirror https://slow.example/f.iso (different content at offset 2952790016)
```

This is a strong check, not a proof. Verify a checksum afterwards if the file
matters — that is the only thing that actually guarantees the result, with or
without mirroring.

## The progress matrix

One averaged progress bar tells you a download is slow. It doesn't tell you
*why*. pearl gives every connection its own row, so a single choking mirror,
a throttled route, or a socket that died and is backing off is obvious at a
glance.

Each row shows the byte range that connection currently owns, how far
through that range it is, its transfer rate, and its state — `connecting`,
`downloading`, `stealing`, `retrying`, `done`, or `failed`. A connection
that is retrying or has failed spends the rest of its line explaining why.

With mirrors, a `mirror` column shows which one each connection is currently
pulling from, and turns red when that mirror has been retired — so a
connection moving between servers is something you watch happen rather than
something you infer afterwards. Labels are the shortest form that still tells
the mirrors apart, falling back to numeric tags when the hostnames alone
can't (several ports on one host, say).

Rates are exponentially smoothed (70% history, 30% current sample), because
raw per-tick deltas jump around far too much to read. The matrix repaints
about eight times a second by rewinding over its own block, so it never
scrolls the terminal.

Worker rows are written with atomics and read without locks, so painting the
display can never block a transfer. When stdout is not a terminal — a pipe,
a log file, CI — or with `-plain`, pearl prints one appended progress line
every two seconds instead. `NO_COLOR` is honoured.

## Resuming

While a ranged download runs, pearl keeps a small JSON sidecar next to the
output — `<output>.pearl` — recording every byte range and how far each has
got. It is written atomically via a temp file and a rename, so it can never
be read back half-written.

Checkpoints are paced by **whichever comes first: 64 MiB written, or two
seconds**. Pacing on time alone would be wrong at speed, because each
checkpoint fsyncs the output and an fsync costs in proportion to the dirty
pages it flushes — measured here, 100 MiB takes ~70 ms to flush versus ~4 ms
to write into page cache. At 500 MB/s a two-second interval would flush a
gigabyte in one stall, long enough to back the pipeline up and close the TCP
receive window. The byte cap keeps each individual flush bounded however fast
the transfer runs; the time cap makes sure a trickle still gets checkpointed.

Crucially, the output file is **fsynced before each checkpoint**. State that
claimed bytes the kernel hadn't written yet would resume past a hole and
produce a silently corrupt file; flushing first means the sidecar can only
ever under-claim, never over-claim.

Interrupt with Ctrl-C and pearl finishes its in-flight writes, saves a
checkpoint, and exits `130`:

```
Progress saved to ubuntu.iso.pearl — rerun the same command to resume (2.55 GiB of 5.62 GiB done).
```

Rerunning the same command picks up where it left off. A lost network
connection or a hard kill resumes the same way, losing at most one
checkpoint interval of work. The sidecar is deleted once the download
completes.

### Resume safety

Before resuming, pearl checks that the remote file is still the one it
started downloading: the saved `ETag`, `Last-Modified` and total size must
all still match. If the download started with a validator and the server has
since stopped sending one, size alone is too weak to trust and pearl starts
over.

Validators are recorded and checked **per mirror**, because that is what they
are: an ETag is only meaningful to the server that issued it, so comparing
one mirror's against another would reject every resume. A mirror present in
both runs has to still be serving what it served before, and at least one
mirror has to overlap — resuming against an entirely new set of mirrors is
refused, since nothing then ties the bytes already on disk to the bytes about
to be fetched. Adding a mirror to a resumed download is fine.

Every range request also carries `If-Range`, using the validator issued by
the mirror it is sent to, so a server that changed the file mid-download
returns a full body rather than splicing new bytes into old ones — and pearl
only accepts `206 Partial Content`, so that surfaces as an error instead of a
corrupt output.

If anything has changed, pearl says so and restarts rather than producing a
file that is half of each version. Use `-fresh` to discard a checkpoint
deliberately.

You can resume with a different `-c` than you started with; the saved ranges
are redistributed across however many connections you ask for.

## Work stealing

Connections do not own their byte ranges for life. Every range is held by a
central coordinator, and a connection only borrows one.

When a connection finishes its range, instead of going idle it takes the
back half of whichever range has the most bytes left — by definition the
connection making the least progress. The victim keeps the contiguous prefix
it is already streaming, so its open connection stays useful, and it stops
as soon as it reaches its new, shorter end.

Ranges with less than 4 MiB outstanding are left alone. Below that, a fresh
TCP connection and TLS handshake cost more than the work they'd take on, and
a nearly-finished download would shred itself into thousands of tiny
requests.

The subtle part is doing this without corrupting the file. A steal shrinks a
range that another connection is actively writing into, so the two decisions
— *how many bytes may I write* and *where does my range end* — must be
atomic with respect to each other. pearl uses a reserve/commit protocol:
every chunk calls `begin` to reserve bytes at the range's write position
under one lock, writes them, then `commit`s. Bytes reserved by an in-flight
write are off limits to thieves, so a steal can never land inside a write
that is already happening. `abort` releases a reservation whose write failed
so the bytes are retried rather than skipped.

The lock is taken roughly once per 512 KiB written, which is far too
infrequent to contend, and is never held across I/O.

## Capping the rate

`-max-speed` limits how fast the transfer runs, taking the same spellings as
`-buf`, so `2M` means the same thing in both:

```
pearl -u https://example.com/file.iso -max-speed 2M
```

It is **one token bucket shared by every connection**, not one per
connection. That is the number people actually mean — what pearl takes off
the line in total — and a per-connection cap would silently multiply by `-c`,
making `-max-speed 2M -c 8` a 16 MB/s download. Sharing one bucket also keeps
the cap steady while ranges are stolen between connections and while mirrors
are retired, because none of that changes how many tokens exist.

The allowance is spent *after* each read rather than reserved before one.
Bytes that have arrived cannot be un-received — by the time `Read` returns
they are already off the network and in the socket buffer — so paying
afterwards is what actually throttles the transfer: the worker stops reading,
the socket buffer fills, the receive window closes, and the *server* slows
down. That is the same backpressure `-pipeline` is sized around. Reserving in
advance would add latency without changing how fast anything arrived.

Two details keep the edges sane. The bucket always holds at least one whole
`-buf` chunk, so a cap smaller than the chunk size runs slowly instead of
waiting forever for tokens that could never accumulate. And it starts full,
so a download short enough never to reach the cap finishes at full speed
rather than paying for a limit it was not going to hit.

Without the flag there is no limiter at all — the response body is passed
through unwrapped, so an unthrottled download keeps exactly the read path it
had.

## Disk and buffer behaviour

### The output file is really preallocated

`Truncate` sets a file's length without allocating anything, leaving it
sparse — so the filesystem has to find and allocate blocks *during* the
download, on the write path, scattered across the whole file. pearl reserves
the blocks up front instead (`fallocate` on Linux, `F_PREALLOCATE` on macOS),
moving that work to one call before any bytes arrive.

The difference is visible on a partly-downloaded file — logical size versus
blocks actually allocated:

```
                      logical    allocated
sparse (truncate):    128M       0B
pearl (preallocate):  128M       128M
```

Preallocation is best-effort: filesystems that don't support it just get the
old lazy behaviour, so it never fails a download.

### Buffers are pooled

Copy buffers are recycled across segments. Every segment, every retry and
every steal runs another copy, and at half a megabyte apiece a fresh
allocation each time is a steady stream of garbage for the collector to chase
during the transfer.

### Tuning the pipeline

Each connection reads in `-buf` sized chunks and keeps `-pipeline` of them in
flight between the socket and the disk. Their product is how much data one
connection can hold before the reader has to wait:

- **Bigger `-buf`** means proportionally fewer syscalls per byte, which sounds
  like it should be free speed and measurably isn't — see below. Raise it only
  if you have measured a reason to on your own hardware.
- **Deeper `-pipeline`** is slack that absorbs a slow write. With too little,
  a disk hiccup stops the reader, the socket buffer fills, the receive window
  closes, and the *server* stalls. This is the setting that matters if your
  destination disk is slower or burstier than your network.

The flags multiply with `-c`, so pearl warns if the three together would
reserve more than a gigabyte of buffers.

### What the profile says

Against an unthrottled loopback server pearl sustains **~3 GB/s**, and a CPU
profile attributes **83% of the time to raw syscalls** — socket reads and
`pwrite` to disk. The coordinator, buffer pool and display do not meaningfully
appear. At any real network speed pearl is nowhere near being the bottleneck,
which is worth knowing before reaching for more tuning: measured on the same
harness, raising `-buf` does not help — the smallest size tested is the
fastest, and 8 MiB chunks are slower than the default while allocating ~280 MB
per download for nothing.

### What pearl deliberately does not do

It doesn't set `SO_RCVBUF` on its sockets. That's the knob people reach for to
"fix" the bandwidth-delay product, but setting it explicitly on Linux
*disables* the kernel's receive-buffer auto-tuning, which is usually better
than any fixed value a downloader could guess. Leaving it alone lets the
kernel size the window to the path it actually measures.

## HTTP/3

`-http3` runs the transfer over QUIC instead of TCP. It is opt-in, and if the
QUIC handshake doesn't succeed — UDP blocked, no HTTP/3 on the server — pearl
says so once and falls back to TCP rather than failing:

```
pearl: HTTP/3 unavailable (timeout: no recent network activity), falling back to TCP
```

The reachability check is separate from the mirror probe on purpose: any HTTP
response over QUIC counts as reachable, *including a 404*, so a genuine HTTP
error is reported once instead of being pointlessly retried over TCP.

**Each worker gets its own QUIC connection.** This is the part that matters
and the part that is easy to get wrong. An `http3.Transport` pools
connections by host, so the obvious implementation — one shared transport —
puts every worker on a single QUIC connection. Their *streams* are
independent, which is the head-of-line blocking fix people want from HTTP/3,
but they all sit behind one congestion controller and share one bandwidth
allocation. That is exactly what `-c` exists to defeat, so pearl builds one
transport, one QUIC connection and one UDP socket per worker. There is a test
that asserts the server actually sees that many distinct connections.

pearl also raises QUIC's flow control windows well above the library
defaults (512 KB initial, 6 MB maximum per stream). Those are sized for web
traffic and become the binding constraint on a fat, high-latency path long
before the network does — a 6 MB window at 100 ms RTT caps a stream around
60 MB/s regardless of what the link can carry.

### Whether it will actually help you

Probably less than you'd expect, and it's off by default for that reason.

The head-of-line blocking that HTTP/3 fixes is a **cross-stream** problem: a
lost packet stalls every stream sharing that TCP connection. That is why it
hurts HTTP/2, which multiplexes everything onto one connection. pearl already
disables HTTP/2 and gives every worker its own TCP connection, so a drop on
connection 3 stalls connection 3 and nothing else — and work stealing then
drains its remaining bytes to whichever connections are healthy. The
micro-freeze on lossy Wi-Fi is largely designed out already.

What QUIC genuinely adds here is faster handshakes, better loss recovery
within a single stream, and connection migration across network changes.
Real, but modest for a handful of long-lived bulk transfers. Against that:
UDP is frequently deprioritised or throttled more aggressively than TCP on
consumer ISPs and corporate networks, and userspace QUIC costs noticeably
more CPU per byte than kernel TCP, which can itself become the ceiling on a
fast link.

Measure it on your own network and servers before assuming it's faster. For
getting past a server that throttles you, mirrors are the far bigger lever.

## How it works

1. **Probe.** A `HEAD` request establishes content length and whether the
   server advertises `Accept-Ranges: bytes`, falling back to a one-byte
   ranged `GET` — plenty of servers reject `HEAD`, or fail to advertise
   `Accept-Ranges` while honouring `Range` perfectly well. Redirects are
   resolved once here so the range requests skip the hops. With several URLs,
   the first one that answers becomes the primary and defines the file; the
   rest are probed and content-checked against it before being admitted.

2. **Ranged path.** The output file is pre-allocated and split into `-c`
   roughly equal ranges, each fetched on its own connection. HTTP/2 is
   disabled deliberately: it would multiplex every request onto a single TCP
   connection and one congestion window, serializing the very concurrency we
   are trying to get. Under `-http3` the same reasoning produces one QUIC
   connection per worker rather than one shared one.

3. **Pipelined I/O.** Within each connection, network reads and disk writes
   run on separate goroutines connected by a small buffer pool, three
   buffers deep. The read for chunk N+1 proceeds while chunk N is still
   being written, instead of the two alternating serially. Writes go through
   `WriteAt`, so all connections share one file handle with no seeking. Under
   `-max-speed` the shared token bucket sits on the read side of this loop,
   throttling by declining to read rather than by discarding anything.

4. **Rebalancing.** As described above — idle connections steal from the
   slowest.

5. **Retries and failover.** A failed connection retries with exponential
   backoff (250ms doubling to a 5s cap), resuming from its range's current
   position rather than restarting the range. A connection that dies at 90%
   re-fetches only the last 10%. Each attempt re-picks its mirror, so a
   retry after a mirror is retired lands somewhere healthy. Past its retry
   budget, the range is handed back to the coordinator so the checkpoint
   still describes it, and the download exits non-zero — resumable.

   Ranges and mirrors are deliberately owned by different components: the
   coordinator knows nothing about servers and the source pool knows nothing
   about byte ranges. That separation is what makes "start a range on one
   mirror, finish it on another" fall out for free instead of needing a
   special case.

6. **Sequential fallback.** If the server won't serve ranges, or hides the
   content length, pearl streams the file on a single connection using the
   same pipelined read/write path — and the same rate limiter. There is
   nothing to rebalance and nothing safe to resume, so no sidecar is written.

## Notes

- Parallel connections only help when a single TCP stream can't fill the
  available bandwidth — high latency, packet loss, or per-connection server
  throttling. On a link already saturated by one connection, `-c` won't make
  much difference.
- Rebalancing helps when connections are *unevenly* slow: one bad mirror,
  one throttled route. It cannot speed up a uniformly slow link.
- Mirrors are the answer to *per-server* throttling specifically. If the
  bottleneck is your own link rather than any one server, extra mirrors add
  nothing — they cannot manufacture bandwidth you don't have.
- HTTP/3 is off by default and worth measuring before you trust it. See
  [HTTP/3](#http3) above for why it is unlikely to be the win it sounds like.
- Very high `-c` values are often counterproductive. Many servers rate-limit
  per connection but also cap connections per client, and past a dozen or so
  you are mostly adding handshakes.
- `-max-speed` caps the download as a whole, so it does not need adjusting
  when you change `-c` or add mirrors.

## Tests

```
go test ./...
go test -race -count=2 ./...
```

The suite concentrates on the coordinator and the source pool, where the
concurrency and correctness bugs live: that initial splits tile the file
exactly, that stealing under 16 workers with randomized chunk sizes never
leaves a gap or an overlap, that a steal never lands inside an in-flight
write, that a victim is never left with an empty range, that ranges under the
threshold are never stolen, and that state files round-trip with their
per-mirror resume guards intact.

Mirroring is covered end to end against local servers: a mirror serving
different bytes at the same content length is rejected at probe time, a
mirror that dies mid-transfer is retired and its work finishes elsewhere, and
the assembled file is compared byte for byte against the source in both
cases.

The I/O path is covered too: chunk round-tripping at several pipeline depths,
read and write error propagation, buffer reuse (via an allocation count),
size-flag parsing, and that preallocation reserves blocks while a resume never
truncates what is already on disk. Checkpoint pacing is tested from both
ends — that a fast transfer checkpoints on the byte threshold well before the
two-second cap, and that a trickle still waits for and then hits the time cap.

HTTP/3 is tested against a real QUIC server on loopback — a full ranged
download compared byte for byte, plus an assertion that the server saw one
distinct QUIC connection per worker rather than one shared between them.

### Benchmarks

```
go test -bench . -benchmem
```

They run against an unthrottled loopback server, so they measure what pearl
costs rather than what a network costs — useful for catching a regression,
useless as a prediction of download speed.

| Benchmark | What it answers |
|-----------|-----------------|
| `BenchmarkDownload` | Throughput at 4, 8 and 16 connections |
| `BenchmarkBufferSize` | Whether `-buf` moves throughput (measured: it doesn't) |
| `BenchmarkCheckpointCost` | What checkpointing costs a whole download |
| `BenchmarkCheckpointSync` | What one `fsync` costs while workers are writing |

`BenchmarkCheckpointSync` is the one that drove the byte-paced checkpoint.
Writing 100 MiB across eight workers runs at roughly 23 GB/s into page cache
and 1.8 GB/s if you `fsync` it — an order of magnitude, and the gap is
proportional to how much has been written since the last flush. That is the
whole argument for capping a checkpoint on bytes rather than on time.
