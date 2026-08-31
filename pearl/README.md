# pearl

A fast, minimal command-line downloader written in Go. When the server
supports HTTP range requests, pearl splits the download into concurrent
chunks over multiple connections; otherwise it falls back to a single
streamed download. Reads and writes are pipelined per connection so
network I/O and disk I/O overlap instead of blocking on each other.

## Build

Requires Go 1.27+.

```
go build -o pearl .
```

## Install

```
sudo cp pearl /usr/local/bin/pearl
```

## Usage

```
pearl -u URL [-c concurrency] [-o output]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-u` | URL to download (required) | — |
| `-c` | Number of concurrent ranged connections | `8` |
| `-o` | Output file path | basename of the URL path, or `download` |

### Example

```
pearl -u https://cdimage.ubuntu.com/ubuntu/releases/26.04.1/release/ubuntu-26.04.1-desktop-arm64.iso \
      -o ~/Downloads/ubuntu-26.04.1-desktop-arm64.iso \
      -c 16
```

Output:

```
Downloaded 6039797760 / 6039797760 bytes (100.0%)
Saved /Users/you/Downloads/ubuntu-26.04.1-desktop-arm64.iso (6039797760 bytes) in 1m12s, average 79.61 MB/s
```

## How it works

1. Sends a `HEAD` request to determine content length and whether the
   server advertises `Accept-Ranges: bytes`.
2. **If ranges are supported**: pre-allocates the output file, splits it
   into `-c` roughly equal byte ranges, and downloads each range on its
   own HTTP connection (HTTP/2 is disabled so connections aren't
   multiplexed onto a single stream). Each worker's network reads and
   disk writes run on separate goroutines so a slow write never stalls
   the socket.
3. **If ranges aren't supported** (or content length is unknown): falls
   back to a single sequential download using the same pipelined
   read/write path.
4. Prints live progress and a final summary with elapsed time and
   average throughput.

## Notes

- Parallel connections only help when a single TCP stream can't fill
  the available bandwidth (high latency, packet loss, or per-connection
  server throttling). On a link that's already saturated by one
  connection, `-c` won't make much difference.
- Aborts with a non-zero exit code if any range download fails or the
  server response doesn't match the expected size.
