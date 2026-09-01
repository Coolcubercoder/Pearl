package main

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// clientSet hands each worker an HTTP client.
//
// Over TCP one shared client is correct: the transport pool already opens an
// independent TCP connection per worker, each with its own congestion window.
//
// Over HTTP/3 it is not. An http3.Transport pools QUIC connections by host,
// so a single shared transport would put every worker on one QUIC connection.
// Their streams would be independent -- that is the head-of-line blocking fix
// -- but they would all sit behind a single congestion controller and share
// one bandwidth allocation, which is precisely what -c exists to avoid. So
// HTTP/3 gets one transport, and therefore one QUIC connection and one UDP
// socket, per worker.
type clientSet struct {
	shared     *http.Client
	perWorker  []*http.Client
	transports []*http3.Transport
	http3      bool
}

func (c *clientSet) forWorker(worker int) *http.Client {
	if len(c.perWorker) == 0 {
		return c.shared
	}
	return c.perWorker[worker%len(c.perWorker)]
}

// probeClient is used for the pre-flight requests, before workers exist.
func (c *clientSet) probeClient() *http.Client {
	if c.shared != nil {
		return c.shared
	}
	return c.perWorker[0]
}

func (c *clientSet) Close() {
	for _, t := range c.transports {
		t.Close()
	}
}

func newTCPClientSet(concurrency, readBuffer int) *clientSet {
	return &clientSet{shared: newHTTPClient(concurrency, readBuffer)}
}

func newHTTPClient(concurrency, readBuffer int) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			// HTTP/2 multiplexes all requests to a host over one TCP connection,
			// which would serialize our "concurrent" range requests onto a single
			// congestion window. Disabling it forces one independent connection
			// per worker, so concurrency actually parallelizes throughput.
			TLSNextProto:          map[string]func(string, *tls.Conn) http.RoundTripper{},
			MaxIdleConns:          concurrency * 2,
			MaxIdleConnsPerHost:   concurrency * 2,
			MaxConnsPerHost:       concurrency * 2,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   15 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			DisableCompression:    true,
			// Match the transport's read buffering to our copy buffer size so
			// large transfers need fewer read syscalls per connection.
			ReadBufferSize: readBuffer,
		},
	}
}

// quicConfig tunes QUIC for bulk transfer. The library defaults (512 KB
// initial, 6 MB maximum stream window) are sized for web traffic and become
// the binding constraint on a fat, high-latency path long before the network
// does: a 6 MB window at 100ms RTT caps a stream at roughly 60 MB/s no matter
// what the link can carry.
func quicConfig() *quic.Config {
	return &quic.Config{
		HandshakeIdleTimeout:           15 * time.Second,
		MaxIdleTimeout:                 90 * time.Second,
		KeepAlivePeriod:                15 * time.Second,
		InitialStreamReceiveWindow:     4 << 20,
		MaxStreamReceiveWindow:         32 << 20,
		InitialConnectionReceiveWindow: 8 << 20,
		MaxConnectionReceiveWindow:     64 << 20,
	}
}

// quicReachable reports whether QUIC can be established to a host at all. Any
// HTTP response counts, whatever its status: a 404 over QUIC still proves the
// handshake succeeded and UDP is getting through, which is the only question
// being asked here.
func quicReachable(ctx context.Context, client *http.Client, rawURL string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodHead, rawURL, nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	response.Body.Close()
	return nil
}

// newHTTP3ClientSet builds one QUIC transport -- and so one QUIC connection
// and one UDP socket -- per worker. tlsConfig is nil outside tests.
func newHTTP3ClientSet(workers int, tlsConfig *tls.Config) *clientSet {
	set := &clientSet{http3: true}
	for i := 0; i < workers; i++ {
		transport := &http3.Transport{QUICConfig: quicConfig(), TLSClientConfig: tlsConfig}
		set.transports = append(set.transports, transport)
		set.perWorker = append(set.perWorker, &http.Client{Transport: transport})
	}
	return set
}
