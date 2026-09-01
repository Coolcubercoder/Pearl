package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	mrand "math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
)

// selfSignedTLS returns a server TLS config and the matching client config
// that trusts it, for talking QUIC to a loopback test server.
func selfSignedTLS(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "pearl-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)

	server := &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}},
		NextProtos:   []string{http3.NextProtoH3},
	}
	client := &tls.Config{RootCAs: pool, NextProtos: []string{http3.NextProtoH3}}
	return server, client
}

// startHTTP3Server serves body over QUIC on loopback, recording the distinct
// QUIC connections it sees so a test can assert how many were opened.
func startHTTP3Server(t *testing.T, body []byte) (addr string, clientTLS *tls.Config, peers func() int) {
	t.Helper()
	serverTLS, clientTLS := selfSignedTLS(t)

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}

	var mu sync.Mutex
	seen := map[string]bool{}
	server := &http3.Server{
		TLSConfig: serverTLS,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			seen[r.RemoteAddr] = true
			mu.Unlock()
			http.ServeContent(w, r, "f.bin", time.Time{}, bytes.NewReader(body))
		}),
	}
	go server.Serve(conn)
	t.Cleanup(func() { server.Close(); conn.Close() })

	return conn.LocalAddr().String(), clientTLS, func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(seen)
	}
}

func TestHTTP3DownloadOverQUIC(t *testing.T) {
	content := make([]byte, 4<<20)
	mrand.New(mrand.NewSource(3)).Read(content)

	addr, clientTLS, peers := startHTTP3Server(t, content)
	url := fmt.Sprintf("https://%s/f.bin", addr)

	const workers = 4
	clients := newHTTP3ClientSet(workers, clientTLS)
	defer clients.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sources, info, err := probeSources(ctx, clients.probeClient(), []string{url})
	if err != nil {
		t.Fatalf("probe over HTTP/3: %v", err)
	}
	if !info.ranges || info.size != int64(len(content)) {
		t.Fatalf("probe reported ranges=%v size=%d, want true and %d", info.ranges, info.size, len(content))
	}

	out := filepath.Join(t.TempDir(), "out.bin")
	file, err := os.Create(out)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer file.Close()
	if err := file.Truncate(info.size); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	d := &downloader{
		clients:    clients,
		outputPath: out,
		statePath:  out + ".pearl",
		file:       file,
		totalSize:  info.size,
		retries:    3,
		sources:    newSourcePool(sources, workers),
		pipe:       newPipeline(defaultBufferSize, defaultPipelineDepth),
		co:         newCoordinator(splitEvenly(info.size, workers)),
		meter:      &atomic.Int64{},
	}
	for i := 0; i < workers; i++ {
		stat := &workerStat{}
		stat.source.Store(-1)
		d.stats = append(d.stats, stat)
	}
	if err := d.run(ctx); err != nil {
		t.Fatalf("HTTP/3 download: %v", err)
	}

	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("file downloaded over HTTP/3 does not match the source")
	}

	// The whole reason for one transport per worker: a shared http3.Transport
	// would pool every request onto a single QUIC connection, putting all the
	// workers behind one congestion controller and one bandwidth share.
	if n := peers(); n < workers {
		t.Fatalf("server saw %d QUIC connections, want %d (one per worker)", n, workers)
	}
}

func TestHTTP3ClientSetGivesEachWorkerItsOwnTransport(t *testing.T) {
	clients := newHTTP3ClientSet(3, nil)
	defer clients.Close()

	seen := map[*http.Client]bool{}
	for worker := 0; worker < 3; worker++ {
		seen[clients.forWorker(worker)] = true
	}
	if len(seen) != 3 {
		t.Fatalf("3 workers shared %d clients, want 3 distinct ones", len(seen))
	}
}

func TestTCPClientSetSharesOneClient(t *testing.T) {
	// Over TCP the transport pool already opens an independent connection per
	// worker, so sharing one client is correct and cheaper.
	clients := newTCPClientSet(4, defaultBufferSize)
	first := clients.forWorker(0)
	for worker := 1; worker < 4; worker++ {
		if clients.forWorker(worker) != first {
			t.Fatal("TCP workers should share a single pooled client")
		}
	}
}
