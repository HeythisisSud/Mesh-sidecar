package proxy


import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/HeythisisSud/mesh-sidecar/members" // TODO: adjust to your actual module path
)

// TestNewProxy_SpreadsAcrossAliveBackends spins up several fake
// backends, registers them as "Alive" members on a real Node, then
// fires many requests through the proxy and checks that more than
// one distinct backend answered -- proving selection isn't hardcoded
// to a single target.
func TestNewProxy_SpreadsAcrossAliveBackends(t *testing.T) {
	const numBackends = 3
	const numRequests = 30

	backends := make([]*httptest.Server, numBackends)
	hitCounts := make(map[string]int)

	// 1. Spin up N fake backends, each identifying itself in its response.
	for i := 0; i < numBackends; i++ {
		id := fmt.Sprintf("backend-%d", i)
		backends[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(id))
		}))
		defer backends[i].Close()
	}

	// 2. Build a real Node and manually register each backend as an
	// Alive member. NOTE: this direct write to node.Members is only
	// safe because Start() hasn't been called yet -- no gossip/receive
	// goroutines are running concurrently against this Node.
	bindAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to resolve node bind addr: %v", err)
	}
	node := members.NewNode("test-node", bindAddr, nil)

	for i, b := range backends {
		httpHost, httpPortStr, err := net.SplitHostPort(mustTrimScheme(b.URL))
		if err != nil {
			t.Fatalf("failed to split backend host/port: %v", err)
		}
		httpPort, err := strconv.Atoi(httpPortStr)
		if err != nil {
			t.Fatalf("failed to parse backend port: %v", err)
		}

		// Director adds +1000 to the gossip port to get the HTTP
		// port, so the fake "gossip" address here must be httpPort-1000.
		gossipAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", httpHost, httpPort-1000))
		if err != nil {
			t.Fatalf("failed to build fake gossip addr: %v", err)
		}

		id := fmt.Sprintf("member-%d", i)
		node.Members[id] = &members.MemberState{
			ID:     id,
			Addr:   gossipAddr,
			Status: "Alive",
		}
	}

	// 3. Build the proxy against this node.
	handler, err := NewProxy(node)
	if err != nil {
		t.Fatalf("NewProxy returned error: %v", err)
	}
	proxyServer := httptest.NewServer(handler)
	defer proxyServer.Close()

	// 4. Fire many requests through the proxy, recording which
	// backend answered each time.
	for i := 0; i < numRequests; i++ {
		resp, err := http.Get(proxyServer.URL + "/")
		if err != nil {
			t.Fatalf("request %d through proxy failed: %v", i, err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("request %d: failed to read body: %v", i, err)
		}
		hitCounts[string(body)]++
	}

	// 5. Assert more than one distinct backend was hit.
	t.Logf("hit distribution: %v", hitCounts)
	if len(hitCounts) < 2 {
		t.Errorf("expected requests to spread across multiple backends, but only %d distinct backend(s) were hit: %v",
			len(hitCounts), hitCounts)
	}
}

// mustTrimScheme strips "http://" from an httptest.Server URL so
// net.SplitHostPort can parse it directly.
func mustTrimScheme(url string) string {
	const prefix = "http://"
	if len(url) > len(prefix) && url[:len(prefix)] == prefix {
		return url[len(prefix):]
	}
	return url
}