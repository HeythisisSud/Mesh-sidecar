




package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/HeythisisSud/mesh-sidecar/members"
	"github.com/HeythisisSud/mesh-sidecar/proxy"
	"github.com/HeythisisSud/mesh-sidecar/raft"
	pb "github.com/HeythisisSud/mesh-sidecar/raft/proto"
)



func mustUDPConn(t *testing.T) (*net.UDPConn, *net.UDPAddr) {
	t.Helper()
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn, conn.LocalAddr().(*net.UDPAddr)
}

func mustNode(t *testing.T, id string) (*members.Node, *net.UDPAddr) {
	t.Helper()
	conn, addr := mustUDPConn(t)
	node := members.NewNode(id, addr, conn)
	node.Start()
	return node, addr
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func backendServer(t *testing.T, id string) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, id)
	}))
	t.Cleanup(s.Close)
	return s
}



func nodeFromBackends(t *testing.T, servers []*httptest.Server, statuses []string) *members.Node {
	t.Helper()
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	node := members.NewNode("proxy-test", addr, nil)
	for i, s := range servers {
		host, portStr, _ := net.SplitHostPort(strings.TrimPrefix(s.URL, "http://"))
		httpPort, _ := strconv.Atoi(portStr)
		gossipAddr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", host, httpPort-1000))
		id := fmt.Sprintf("m%d", i)
		node.Members[id] = &members.MemberState{
			ID:     id,
			Addr:   gossipAddr,
			Status: statuses[i],
		}
	}
	return node
}



func raftCluster(t *testing.T, count int) ([]*raft.Node, []*raft.KVStore) {
	t.Helper()
	listeners := make([]net.Listener, count)
	addrs := make([]string, count)
	for i := range listeners {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		listeners[i] = l
		addrs[i] = l.Addr().String()
	}
	nodes := make([]*raft.Node, count)
	kvs := make([]*raft.KVStore, count)
	for i := range nodes {
		peers := make([]string, 0, count-1)
		for j, a := range addrs {
			if j != i {
				peers = append(peers, a)
			}
		}
		n := raft.NewNode(fmt.Sprintf("n%d", i), peers)
		srv := grpc.NewServer()
		pb.RegisterRaftServiceServer(srv, raft.NewRaftServer(n))
		go srv.Serve(listeners[i])
		kvs[i] = raft.NewKVStore(n)
		n.Start()
		nodes[i] = n
		t.Cleanup(n.Stop)
	}
	return nodes, kvs
}

func findLeader(t *testing.T, nodes []*raft.Node, d time.Duration) int {
	t.Helper()
	var idx int
	if !waitFor(t, d, func() bool {
		cnt := 0
		for i, n := range nodes {
			if s, _ := n.Status(); s == "Leader" {
				cnt++
				idx = i
			}
		}
		return cnt == 1
	}) {
		t.Fatal("no leader elected within deadline")
	}
	return idx
}



func raftClusterWithStop(t *testing.T, count int) ([]*raft.Node, []*raft.KVStore, func(int)) {
	t.Helper()
	listeners := make([]net.Listener, count)
	addrs := make([]string, count)
	for i := range listeners {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		listeners[i] = l
		addrs[i] = l.Addr().String()
	}
	nodes := make([]*raft.Node, count)
	kvs := make([]*raft.KVStore, count)
	srvs := make([]*grpc.Server, count)
	for i := range nodes {
		peers := make([]string, 0, count-1)
		for j, a := range addrs {
			if j != i {
				peers = append(peers, a)
			}
		}
		n := raft.NewNode(fmt.Sprintf("n%d", i), peers)
		srv := grpc.NewServer()
		srvs[i] = srv
		pb.RegisterRaftServiceServer(srv, raft.NewRaftServer(n))
		go srv.Serve(listeners[i])
		kvs[i] = raft.NewKVStore(n)
		n.Start()
		nodes[i] = n
	}
	t.Cleanup(func() {
		for i, n := range nodes {
			n.Stop()
			srvs[i].Stop()
		}
	})
	return nodes, kvs, func(i int) {
		nodes[i].Stop()
		srvs[i].Stop()
	}
}



func TestSWIM_NodeCreation(t *testing.T) {
	conn, addr := mustUDPConn(t)
	n := members.NewNode("A", addr, conn)
	if n.Members == nil {
		t.Fatal("Members map is nil")
	}
	if n.Incarnation != 0 {
		t.Fatalf("want Incarnation=0, got %d", n.Incarnation)
	}
	if len(n.Members) != 0 {
		t.Fatalf("want empty Members, got %d", len(n.Members))
	}
}

func TestSWIM_PingAck(t *testing.T) {
	nodeA, _ := mustNode(t, "A")
	nodeB, addrB := mustNode(t, "B")

	nodeA.Members["B"] = &members.MemberState{ID: "B", Addr: addrB, Status: members.StatusAlive}
	nodeB.Members["A"] = &members.MemberState{ID: "A", Addr: nodeA.BindAddr, Status: members.StatusAlive}

	time.Sleep(2 * time.Second)

	if len(nodeA.SnapShot()) == 0 {
		t.Error("A snapshot empty after gossip")
	}
	if len(nodeB.SnapShot()) == 0 {
		t.Error("B snapshot empty after gossip")
	}
}

func TestSWIM_JOIN(t *testing.T) {
	seed, seedAddr := mustNode(t, "seed")
	conn2, addr2 := mustUDPConn(t)
	joiner := members.NewNode("joiner", addr2, conn2)
	joiner.Start()

	if err := joiner.Join(seedAddr); err != nil {
		t.Fatalf("Join failed: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		for _, m := range joiner.SnapShot() {
			if m.ID == "seed" {
				return true
			}
		}
		return false
	}) {
		t.Error("joiner does not see seed")
	}
	if !waitFor(t, 2*time.Second, func() bool {
		for _, m := range seed.SnapShot() {
			if m.ID == "joiner" {
				return true
			}
		}
		return false
	}) {
		t.Error("seed does not see joiner")
	}
}

func TestSWIM_Snapshot(t *testing.T) {
	conn, addr := mustUDPConn(t)
	n := members.NewNode("n", addr, conn)
	peer, _ := net.ResolveUDPAddr("udp", "127.0.0.1:19999")
	n.Members["p"] = &members.MemberState{ID: "p", Addr: peer, Status: members.StatusAlive}

	snap := n.SnapShot()
	snap[0].Status = members.StatusConfirm 

	if n.SnapShot()[0].Status != members.StatusAlive {
		t.Error("SnapShot is not a copy — mutation affected live state")
	}
}

func TestSWIM_MergeUpdates_IgnoresStale(t *testing.T) {
	conn, addr := mustUDPConn(t)
	n := members.NewNode("n", addr, conn)
	peer, _ := net.ResolveUDPAddr("udp", "127.0.0.1:20000")
	n.Members["p"] = &members.MemberState{ID: "p", Addr: peer, Status: members.StatusAlive, Incarnation: 5}

	snap := n.SnapShot()
	for _, m := range snap {
		if m.ID == "p" && m.Incarnation != 5 {
			t.Errorf("incarnation changed without valid update: got %d", m.Incarnation)
		}
	}
}

func TestSWIM_MergeUpdates_Confirm(t *testing.T) {
	conn, addr := mustUDPConn(t)
	n := members.NewNode("n", addr, conn)
	peer, _ := net.ResolveUDPAddr("udp", "127.0.0.1:21000")
	n.Members["doomed"] = &members.MemberState{ID: "doomed", Addr: peer, Status: members.StatusSuspect, Incarnation: 1}

	
	delete(n.Members, "doomed")
	if _, ok := n.Members["doomed"]; ok {
		t.Error("Confirm update must remove member from map")
	}
}

func TestSWIM_MergeUpdates_SelfRefutation(t *testing.T) {
	conn, addr := mustUDPConn(t)
	n := members.NewNode("self", addr, conn)
	if n.Incarnation != 0 {
		t.Fatal("initial incarnation must be 0")
	}
}



func TestProxy_SingleBackend(t *testing.T) {
	be := backendServer(t, "hello")
	node := nodeFromBackends(t, []*httptest.Server{be}, []string{members.StatusAlive})
	h, _ := proxy.NewProxy(node)
	ps := httptest.NewServer(h)
	t.Cleanup(ps.Close)

	resp, err := http.Get(ps.URL + "/")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "hello" {
		t.Errorf("want hello, got %q", string(body))
	}
}

func TestProxy_HealthAwareRouting(t *testing.T) {
	alive := backendServer(t, "alive")
	suspect := backendServer(t, "suspect")
	suspect.Close()

	node := nodeFromBackends(t,
		[]*httptest.Server{alive, suspect},
		[]string{members.StatusAlive, members.StatusSuspect},
	)
	h, _ := proxy.NewProxy(node)
	ps := httptest.NewServer(h)
	t.Cleanup(ps.Close)

	hits := map[string]int{}
	for i := 0; i < 20; i++ {
		resp, err := http.Get(ps.URL + "/")
		if err != nil {
			continue
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		hits[string(b)]++
	}
	if hits["suspect"] > 0 {
		t.Errorf("suspect backend hit %d times", hits["suspect"])
	}
}

func TestProxy_NoAliveMembers(t *testing.T) {
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	node := members.NewNode("empty", addr, nil)
	h, _ := proxy.NewProxy(node)
	ps := httptest.NewServer(h)
	t.Cleanup(ps.Close)

	resp, _ := http.Get(ps.URL + "/")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("want 503, got %d", resp.StatusCode)
	}
}

func TestProxy_SpreadAcrossAliveBackends(t *testing.T) {
	servers := []*httptest.Server{
		backendServer(t, "b0"),
		backendServer(t, "b1"),
		backendServer(t, "b2"),
	}
	node := nodeFromBackends(t, servers, []string{members.StatusAlive, members.StatusAlive, members.StatusAlive})
	h, _ := proxy.NewProxy(node)
	ps := httptest.NewServer(h)
	t.Cleanup(ps.Close)

	hits := map[string]int{}
	for i := 0; i < 30; i++ {
		resp, err := http.Get(ps.URL + "/")
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		hits[string(b)]++
	}
	t.Logf("hit distribution: %v", hits)
	if len(hits) < 2 {
		t.Errorf("want spread >=2 backends, got %d: %v", len(hits), hits)
	}
}



func TestRaft_ElectionSingleNode(t *testing.T) {
	n := raft.NewNode("solo", []string{})
	n.Start()
	t.Cleanup(n.Stop)
	
	if !waitFor(t, 1*time.Second, func() bool {
		s, _ := n.Status()
		return s == "Leader"
	}) {
		s, term := n.Status()
		t.Errorf("single node not leader after 1s: %s term=%d", s, term)
	}
}

func TestRaft_ElectionThreeNodes(t *testing.T) {
	nodes, _ := raftCluster(t, 3)
	idx := findLeader(t, nodes, 2*time.Second)
	followers := 0
	for i, n := range nodes {
		if i == idx {
			continue
		}
		if s, _ := n.Status(); s == "Follower" {
			followers++
		}
	}
	if followers != 2 {
		t.Errorf("want 2 followers, got %d", followers)
	}
}

func TestRaft_LeaderStability(t *testing.T) {
	nodes, _ := raftCluster(t, 3)
	idx := findLeader(t, nodes, 2*time.Second)
	_, term0 := nodes[idx].Status()
	time.Sleep(2 * time.Second)
	s, term1 := nodes[idx].Status()
	if s != "Leader" {
		t.Error("leader stepped down unexpectedly")
	}
	if term1 != term0 {
		t.Errorf("term changed %d->%d: spurious re-election", term0, term1)
	}
}

func TestRaft_LeaderFailover(t *testing.T) {
	nodes, _, stopNode := raftClusterWithStop(t, 3)
	oldIdx := findLeader(t, nodes, 2*time.Second)
	_, oldTerm := nodes[oldIdx].Status()
	stopNode(oldIdx)

	remaining := make([]*raft.Node, 0, 2)
	for i, n := range nodes {
		if i != oldIdx {
			remaining = append(remaining, n)
		}
	}
	if !waitFor(t, 2*time.Second, func() bool {
		for _, n := range remaining {
			if s, term := n.Status(); s == "Leader" && term > oldTerm {
				return true
			}
		}
		return false
	}) {
		t.Error("new leader not elected after old leader stopped")
	}
}

func TestRaft_LogReplication(t *testing.T) {
	nodes, kvs := raftCluster(t, 3)
	idx := findLeader(t, nodes, 2*time.Second)

	cmds := []string{"SET a 1", "SET b 2", "SET c 3"}
	for _, cmd := range cmds {
		if _, ok := nodes[idx].Submit(cmd); !ok {
			t.Fatalf("Submit(%q) failed", cmd)
		}
	}

	
	
	if !waitFor(t, 5*time.Second, func() bool {
		for _, kv := range kvs {
			if _, ok := kv.Get("a"); !ok {
				return false
			}
			if _, ok := kv.Get("b"); !ok {
				return false
			}
			if _, ok := kv.Get("c"); !ok {
				return false
			}
		}
		return true
	}) {
		for i, kv := range kvs {
			t.Logf("node %d snapshot: %v", i, kv.Snapshot())
		}
		t.Error("not all nodes applied all entries within 5s")
	}
}

func TestRaft_MajorityCommit(t *testing.T) {
	nodes, _, stopNode := raftClusterWithStop(t, 3)
	idx := findLeader(t, nodes, 2*time.Second)
	for i := range nodes {
		if i != idx {
			stopNode(i)
			break
		}
	}
	time.Sleep(100 * time.Millisecond)
	if _, ok := nodes[idx].Submit("SET majority yes"); !ok {
		t.Error("Submit must succeed with 2 of 3 nodes alive")
	}
}

func TestRaft_NoCommitWithoutMajority(t *testing.T) {
	nodes, _, stopNode := raftClusterWithStop(t, 3)
	idx := findLeader(t, nodes, 2*time.Second)

	
	
	for i := range nodes {
		if i != idx {
			stopNode(i)
		}
	}
	
	time.Sleep(300 * time.Millisecond)

	
	_, ok := nodes[idx].Submit("SET no-majority yes")
	if ok {
		t.Error("Submit must not commit without majority (2 of 3 required, only 1 available)")
	}
}



func TestKV_SetAndGet(t *testing.T) {
	nodes, kvs := raftCluster(t, 3)
	idx := findLeader(t, nodes, 2*time.Second)

	if err := kvs[idx].Set("x", "1"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	for i, kv := range kvs {
		if !waitFor(t, 2*time.Second, func() bool {
			v, ok := kv.Get("x")
			return ok && v == "1"
		}) {
			v, ok := kv.Get("x")
			t.Errorf("node %d: Get(x)=(%q,%v) want (\"1\",true)", i, v, ok)
		}
	}
}

func TestKV_Overwrite(t *testing.T) {
	nodes, kvs := raftCluster(t, 3)
	idx := findLeader(t, nodes, 2*time.Second)
	kvs[idx].Set("x", "1")
	kvs[idx].Set("x", "2")
	for i, kv := range kvs {
		if !waitFor(t, 2*time.Second, func() bool {
			v, ok := kv.Get("x")
			return ok && v == "2"
		}) {
			v, _ := kv.Get("x")
			t.Errorf("node %d: want x=2, got %q", i, v)
		}
	}
}

func TestKV_Delete(t *testing.T) {
	nodes, kvs := raftCluster(t, 3)
	idx := findLeader(t, nodes, 2*time.Second)
	kvs[idx].Set("y", "hello")
	
	waitFor(t, 2*time.Second, func() bool {
		for _, kv := range kvs {
			if _, ok := kv.Get("y"); !ok {
				return false
			}
		}
		return true
	})
	kvs[idx].Delete("y")
	for i, kv := range kvs {
		if !waitFor(t, 3*time.Second, func() bool {
			_, ok := kv.Get("y")
			return !ok
		}) {
			t.Errorf("node %d: key y present after Delete", i)
		}
	}
}

func TestKV_ConsistentState(t *testing.T) {
	nodes, kvs := raftCluster(t, 3)
	
	lidx := findLeader(t, nodes, 2*time.Second)
	for i := 0; i < 10; i++ {
		if err := kvs[lidx].Set(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("Set k%d: %v", i, err)
		}
	}

	
	if !waitFor(t, 3*time.Second, func() bool {
		for _, kv := range kvs {
			snap := kv.Snapshot()
			if len(snap) < 10 {
				return false
			}
		}
		return true
	}) {
		for i, kv := range kvs {
			t.Logf("node %d has %d keys", i, len(kv.Snapshot()))
		}
		t.Error("nodes did not converge within 3s")
	}

	snaps := make([]map[string]string, len(kvs))
	for i, kv := range kvs {
		snaps[i] = kv.Snapshot()
	}
	for i := 1; i < len(snaps); i++ {
		if !reflect.DeepEqual(snaps[0], snaps[i]) {
			t.Errorf("node 0 and node %d snapshots differ:\n  n0: %v\n  n%d: %v",
				i, snaps[0], i, snaps[i])
		}
	}

	
	snap := kvs[0].Snapshot()
	snap["injected"] = "ghost"
	if _, ok := kvs[0].Get("injected"); ok {
		t.Error("Snapshot returned a live reference")
	}
}

func TestKV_NotLeaderReturnsError(t *testing.T) {
	nodes, kvs := raftCluster(t, 3)
	leaderIdx := findLeader(t, nodes, 2*time.Second)
	for i, kv := range kvs {
		if i == leaderIdx {
			continue
		}
		if err := kv.Set("k", "v"); err == nil {
			t.Errorf("node %d (follower) Set returned nil error", i)
		}
		break
	}
}


var _ = func() []string {
	m := map[string]string{"a": "1"}
	ks := make([]string, 0)
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
