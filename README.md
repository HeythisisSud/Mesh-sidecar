# mesh-sidecar

A distributed systems learning project built from scratch in Go. Implements a service mesh sidecar (SWIM gossip + L7 proxy + eBPF L4 redirection) and a Raft consensus layer with a distributed key-value store — without using any existing mesh framework or consensus library.

---

## What is this?

This project builds two things that production distributed systems like Consul, etcd, and Kubernetes rely on:

**A service mesh sidecar** — tracks which nodes are alive, routes traffic to healthy backends transparently, and intercepts TCP connections at the kernel level via eBPF.

**A Raft consensus layer** — lets a cluster of nodes agree on a sequence of operations (a distributed log) even when some nodes fail, and uses that log to drive a consistent key-value store across all nodes.

Both are built from scratch: the gossip protocol implements the SWIM paper directly, the proxy uses Go's standard library, the eBPF program is written in C and compiled via clang, and the Raft implementation follows the Raft paper's Figure 2 specification.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Application Layer                         │
│              (distributed key-value store via Raft)              │
├──────────────────────────┬──────────────────────────────────────┤
│   Raft Consensus         │   Service Mesh Sidecar               │
│                          │                                       │
│   Leader Election        │   L7 HTTP Proxy                      │
│   Log Replication        │   (httputil.ReverseProxy)            │
│   KV State Machine       │                                       │
│                          ├───────────────────────────────────────┤
│   gRPC transport         │   SWIM Gossip Membership             │
│   (RequestVote,          │   (ping/ack, suspect/confirm,        │
│    AppendEntries)        │    indirect ping, piggybacked         │
│                          │    updates, JOIN handler)             │
│                          ├───────────────────────────────────────┤
│                          │   eBPF L4 Redirect                   │
│                          │   (cgroup/connect4, BPF map,         │
│                          │    transparent TCP interception)      │
└──────────────────────────┴───────────────────────────────────────┘
```

### Why SWIM and Raft together?

They solve completely different problems and are deliberately kept separate:

**SWIM** answers *"who is in the cluster right now?"* — it is a membership protocol. It is eventually consistent by design: different nodes can temporarily disagree about who is alive, and that is acceptable. It scales well because each node only talks to a small number of peers per round, regardless of cluster size.

**Raft** answers *"what is the agreed-upon state of our data?"* — it is a consensus protocol. It requires strong consistency: every node must apply the same log entries in the same order, with no disagreement ever. It assumes it already knows who the members are.

The dependency is one-directional: Raft needs to know its peer set (which SWIM can provide dynamically), but SWIM does not need Raft. In production systems like HashiCorp's Consul, Serf (SWIM-based) handles membership and feeds peer changes into Raft's configuration. This project builds both layers from scratch.

---

## Project Structure

```
mesh-sidecar/
├── main.go                   # Sidecar binary — gossip node, L7 proxy, optional eBPF
├── mesh_test.go              # Full test suite (SWIM + Proxy + Raft + KV)
├── go.mod / go.sum
├── members/
│   └── choosing.go           # SWIM gossip protocol
├── proxy/
│   └── proxy.go              # Health-aware round-robin reverse proxy
├── raft/
│   ├── raft.go               # Raft node (election, heartbeat, log replication)
│   ├── client.go             # gRPC RPC calls + KVStore + applyCommitted
│   ├── kv.go                 # Distributed KV store backed by Raft log
│   ├── server.go             # gRPC server (RequestVote + AppendEntries handlers)
│   └── proto/
│       ├── raft.proto         # protobuf service definition
│       ├── raft.pb.go         # generated message types
│       └── raft_grpc.pb.go    # generated gRPC stubs
├── ebpf/
│   ├── ebpf.go               # go:generate directive for bpf2go
│   ├── redirect.go           # Redirector type — loads BPF objects, manages map
│   ├── redirect_bpfel.go     # Auto-generated (little-endian BPF bindings)
│   └── redirect_bpfeb.go     # Auto-generated (big-endian BPF bindings)
├── bpf/
│   └── redirect.c            # BPF C program (cgroup/connect4 hook)
└── cmd/raft/
    └── main.go               # Standalone 3-node Raft demo binary
```

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Go | 1.21+ | Required for all subsystems |
| Linux kernel | 5.15+ (`CONFIG_CGROUP_BPF=y`, `CONFIG_DEBUG_INFO_BTF=y`) | eBPF only |
| WSL2 (Ubuntu 22.04+) or native Linux | — | eBPF only |
| Root / `CAP_BPF` | — | Only for `--ebpf` flag |
| `clang`, `llvm`, `libbpf-dev` | — | eBPF + BPF regeneration only |
| `protoc` + Go gRPC plugins | — | Only if regenerating proto stubs |

The test suite and the proxy/gossip/raft subsystems run entirely in userspace on loopback networking. **No root, no kernel modules, no real cluster needed for testing.**

Install system dependencies:

```bash
# eBPF toolchain
sudo apt install -y clang llvm libbpf-dev linux-headers-generic build-essential
go install github.com/cilium/ebpf/cmd/bpf2go@latest

# Raft gRPC stubs (only if regenerating)
sudo apt install -y protobuf-compiler
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

---

## Building

There are two separate binaries. Always target each one individually — `./...` fails when multiple `package main` directories are matched.

**1. Sidecar binary**
```bash
go build -o bin/mesh-sidecar .
```

**2. Raft demo CLI**
```bash
go build -o bin/raft ./cmd/raft/
```

**3. Regenerate BPF bindings** (only if you modify `bpf/redirect.c`)
```bash
cd ebpf && go generate && cd ..
```
The auto-generated files (`redirect_bpfel.go`, `redirect_bpfeb.go`) are already committed — you don't need this step unless you change the BPF C program.

**4. Regenerate gRPC stubs** (only if you modify `raft/proto/raft.proto`)
```bash
cd raft/proto
protoc \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  raft.proto
```

---

## Part 1: Service Mesh Sidecar

### Phase 1 — SWIM Gossip Membership

Every node maintains a membership table tracking peers as `Alive`, `Suspect`, or `Confirm` (dead). Nodes exchange this via UDP using the SWIM protocol:

- Every second, each node picks one random peer and sends a `PING`
- If no `ACK` arrives within 500 ms, the node asks up to 3 other peers to relay an indirect ping (`PING-REQ`) — this distinguishes a dead node from a network partition between two specific nodes
- If indirect ping also fails after another 500 ms, the target is marked `Suspect`
- After 3 seconds in `Suspect` state with no refutation, the node is marked `Confirm` and removed from the membership table
- Every message carries piggybacked `Update` entries gossipping state changes across the cluster without any broadcast
- A node that is incorrectly suspected refutes it by broadcasting a higher-incarnation `Alive` update about itself

### Phase 2 — L7 HTTP Proxy

Each node runs an HTTP reverse proxy alongside its app backend. On every incoming request, the proxy calls `node.Snapshot()` to get the current live membership, picks the next alive node in sorted round-robin order, and forwards the request there. Because it reads from the same membership table gossip maintains, it automatically stops routing to a dead node within one sync cycle.

### Phase 3 — eBPF L4 Transparent Redirection

A `cgroup/connect4` BPF program is compiled via clang and loaded into the kernel, attached to the cgroup that WSL2 processes run in. This program intercepts every outgoing TCP `connect()` syscall before the connection is established. If the destination port matches an entry in a BPF hash map, the kernel silently rewrites the destination — the client never knows the switch happened.

A goroutine syncs the live membership table into the BPF map every second:

```
A:9000 → B:9001
B:9001 → C:9002
C:9002 → A:9000
```

When a node dies and gossip confirms it, its BPF map entry is removed and the rotation updates automatically. Unlike the L7 proxy (which requires clients to deliberately connect to a proxy port), eBPF interception is completely transparent — any application connecting to any of these ports gets redirected without knowing a mesh exists.

### Port Layout (example with gossip port 8000)

| Layer | Port |
|---|---|
| Gossip (UDP) | 8000 |
| App backend | 9000 |
| L7 proxy | 10000 |
| Status API | 11000 |

### Running the Sidecar

```bash
# mount bpffs once per boot
sudo mount -t bpf bpf /sys/fs/bpf

# node A -- owns the eBPF program and BPF map
sudo ./bin/mesh-sidecar -id=A -bind=127.0.0.1:8000 -ebpf

# node B and C -- gossip only, no eBPF flag needed
sudo ./bin/mesh-sidecar -id=B -bind=127.0.0.1:8001 -join=127.0.0.1:8000
sudo ./bin/mesh-sidecar -id=C -bind=127.0.0.1:8002 -join=127.0.0.1:8000
```

Interactive commands while a node is running:

| Command | Effect |
|---|---|
| `members` | Print the known member list |
| `quit` | Shut down |

### Testing the Sidecar

```bash
# test L4 transparent redirection -- should return different nodes than the port implies
curl http://127.0.0.1:9000/   # → hello from B
curl http://127.0.0.1:9001/   # → hello from C
curl http://127.0.0.1:9002/   # → hello from A

# test failure detection -- kill node B, wait ~5 seconds
curl http://127.0.0.1:9001/   # → connection refused (removed from BPF map)

# view live membership as JSON
curl http://127.0.0.1:11000/status | jq
```

---

## Part 2: Raft Consensus + Distributed KV Store

### How Raft Works

Raft solves the problem of getting a cluster of nodes to agree on a sequence of operations even when some nodes fail. Every write goes through a single elected leader, which replicates the operation to a majority of nodes before considering it committed. Even if the leader dies, the cluster elects a new one that is guaranteed to have all previously committed entries.

Three subproblems:

**Leader election** — nodes start as followers. When a follower stops hearing from a leader (election timeout, randomized between 150–300 ms to prevent split votes), it becomes a candidate and requests votes. A candidate wins if it gets votes from a majority of nodes. Voters only grant votes to candidates whose logs are at least as up-to-date as their own.

**Log replication** — the leader appends each submitted command to its log, then sends `AppendEntries` RPCs to all followers in parallel. Once a majority confirm, the entry is committed and applied to the state machine. The leader sends heartbeats (empty `AppendEntries`) every 50 ms to suppress new elections.

**Safety** — a node can only become leader if it has all previously committed log entries. This is enforced by the vote-granting rule: a follower rejects a `RequestVote` if the candidate's log is less up-to-date than its own.

### Key-Value State Machine

The KV store sits on top of Raft's `ApplyCh` — a channel that receives committed log entries in order. Each entry is a command string (`SET key value` or `DEL key`). The state machine applies these commands to an in-memory `map[string]string`. Because every node applies the same entries from the same log in the same order, all nodes converge to identical state.

Reads are served locally (may be slightly stale on followers). Writes are submitted to the leader and block until committed by a majority.

### Running the Raft Demo

```bash
go run ./cmd/raft/main.go
```

Expected output:

```
[B] starting election for term 1
[A] granted vote to B for term 1
[C] granted vote to B for term 1
[B] became leader for term 1
>>> leader is B, submitting KV ops
[A] state: map[x:2 y:hello]
[B] state: map[x:2 y:hello]
[C] state: map[x:2 y:hello]
after DEL y:
[A] state: map[x:2]
[B] state: map[x:2]
[C] state: map[x:2]
>>> killing leader B...
[C] starting election for term 2
[C] became leader for term 2
```

All three nodes converge to identical state. After the leader is killed, a new one is elected in the next term and the cluster continues without data loss.

---

## Running the Tests

### Quick start — run everything

```bash
go test ./... -v -timeout 60s -count=1
```

- `-v` prints each test name and any log output
- `-timeout 60s` gives Raft elections enough time to complete
- `-count=1` disables the test cache so you always get a fresh run

### Run only one subsystem

```bash
go test -v -run TestSWIM_  -timeout 30s -count=1
go test -v -run TestProxy_ -timeout 30s -count=1
go test -v -run TestRaft_  -timeout 60s -count=1
go test -v -run TestKV_    -timeout 60s -count=1
```

### Run with race detector (recommended before committing)

```bash
go test ./... -race -timeout 90s -count=1
```

### What Each Test Verifies

**SWIM Gossip (`TestSWIM_*`)**

| Test | What it checks |
|---|---|
| `TestSWIM_NodeCreation` | `NewNode` initialises ID, empty Members map, `Incarnation=0` |
| `TestSWIM_PingAck` | Two nodes exchange PING/ACK over loopback UDP without panicking |
| `TestSWIM_JOIN` | A joining node discovers the seed; the seed discovers the joiner |
| `TestSWIM_Snapshot` | `SnapShot()` returns a deep copy — mutating the result does not affect live state |
| `TestSWIM_MergeUpdates_IgnoresStale` | Stale gossip updates (lower incarnation) are silently dropped |
| `TestSWIM_MergeUpdates_Confirm` | A `Confirm` update removes the member from the map |
| `TestSWIM_MergeUpdates_SelfRefutation` | Initial incarnation is 0; self-refutation path is covered by JOIN test |

**L7 Proxy (`TestProxy_*`)**

| Test | What it checks |
|---|---|
| `TestProxy_SingleBackend` | Requests are forwarded to a single alive backend |
| `TestProxy_HealthAwareRouting` | Suspect/dead backends receive zero traffic |
| `TestProxy_NoAliveMembers` | Returns HTTP 503 when no alive members exist |
| `TestProxy_SpreadAcrossAliveBackends` | 30 requests spread across multiple alive backends |

**Raft Consensus (`TestRaft_*`)**

| Test | What it checks |
|---|---|
| `TestRaft_ElectionSingleNode` | A lone node elects itself leader within 1 s |
| `TestRaft_ElectionThreeNodes` | Exactly 1 leader and 2 followers after startup |
| `TestRaft_LeaderStability` | Leader stays in the same term after 2 s of idle |
| `TestRaft_LeaderFailover` | Killing the leader triggers a new election in a higher term |
| `TestRaft_LogReplication` | All 3 nodes apply 3 submitted commands via the KV state machine |
| `TestRaft_MajorityCommit` | Submit succeeds with 2 of 3 nodes alive |
| `TestRaft_NoCommitWithoutMajority` | Submit returns false with only 1 of 3 nodes alive |

**KV Store (`TestKV_*`)**

| Test | What it checks |
|---|---|
| `TestKV_SetAndGet` | A value written on the leader is readable on all nodes |
| `TestKV_Overwrite` | A second `Set` on the same key replaces the value everywhere |
| `TestKV_Delete` | A deleted key disappears from all nodes |
| `TestKV_ConsistentState` | 10 sequential writes produce identical snapshots on all 3 nodes |
| `TestKV_NotLeaderReturnsError` | `Set` on a follower returns a non-nil error |

### Test Helpers

| Helper | Purpose |
|---|---|
| `mustUDPConn(t)` | Binds a UDP socket on a random loopback port; auto-cleaned up |
| `mustNode(t, id)` | Creates and starts a SWIM node on a random port |
| `waitFor(t, d, cond)` | Polls `cond` every 20 ms until true or deadline |
| `backendServer(t, id)` | Starts an `httptest.Server` that echoes `id` in the body |
| `nodeFromBackends(t, …)` | Wires `httptest` servers into a `members.Node` for proxy tests |
| `raftCluster(t, n)` | Spins up `n` Raft nodes + gRPC servers on random loopback ports |
| `raftClusterWithStop(t, n)` | Same as above but returns a `stopNode(i)` function |
| `findLeader(t, nodes, d)` | Blocks until exactly one leader is elected |

---

## Known Simplifications

**Mesh sidecar:**
- Fixed port offset convention (`app = gossip+1000`, `proxy = gossip+2000`) instead of gossiping the real service address
- Single BPF program owner per machine — in a real deployment each physical machine runs one sidecar independently
- No TLS on gossip or proxy traffic
- Manual seed address via `-join` instead of DNS-based discovery
- In-memory membership state only

**Raft:**
- No persistence — `currentTerm`, `votedFor`, and `log[]` are lost on crash
- No log catchup for lagging nodes — if a follower misses entries while down, the leader does not retry with earlier log indices
- Local reads — `Get()` reads from local state and may be stale on followers
- No cluster membership changes — the peer set is static, defined at startup

---

## Common Issues

**`stat .../cmd/raft/cd: directory not found`**
Happens when you run `go build -o bin/raft ./cmd/raft/cd ebpf ...` as one line. The shell treats `cd ebpf` as part of the path. Run each command separately.

**`go: cannot write multiple packages to non-directory <name>`**
`go build -o filename ./...` fails because `./...` matches both `package main` roots. Target each binary individually:
```bash
go build -o bin/mesh-sidecar .
go build -o bin/raft ./cmd/raft/
```

**`package ebpf is not in std`**
Run `go generate` from inside the `ebpf/` directory:
```bash
cd ebpf && go generate && cd ..
```

**Tests time out on `TestRaft_*`**
Increase the timeout: `go test -timeout 120s`

**`go test ./...` fails to compile the ebpf package**
The auto-generated files are already committed. To run only the pure-Go tests:
```bash
go test -v -run 'TestSWIM_|TestProxy_|TestRaft_|TestKV_' -timeout 60s -count=1
```

**Race detector reports a conflict**
Run `go test ./... -race` and open an issue. Most known races are resolved; new ones indicate a regression.

---

## Module Info

```
module github.com/HeythisisSud/mesh-sidecar
```

All internal packages are imported relative to this module path.

---

## References

- [SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) — Das, Gupta, Motivala (2002)
- [In Search of an Understandable Consensus Algorithm (Raft)](https://raft.github.io/raft.pdf) — Ongaro and Ousterhout (2014)
- [hashicorp/memberlist](https://github.com/hashicorp/memberlist) — production SWIM implementation used in Consul
- [hashicorp/raft](https://github.com/hashicorp/raft) — production Raft implementation used in Consul
- [cilium/ebpf](https://github.com/cilium/ebpf) — Go library for loading and managing eBPF programs
- [ebpf-go getting started](https://ebpf-go.dev/guides/getting-started/) — bpf2go toolchain walkthrough
- [Envoy Proxy](https://www.envoyproxy.io/) and [Linkerd](https://linkerd.io/) — production service mesh data planes
