# mesh-sidecar

A distributed systems learning project built from scratch in Go. Implements a service mesh sidecar (SWIM gossip + L7 proxy + eBPF L4 redirection) and a Raft consensus layer with a distributed key-value store — without using any existing mesh framework or consensus library.

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
│   └── server.go             # gRPC server (RequestVote + AppendEntries handlers)
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

| Requirement | Version |
|---|---|
| Go | 1.21+ |
| Linux (for eBPF) | kernel 5.10+ |
| Root / CAP_BPF | Only for `--ebpf` flag |

> The test suite and the proxy/gossip/raft subsystems run entirely in **userspace** on loopback networking. No root, no kernel modules, no real cluster needed for testing.

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
go test -v -run TestSWIM_ -timeout 30s -count=1

go test -v -run TestProxy_ -timeout 30s -count=1

go test -v -run TestRaft_ -timeout 60s -count=1

go test -v -run TestKV_ -timeout 60s -count=1
```

### Run a single specific test

```bash
go test -v -run TestRaft_LeaderFailover -timeout 30s -count=1
```

### Run with race detector (recommended before committing)

```bash
go test ./... -race -timeout 90s -count=1
```

---

## What Each Test Verifies

### SWIM Gossip (`TestSWIM_*`)

| Test | What it checks |
|---|---|
| `TestSWIM_NodeCreation` | `NewNode` initialises ID, empty Members map, Incarnation=0 |
| `TestSWIM_PingAck` | Two nodes exchange PING/ACK over loopback UDP without panicking |
| `TestSWIM_JOIN` | A joining node discovers the seed; the seed discovers the joiner |
| `TestSWIM_Snapshot` | `SnapShot()` returns a deep copy — mutating the result does not affect live state |
| `TestSWIM_MergeUpdates_IgnoresStale` | Stale gossip updates (lower incarnation) are silently dropped |
| `TestSWIM_MergeUpdates_Confirm` | A Confirm update removes the member from the map |
| `TestSWIM_MergeUpdates_SelfRefutation` | Initial incarnation is 0; self-refutation path is covered by JOIN test |

### L7 Proxy (`TestProxy_*`)

| Test | What it checks |
|---|---|
| `TestProxy_SingleBackend` | Requests are forwarded to a single alive backend |
| `TestProxy_HealthAwareRouting` | Suspect/dead backends receive zero traffic |
| `TestProxy_NoAliveMembers` | Returns `HTTP 503` when no alive members exist |
| `TestProxy_SpreadAcrossAliveBackends` | 30 requests spread across multiple alive backends |

### Raft Consensus (`TestRaft_*`)

| Test | What it checks |
|---|---|
| `TestRaft_ElectionSingleNode` | A lone node elects itself leader within 1 s |
| `TestRaft_ElectionThreeNodes` | Exactly 1 leader and 2 followers after startup |
| `TestRaft_LeaderStability` | Leader stays in the same term after 2 s of idle |
| `TestRaft_LeaderFailover` | Killing the leader triggers a new election in a higher term |
| `TestRaft_LogReplication` | All 3 nodes apply 3 submitted commands via the KV state machine |
| `TestRaft_MajorityCommit` | Submit succeeds with 2 of 3 nodes alive |
| `TestRaft_NoCommitWithoutMajority` | Submit returns false with only 1 of 3 nodes alive |

### KV Store (`TestKV_*`)

| Test | What it checks |
|---|---|
| `TestKV_SetAndGet` | A value written on the leader is readable on all nodes |
| `TestKV_Overwrite` | A second `Set` on the same key replaces the value everywhere |
| `TestKV_Delete` | A deleted key disappears from all nodes |
| `TestKV_ConsistentState` | 10 sequential writes produce identical snapshots on all 3 nodes |
| `TestKV_NotLeaderReturnsError` | `Set` on a follower returns a non-nil error |

---

## Understanding Test Helpers

```
mustUDPConn(t)            → binds a UDP socket on a random loopback port; auto-cleaned up
mustNode(t, id)           → creates and starts a SWIM node on a random port
waitFor(t, d, cond)       → polls cond every 20 ms until true or deadline
backendServer(t, id)      → starts an httptest.Server that echoes id in the body
nodeFromBackends(t, …)    → wires httptest servers into a members.Node for proxy tests
raftCluster(t, n)         → spins up n Raft nodes + gRPC servers on random loopback ports
raftClusterWithStop(t, n) → same as raftCluster but returns a stopNode(i) function
findLeader(t, nodes, d)   → blocks until exactly one leader is elected
```

---

## Building the Binaries

There are **two separate binaries** in this project. Build each one with its own command.

> The eBPF auto-generated files (`redirect_bpfel.go`, `redirect_bpfeb.go`) are already committed — you do **not** need to run `go generate` unless you modify `bpf/redirect.c`.

### 1. Sidecar binary (main entry point)

```bash
go build -o bin/mesh-sidecar .
```

### 2. Raft demo CLI

```bash
go build -o bin/raft ./cmd/raft/
```

### 3. Regenerate BPF bindings (only if you edit `bpf/redirect.c`)

Requires `clang` and `libbpf-dev`:

```bash
cd ebpf && go generate && cd ..
```

> **Common mistake:** `go build -o some-file ./...` fails when `./...` matches multiple `package main` directories. Always target a specific package path.

---

## Running the Binary (Manual Demo)

> **Not required for testing.** The binary needs a real Linux environment and optionally root for eBPF.

### Start a seed node

```bash
go run . -id=A -bind=127.0.0.1:8000
```

### Join from a second terminal

```bash
go run . -id=B -bind=127.0.0.1:8001 -join=127.0.0.1:8000
```

### Enable eBPF L4 redirect (Linux + root only)

```bash
sudo go run . -id=A -bind=127.0.0.1:8000 -ebpf
```

### Interactive commands (while the node is running)

```
members   — print the known member list
quit      — shut down
```

### Port layout

| Port | Purpose |
|---|---|
| `bind` (e.g. 8000) | SWIM gossip (UDP) |
| `bind + 1000` (e.g. 9000) | App HTTP server |
| `bind + 2000` (e.g. 10000) | L7 proxy HTTP server |

---

## Common Issues

**`stat .../cmd/raft/cd: directory not found`**
This happens when you run `go build -o bin/raft ./cmd/raft/cd ebpf ...` as one line. The shell treats `cd ebpf` as part of the path. Run each command separately.

**`go: cannot write multiple packages to non-directory <name>`**
`go build -o filename ./...` fails because `./...` matches both `package main` roots. Target each binary individually:
```bash
go build -o bin/mesh-sidecar .
go build -o bin/raft ./cmd/raft/
```

**`package ebpf is not in std`**
Run `go generate` from inside the `ebpf/` directory, not the repo root:
```bash
cd ebpf && go generate && cd ..
```

**Tests time out on `TestRaft_*`**
Increase the timeout: `go test -timeout 120s`

**`go test ./...` fails to compile the `ebpf` package**
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