# mesh-sidecar

A service mesh sidecar built from scratch in Go, for learning purposes. Implements three layers of a real service mesh — gossip-based membership, L7 HTTP proxying, and eBPF L4 transparent redirection — without using any existing mesh framework.

---

## What is a service mesh sidecar?

In a distributed system, services need to find and talk to each other reliably. A service mesh sidecar sits alongside each application instance and handles that networking transparently — tracking which nodes are alive, routing traffic to healthy backends, and failing over automatically when something dies. The application itself doesn't need to know any of this is happening.

This project implements the core of that idea from first principles, across three phases.

---

## Architecture

```
┌─────────────────────────────────────────┐
│             Application                 │
│         (fake HTTP backend)             │
├─────────────────────────────────────────┤
│          L7 Proxy (Phase 2)             │
│   httputil.ReverseProxy + Snapshot()    │
│   routes HTTP traffic to alive nodes    │
├─────────────────────────────────────────┤
│      SWIM Gossip Membership (Phase 1)   │
│   UDP ping/ack, suspect/confirm,        │
│   piggybacked updates, indirect ping    │
├─────────────────────────────────────────┤
│     eBPF L4 Redirect (Phase 3)          │
│   cgroup/connect4 hook, BPF map,        │
│   transparent TCP redirection           │
└─────────────────────────────────────────┘
```

Each node runs all three layers simultaneously in a single binary. Ports per node (example with bind port 8000):

| Layer | Port |
|---|---|
| Gossip (UDP) | 8000 |
| App backend | 9000 |
| L7 proxy | 10000 |
| Status API | 11000 |

---

## How each phase works

### Phase 1 — SWIM gossip membership

Every node maintains a membership table of who it knows about and whether they are `Alive`, `Suspect`, or `Confirm` (dead). Nodes exchange this information via UDP using the SWIM protocol:

- Every second, each node picks one random peer and sends it a `PING`
- If no `ACK` arrives within 500ms, the node asks up to 3 other peers to relay an indirect ping (`PING-REQ`) — this distinguishes a dead node from a network partition between two specific nodes
- If indirect ping also fails after another 500ms, the target is marked `Suspect`
- After 3 seconds in Suspect state with no refutation, the node is marked `Confirm` and removed from the membership table
- Every message carries piggybacked `Update` entries, gossipping state changes across the cluster without any broadcast

A node that is incorrectly suspected can refute it by broadcasting a higher-incarnation `Alive` update about itself.

### Phase 2 — L7 HTTP proxy

Each node runs an HTTP reverse proxy (`httputil.ReverseProxy`) alongside its app backend. Instead of routing to a hardcoded address, the proxy calls `node.Snapshot()` on every incoming request to get the current live membership, picks the next alive node in sorted round-robin order, and forwards the request there.

Because the proxy reads from the same membership table that gossip maintains, it automatically stops sending traffic to a node within one sync cycle of that node being marked dead — no manual intervention, no config reload.

### Phase 3 — eBPF L4 transparent redirection

The L7 proxy requires the client to deliberately connect to the proxy port. eBPF removes that requirement entirely.

A `cgroup/connect4` BPF program is compiled (via clang) and loaded into the kernel, attached to the cgroup that WSL2 processes live in. This program intercepts every outgoing TCP `connect()` syscall before the connection is established. If the destination port matches an entry in a BPF hash map, the kernel silently rewrites the destination IP and port — the client never knows the switch happened.

A goroutine in the owning node (node A) syncs the live membership table into the BPF map every second, maintaining the rotation:

```
A:9000 → B:9001
B:9001 → C:9002
C:9002 → A:9000
```

When a node dies and is confirmed by gossip, its BPF map entry is removed and the rotation updates automatically.

---

## Requirements

- Linux kernel >= 5.15 with `CONFIG_CGROUP_BPF=y` and `CONFIG_DEBUG_INFO_BTF=y`
- WSL2 (Ubuntu 22.04 or 24.04) or native Linux
- Go 1.21+
- `clang`, `llvm`, `libbpf-dev` (for eBPF compilation)

```bash
sudo apt install -y clang llvm libbpf-dev linux-headers-generic build-essential
go install github.com/cilium/ebpf/cmd/bpf2go@latest
```

---

## Building

```bash
# compile the eBPF C program into Go-embeddable bytecode
cd ebpf
go generate
cd ..

# build the binary
go build -o mesh-sidecar ./...
```

---

## Running

```bash
# mount bpffs once per boot (needed for shared BPF map)
sudo mount -t bpf bpf /sys/fs/bpf

# node A -- owns the eBPF program and BPF map
sudo ./mesh-sidecar -id=A -bind=127.0.0.1:8000 -ebpf

# node B -- joins through A, gossip only
sudo ./mesh-sidecar -id=B -bind=127.0.0.1:8001 -join=127.0.0.1:8000

# node C -- joins through A, gossip only
sudo ./mesh-sidecar -id=C -bind=127.0.0.1:8002 -join=127.0.0.1:8000
```

**REPL commands** (in any node terminal):

```
members   -- print current membership view
quit      -- graceful shutdown
```

**Test L4 redirection** (after all three nodes have joined):

```bash
# these should return responses from DIFFERENT nodes than the port implies
curl http://127.0.0.1:9000/   # → hello from B
curl http://127.0.0.1:9001/   # → hello from C
curl http://127.0.0.1:9002/   # → hello from A
```

**Test failure detection**:

```bash
# kill node B (Ctrl+C in its terminal)
# wait ~5 seconds for suspect → confirm cascade
curl http://127.0.0.1:9001/   # → connection refused (B removed from map)
```

**Status API**:

```bash
curl http://127.0.0.1:11000/status | jq
```

---

## Project structure

```
mesh-sidecar/
├── main.go              # wires all three phases together
├── members/
│   └── choosing.go      # SWIM gossip: Node, MemberState, ping/ack/suspect/confirm
├── proxy/
│   └── proxy.go         # L7 HTTP reverse proxy with live membership routing
└── ebpf/
    ├── redirect.c        # eBPF C program: cgroup/connect4 hook + BPF map
    ├── ebpf.go           # bpf2go generate directive
    ├── redirect.go       # Go loader: attach program, read/write BPF map
    ├── redirect_bpfel.go # generated by bpf2go (little-endian)
    └── redirect_bpfeb.go # generated by bpf2go (big-endian)
```

---

## Known simplifications

This is a learning project. Several things are deliberately simplified compared to a production service mesh:

**Fixed port offset convention** — app port = gossip port + 1000, proxy port = gossip port + 2000. A real system would gossip the actual service address so nodes aren't coupled to a port numbering scheme.

**Single BPF program owner** — only one node (whichever starts first with `-ebpf`) attaches the kernel program. In a real deployment each physical machine runs one sidecar that owns its own cgroup attachment independently.

**No TLS** — all gossip and proxy traffic is plaintext. Production meshes use mTLS for both encryption and identity verification between nodes.

**Manual seed address** — a joining node needs to be given one existing member's address via `-join`. Real systems use DNS SRV records, cloud provider metadata APIs, or a dedicated discovery service to bootstrap this.

**No service registry** — there is one implicit "service" (the fake HTTP backend on each node). A real mesh would separate the concepts of "node" and "service instance," supporting multiple services per node and routing by service name rather than by port.

**In-memory state only** — membership state is lost on restart. A real system would persist enough state to rejoin the cluster gracefully after a crash.

---

## References

- [SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol](https://ieeexplore.ieee.org/document/1028914) — Das, Gupta, Motivala (2002). The paper this gossip implementation is based on.
- [hashicorp/memberlist](https://github.com/hashicorp/memberlist) — production Go implementation of SWIM, used in Consul and Serf. Useful to compare against after reading the paper.
- [cilium/ebpf](https://github.com/cilium/ebpf) — the Go library used for loading and managing eBPF programs.
- [ebpf-go getting started guide](https://ebpf-go.dev/guides/getting-started/) — walkthrough of the bpf2go toolchain used in Phase 3.
- [Envoy Proxy](https://www.envoyproxy.io/) and [Linkerd](https://linkerd.io/) — production service mesh data planes that implement the same ideas at scale.
