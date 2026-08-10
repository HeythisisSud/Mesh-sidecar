package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	

	meshebpf "github.com/HeythisisSud/mesh-sidecar/ebpf"
	"github.com/HeythisisSud/mesh-sidecar/members"
	"github.com/HeythisisSud/mesh-sidecar/proxy"
)

func main() {
	id := flag.String("id", "", "unique ID for this node (required)")
	bind := flag.String("bind", "", "address to bind on, e.g. 127.0.0.1:8000 (required)")
	join := flag.String("join", "", "address of an existing member to join (optional)")
	useEBPF := flag.Bool("ebpf", false, "enable eBPF L4 redirect (requires root)")
	flag.Parse()

	if *id == "" || *bind == "" {
		log.Fatal("both -id and -bind are required")
	}

	bindAddr, err := net.ResolveUDPAddr("udp", *bind)
	if err != nil {
		log.Fatalf("bad bind address: %v", err)
	}

	conn, err := net.ListenUDP("udp", bindAddr)
	if err != nil {
		log.Fatalf("failed to bind: %v", err)
	}
	defer conn.Close()

	node := members.NewNode(*id, bindAddr, conn)
	node.Start()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
	    <-sigCh
	    fmt.Println("\nshutting down...")
	    if redirector != nil {
	        redirector.Close()
	    }
	    conn.Close()
	    os.Exit(0)
	}()

	// fake app backend -- represents "the real service" on this node
	appPort := bindAddr.Port + 1000
	go func() {
		err := http.ListenAndServe(fmt.Sprintf(":%d", appPort), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("hello from " + *id))
		}))
		if err != nil {
			log.Printf("app server failed: %v", err)
		}
	}()

	// L7 proxy -- Phase 2, still running alongside eBPF
	proxyHandler, err := proxy.NewProxy(node)
	if err != nil {
		log.Fatalf("failed to create proxy: %v", err)
	}
	proxyPort := bindAddr.Port + 2000
	go func() {
		err := http.ListenAndServe(fmt.Sprintf(":%d", proxyPort), proxyHandler)
		if err != nil {
			log.Printf("proxy server failed: %v", err)
		}
	}()

	// L4 eBPF redirector -- Phase 3, optional via --ebpf flag
	var redirector *meshebpf.Redirector
	if *useEBPF {
    cgroupPath, err := meshebpf.DefaultCgroupPath()
    if err != nil {
        log.Fatalf("cgroup not found: %v", err)
    }

    redirector, err = meshebpf.NewRedirector(cgroupPath)
    if err != nil {
    log.Fatalf("failed to create eBPF redirector: %v", err)
} else {
        defer redirector.Close()
        log.Println("eBPF redirector attached -- this node owns the BPF map")

        // only the owning node syncs the map
        go func() {
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()
    for range ticker.C {
        syncRedirectMap(redirector, node, *id, bindAddr)
    }
}()
    }
	statusPort := bindAddr.Port + 3000
go func() {
    http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
        snapshot := node.SnapShot()
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(snapshot)
    })
    http.ListenAndServe(fmt.Sprintf(":%d", statusPort), nil)
}()
}

	fmt.Printf("node %q listening on %s (app: %d, proxy: %d)\n", *id, *bind, appPort, proxyPort)

	if *join != "" {
		peerAddr, err := net.ResolveUDPAddr("udp", *join)
		if err != nil {
			log.Fatalf("bad join address: %v", err)
		}
		fmt.Printf("joining via %s...\n", *join)
		if err := node.Join(peerAddr); err != nil {
			log.Printf("join failed: %v", err)
		} else {
			fmt.Println("join succeeded")
		}
	}

	fmt.Println("commands: 'members' to list known members, 'quit' to exit")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		switch strings.TrimSpace(scanner.Text()) {
		case "members":
			printMembers(node)
		case "quit":
			return
		default:
			fmt.Println("unknown command (try 'members' or 'quit')")
		}
	}
}

// syncRedirectMap pushes the current alive member list into the BPF
// redirect map so the kernel knows where to forward connections.
func syncRedirectMap(r *meshebpf.Redirector, node *members.Node, selfID string, selfAddr *net.UDPAddr) {
	snapshot := node.SnapShot()

	// build alive list including self
	alive := []members.MemberState{{
		ID:     selfID,
		Addr:   selfAddr,
		Status: "Alive",
	}}
	for _, m := range snapshot {
		if m.Status == "Alive" {
			alive = append(alive, m)
		}
	}
	sort.Slice(alive, func(i, j int) bool {
		return alive[i].ID < alive[j].ID
	})

	// remove dead members from map
	for _, m := range snapshot {
		if m.Status != "Alive" {
			_, portStr, _ := net.SplitHostPort(m.Addr.String())
			gossipPort, _ := strconv.Atoi(portStr)
			r.RemoveTarget(uint16(gossipPort + 1000))
		}
	}

	if len(alive) < 2 {
		return
	}

	// each node only installs its OWN redirect entry to avoid loops:
	// when curl hits port 9001 after being redirected, the hook finds
	// no entry for 9001 in node A's map and lets it through
	// write ALL entries -- A→B, B→C, C→A
for i, m := range alive {
    target := alive[(i+1)%len(alive)]

    _, srcPortStr, _ := net.SplitHostPort(m.Addr.String())
    srcGossipPort, _ := strconv.Atoi(srcPortStr)
    srcAppPort := uint16(srcGossipPort + 1000)

    targetHost, targetPortStr, _ := net.SplitHostPort(target.Addr.String())
    targetGossipPort, _ := strconv.Atoi(targetPortStr)
    targetAppPort := uint16(targetGossipPort + 1000)
    targetIP := net.ParseIP(targetHost)
    if targetIP == nil {
        continue
    }

    if err := r.SetTarget(srcAppPort, targetIP, targetAppPort); err != nil {
        log.Printf("set redirect %s→%s: %v", m.ID, target.ID, err)
    } else {
        log.Printf("redirect: :%d → %s:%d (%s)",
            srcAppPort, targetHost, targetAppPort, target.ID)
    }
}
}

func printMembers(node *members.Node) {
	members := node.SnapShot()
	for _, value := range members {
		fmt.Println(value)
	}
}
