package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
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

	// Declare redirector before signal goroutine so the closure can capture it.
	var redirector *meshebpf.Redirector

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

	// Fake app backend — represents "the real service" on this node.
	appPort := bindAddr.Port + 1000
	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", appPort), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("hello from " + *id))
		})); err != nil {
			log.Printf("app server failed: %v", err)
		}
	}()

	// L7 proxy — Phase 2.
	proxyHandler, err := proxy.NewProxy(node)
	if err != nil {
		log.Fatalf("failed to create proxy: %v", err)
	}
	proxyPort := bindAddr.Port + 2000
	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", proxyPort), proxyHandler); err != nil {
			log.Printf("proxy server failed: %v", err)
		}
	}()

	// L4 eBPF redirector — Phase 3, optional.
	if *useEBPF {
		cgroupPath, err := meshebpf.DefaultCgroupPath()
		if err != nil {
			log.Fatalf("cgroup not found: %v", err)
		}
		redirector, err = meshebpf.NewRedirector(cgroupPath)
		if err != nil {
			log.Fatalf("failed to create eBPF redirector: %v", err)
		}
		defer redirector.Close()
		log.Printf("eBPF L4 redirector attached (cgroup: %s)", cgroupPath)

		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				syncRedirectMap(redirector, node, *id, bindAddr)
			}
		}()
	}

	// Status endpoint — useful for debugging.
	statusPort := bindAddr.Port + 3000
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
			snapshot := node.SnapShot()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(snapshot)
		})
		if err := http.ListenAndServe(fmt.Sprintf(":%d", statusPort), mux); err != nil {
			log.Printf("status server failed: %v", err)
		}
	}()

	fmt.Printf("node %q listening on %s (app: %d, proxy: %d, status: %d)\n",
		*id, *bind, appPort, proxyPort, statusPort)

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

// syncRedirectMap pushes the current alive member list into the BPF redirect map.
// selfID and selfAddr are injected because node.SnapShot() only returns remote peers.
func syncRedirectMap(r *meshebpf.Redirector, node *members.Node, selfID string, selfAddr *net.UDPAddr) {
	snapshot := node.SnapShot()
	snapshot = append(snapshot, members.MemberState{
		ID:     selfID,
		Addr:   selfAddr,
		Status: "Alive",
	})

	var alive []members.MemberState
	var dead []members.MemberState
	for _, m := range snapshot {
		if m.Status == "Alive" {
			alive = append(alive, m)
		} else {
			dead = append(dead, m)
		}
	}

	sort.Slice(alive, func(i, j int) bool { return alive[i].ID < alive[j].ID })

	for _, m := range dead {
		_, portStr, err := net.SplitHostPort(m.Addr.String())
		if err != nil {
			continue
		}
		gossipPort, err := strconv.Atoi(portStr)
		if err != nil {
			continue
		}
		appPort := uint16(gossipPort + 1000)
		if err := r.RemoveTarget(appPort); err != nil {
			log.Printf("remove redirect %s: %v", m.ID, err)
		}
	}

	if len(alive) < 2 {
		return
	}

	for i, m := range alive {
		target := alive[(i+1)%len(alive)]

		_, srcPortStr, err := net.SplitHostPort(m.Addr.String())
		if err != nil {
			continue
		}
		srcGossipPort, err := strconv.Atoi(srcPortStr)
		if err != nil {
			continue
		}
		srcAppPort := uint16(srcGossipPort + 1000)

		targetHost, targetPortStr, err := net.SplitHostPort(target.Addr.String())
		if err != nil {
			continue
		}
		targetGossipPort, err := strconv.Atoi(targetPortStr)
		if err != nil {
			continue
		}
		targetAppPort := uint16(targetGossipPort + 1000)
		targetIP := net.ParseIP(targetHost)
		if targetIP == nil {
			continue
		}

		if err := r.SetTarget(srcAppPort, targetIP, targetAppPort); err != nil {
			log.Printf("set redirect %s->%s: %v", m.ID, target.ID, err)
		}
	}
}

func printMembers(node *members.Node) {
	for _, m := range node.SnapShot() {
		fmt.Println(m)
	}
}
