package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
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
		}
		defer redirector.Close()
		log.Println("eBPF L4 redirector attached")

		// sync membership into BPF map every second
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				syncRedirectMap(redirector, node)
			}
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
func syncRedirectMap(r *meshebpf.Redirector, node *members.Node) {
	snapshot := node.SnapShot()

	// build a list of alive members only
	var alive []members.MemberState
	for _, m := range snapshot {
		if m.Status == "Alive" {
			alive = append(alive, m)
		}
	}

	for i, m := range snapshot {
		host, portStr, err := net.SplitHostPort(m.Addr.String())
		if err != nil {
			continue
		}
		gossipPort, err := strconv.Atoi(portStr)
		if err != nil {
			continue
		}
		appPort := uint16(gossipPort + 1000)

		if m.Status != "Alive" {
			// dead or suspect -- remove from BPF map so kernel
			// stops routing connections to this member
			if err := r.RemoveTarget(appPort); err != nil {
				log.Printf("remove redirect %s: %v", m.ID, err)
			}
			continue
		}

		// pick a different alive member as the redirect target
		// using round-robin rotation based on this member's index
		if len(alive) < 2 {
			// only one alive member -- no one else to redirect to,
			// point it at itself so connections still work
			ip := net.ParseIP(host)
			if ip != nil {
				r.SetTarget(appPort, ip, appPort)
			}
			continue
		}

		// pick the next alive member in the list, skipping self
		var target *members.MemberState
		for j := 1; j <= len(alive); j++ {
			candidate := alive[(i+j)%len(alive)]
			if candidate.ID != m.ID {
				target = &candidate
				break
			}
		}
		if target == nil {
			continue
		}

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

		if err := r.SetTarget(appPort, targetIP, targetAppPort); err != nil {
			log.Printf("set redirect %s → %s: %v", m.ID, target.ID, err)
		} else {
			log.Printf("redirect: connections to :%d → %s:%d (%s)",
				appPort, targetHost, targetAppPort, target.ID)
		}
	}
}

func printMembers(node *members.Node) {
	members := node.SnapShot()
	for _, value := range members {
		fmt.Println(value)
	}
}
