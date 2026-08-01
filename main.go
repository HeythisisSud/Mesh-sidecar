package main


import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/HeythisisSud/mesh-sidecar/members"
)

func main() {
	id := flag.String("id", "", "unique ID for this node (required)")
	bind := flag.String("bind", "", "address to bind on, e.g. 127.0.0.1:8000 (required)")
	join := flag.String("join", "", "address of an existing member to join, e.g. 127.0.0.1:8000 (optional)")
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

	fmt.Printf("node %q listening on %s\n", *id, *bind)

	// If a --join address was given, attempt to join that cluster.
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

	// Simple REPL: type "members" + Enter to print the current view.
	// Type "quit" to exit.
	fmt.Println("commands: 'members' to list known members, 'quit' to exit")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		switch scanner.Text() {
		case "members":
			printMembers(node)
		case "quit":
			return
		default:
			fmt.Println("unknown command (try 'members' or 'quit')")
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("reading commands: %v", err)
	}
}

func printMembers(node *members.Node) {
	// NOTE: this reaches into node.Members directly for a quick test view.
	// Since Members isn't guarded by a public accessor yet, this only
	// works because it's in the same process — a real CLI/API would need
	// an exported, lock-protected method on Node for this.
	fmt.Println("---")
	for id, m := range node.Members {
		fmt.Printf("  %-10s %-20s status=%-8s incarnation=%d last_seen=%s\n",
			id, m.Addr.String(), m.Status, m.Incarnation, m.LastSeen.Format(time.Kitchen))
	}
	fmt.Println("---")
}
