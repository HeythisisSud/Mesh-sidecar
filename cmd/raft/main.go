package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/HeythisisSud/mesh-sidecar/raft"
	pb "github.com/HeythisisSud/mesh-sidecar/raft/proto"
)

func startNode(id string, addr string, peers []string) (*raft.Node, *raft.KVStore) {
	node := raft.NewNode(id, peers)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[%s] failed to listen on %s: %v", id, addr, err)
	}
	srv := grpc.NewServer()
	pb.RegisterRaftServiceServer(srv, raft.NewRaftServer(node))
	go srv.Serve(lis)

	kv := raft.NewKVStore(node)
	node.Start()
	log.Printf("[%s] started on %s", id, addr)
	return node, kv
}

func main() {
	nodes := []struct {
		id    string
		addr  string
		peers []string
	}{
		{"A", "127.0.0.1:7000", []string{"127.0.0.1:7001", "127.0.0.1:7002"}},
		{"B", "127.0.0.1:7001", []string{"127.0.0.1:7000", "127.0.0.1:7002"}},
		{"C", "127.0.0.1:7002", []string{"127.0.0.1:7000", "127.0.0.1:7001"}},
	}

	var raftNodes []*raft.Node
	var kvStores []*raft.KVStore

	for _, n := range nodes {
		rn, kv := startNode(n.id, n.addr, n.peers)
		raftNodes = append(raftNodes, rn)
		kvStores = append(kvStores, kv)
	}

	fmt.Println("started -- waiting for election...")

	// find leader and submit KV operations
	go func() {
		time.Sleep(500 * time.Millisecond)
		for {
			for i, rn := range raftNodes {
				state, _ := rn.Status()
				if state == "Leader" {
					kv := kvStores[i]
					fmt.Printf(">>> leader is %s, submitting KV ops\n", nodes[i].id)

					kv.Set("x", "1")
					kv.Set("y", "hello")
					kv.Set("x", "2") // overwrite x

					// give followers time to apply
					time.Sleep(200 * time.Millisecond)

					// read from all nodes -- should all agree
					for j, store := range kvStores {
						snap := store.Snapshot()
						fmt.Printf("[%s] state: %v\n", nodes[j].id, snap)
					}

					kv.Delete("y")
					time.Sleep(200 * time.Millisecond)

					fmt.Println("after DEL y:")
					for j, store := range kvStores {
						snap := store.Snapshot()
						fmt.Printf("[%s] state: %v\n", nodes[j].id, snap)
					}

					return
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	fmt.Println("shutting down")
}
