package raft

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/HeythisisSud/mesh-sidecar/raft/proto"
)

func (n *Node) callRequestVote(peer string, term, lastLogIndex, lastLogTerm uint64) bool {
	conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[%s] failed to connect to %s: %v", n.id, peer, err)
		return false
	}
	defer conn.Close()

	client := pb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	resp, err := client.RequestVote(ctx, &pb.RequestVoteRequest{
		Term:         term,
		CandidateId:  n.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	})
	if err != nil {
		log.Printf("[%s] RequestVote to %s failed: %v", n.id, peer, err)
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// if response has higher term, step down immediately
	if resp.Term > n.currentTerm {
		n.stepDown(resp.Term)
		return false
	}

	return resp.VoteGranted
}

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	term := n.currentTerm
	peers := n.peers
	commitIndex := n.commitIndex
	n.mu.Unlock()

	for _, peer := range peers {
		go n.callAppendEntries(peer, term, commitIndex, nil)
	}
}

func (n *Node) callAppendEntries(peer string, term, commitIndex uint64, entries []LogEntry) bool {
	n.mu.Lock()
	prevLogIndex := n.nextIndex[peer] - 1
	var prevLogTerm uint64
	if prevLogIndex > 0 && prevLogIndex <= uint64(len(n.log)) {
		prevLogTerm = n.log[prevLogIndex-1].Term
	}
	n.mu.Unlock()

	conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false
	}
	defer conn.Close()

	client := pb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	pbEntries := make([]*pb.LogEntry, len(entries))
	for i, e := range entries {
		pbEntries[i] = &pb.LogEntry{Term: e.Term, Command: e.Command}
	}

	resp, err := client.AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      pbEntries,
		LeaderCommit: commitIndex,
	})
	if err != nil {
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.currentTerm {
		n.stepDown(resp.Term)
		return false
	}
	if len(entries) > 0 {
    log.Printf("[%s] callAppendEntries to %s: success=%v", n.id, peer, resp.Success)
}

	return resp.Success
}

// applyCommitted applies all committed but not yet applied log entries
// to the state machine via ApplyCh.
func (n *Node) applyCommitted() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]
		n.ApplyCh <- entry
	}
}