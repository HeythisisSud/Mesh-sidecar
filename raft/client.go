package raft

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/HeythisisSud/mesh-sidecar/raft/proto"
)

// callRequestVote does NOT hold n.mu across the blocking gRPC call.
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

// callAppendEntries does NOT hold n.mu across the blocking gRPC call.
// If entries are supplied and the follower rejects due to log mismatch,
// nextIndex is decremented and the call retried (standard Raft catch-up).
func (n *Node) callAppendEntries(peer string, term, commitIndex uint64, entries []LogEntry) bool {
	n.mu.Lock()
	if n.nextIndex[peer] == 0 {
		n.nextIndex[peer] = 1
	}
	prevLogIndex := n.nextIndex[peer] - 1
	var prevLogTerm uint64
	if prevLogIndex > 0 && prevLogIndex <= uint64(len(n.log)) {
		prevLogTerm = n.log[prevLogIndex-1].Term
	}
	// If we have entries to send, include everything from prevLogIndex+1 onward.
	if len(entries) > 0 && prevLogIndex+1 <= uint64(len(n.log)) {
		entries = make([]LogEntry, len(n.log)-int(prevLogIndex))
		copy(entries, n.log[prevLogIndex:])
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
	if !resp.Success && len(entries) > 0 && n.nextIndex[peer] > 1 {
		// Log inconsistency: back up and will retry on next heartbeat.
		n.nextIndex[peer]--
	}
	return resp.Success
}

// applyCommitted applies committed-but-not-yet-applied entries to ApplyCh.
//
// CRITICAL: we snapshot the entries under the lock, then release the lock
// BEFORE sending to ApplyCh. Sending to a buffered channel while holding
// n.mu would deadlock if the channel fills (100 entries).
func (n *Node) applyCommitted() {
	n.mu.Lock()
	var toApply []LogEntry
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		toApply = append(toApply, n.log[n.lastApplied-1])
	}
	n.mu.Unlock()

	for _, entry := range toApply {
		n.ApplyCh <- entry
	}
}
