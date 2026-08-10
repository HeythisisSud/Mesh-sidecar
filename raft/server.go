package raft

import (
	"context"
	"log"

	pb "github.com/HeythisisSud/mesh-sidecar/raft/proto"
)

// RaftServer implements the gRPC RaftService interface.
type RaftServer struct {
	pb.UnimplementedRaftServiceServer
	node *Node
}

func NewRaftServer(node *Node) *RaftServer {
	return &RaftServer{node: node}
}

func (s *RaftServer) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	n := s.node
	resp := &pb.RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	// rule 1: if request term < our term, reject immediately
	if req.Term < n.currentTerm {
		log.Printf("[%s] rejecting vote for %s: stale term %d < %d",
			n.id, req.CandidateId, req.Term, n.currentTerm)
		return resp, nil
	}

	// rule 2: if request term > our term, step down first
	if req.Term > n.currentTerm {
		n.stepDown(req.Term)
		resp.Term = n.currentTerm
	}

	// rule 3: only vote if we haven't voted yet in this term,
	// or we already voted for this candidate
	alreadyVoted := n.votedFor != "" && n.votedFor != req.CandidateId
	if alreadyVoted {
		log.Printf("[%s] rejecting vote for %s: already voted for %s",
			n.id, req.CandidateId, n.votedFor)
		return resp, nil
	}

	// rule 4: only vote if candidate's log is at least as up-to-date as ours
	// "up-to-date" means: higher last term, or same last term with longer log
	lastIdx, lastTerm := n.lastLogInfo()
	candidateUpToDate := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx)

	if !candidateUpToDate {
		log.Printf("[%s] rejecting vote for %s: log not up to date", n.id, req.CandidateId)
		return resp, nil
	}

	// grant the vote
	n.votedFor = req.CandidateId
	n.resetElectionTimeout()
	resp.VoteGranted = true
	log.Printf("[%s] granted vote to %s for term %d", n.id, req.CandidateId, req.Term)
	return resp, nil
}

func (s *RaftServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	n := s.node
	resp := &pb.AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: false,
	}

	// rule 1: reject if request term < our term
	if req.Term < n.currentTerm {
		return resp, nil
	}

	// rule 2: if request term >= our term, recognize this as the current leader
	if req.Term > n.currentTerm {
		n.stepDown(req.Term)
		resp.Term = n.currentTerm
	}

	// valid leader -- reset election timeout so we don't start an election
	n.state = Follower
	n.resetElectionTimeout()

	// rule 3: check that our log contains an entry at prevLogIndex with prevLogTerm
	if req.PrevLogIndex > 0 {
		if uint64(len(n.log)) < req.PrevLogIndex {
			// we don't have the entry at prevLogIndex at all
			return resp, nil
		}
		if n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			// we have an entry at prevLogIndex but with a different term
			return resp, nil
		}
	}

	// rule 4: append any new entries, deleting conflicting ones first
	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + uint64(i) + 1
		if uint64(len(n.log)) >= idx {
			if n.log[idx-1].Term != entry.Term {
				// conflict -- truncate log from here
				n.log = n.log[:idx-1]
			} else {
				continue // already have this entry
			}
		}
		n.log = append(n.log, LogEntry{
			Term:    entry.Term,
			Command: entry.Command,
		})
	}

	// rule 5: update commitIndex if leader says so
	if req.LeaderCommit > n.commitIndex {
		lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))
		if req.LeaderCommit < lastNewIndex {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastNewIndex
		}
		go n.applyCommitted()
	}

	resp.Success = true
	return resp, nil
}
