package raft

import (
	"context"
	"log"

	pb "github.com/HeythisisSud/mesh-sidecar/raft/proto"
)


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

	
	if req.Term < n.currentTerm {
		log.Printf("[%s] rejecting vote for %s: stale term %d < %d",
			n.id, req.CandidateId, req.Term, n.currentTerm)
		return resp, nil
	}

	
	if req.Term > n.currentTerm {
		n.stepDown(req.Term)
		resp.Term = n.currentTerm
	}

	
	
	alreadyVoted := n.votedFor != "" && n.votedFor != req.CandidateId
	if alreadyVoted {
		log.Printf("[%s] rejecting vote for %s: already voted for %s",
			n.id, req.CandidateId, n.votedFor)
		return resp, nil
	}

	
	
	lastIdx, lastTerm := n.lastLogInfo()
	candidateUpToDate := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx)

	if !candidateUpToDate {
		log.Printf("[%s] rejecting vote for %s: log not up to date", n.id, req.CandidateId)
		return resp, nil
	}

	
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

	
	if req.Term < n.currentTerm {
		return resp, nil
	}

	
	if req.Term > n.currentTerm {
		n.stepDown(req.Term)
		resp.Term = n.currentTerm
	}

	
	n.state = Follower
	n.resetElectionTimeout()

	
	if req.PrevLogIndex > 0 {
		if uint64(len(n.log)) < req.PrevLogIndex {
			
			return resp, nil
		}
		if n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			
			return resp, nil
		}
	}

	
	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + uint64(i) + 1
		if uint64(len(n.log)) >= idx {
			if n.log[idx-1].Term != entry.Term {
				
				n.log = n.log[:idx-1]
			} else {
				continue 
			}
		}
		n.log = append(n.log, LogEntry{
			Term:    entry.Term,
			Command: entry.Command,
		})
	}

	
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
