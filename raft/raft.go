package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

type State int

const (
	Follower  State = iota
	Candidate State = iota
	Leader    State = iota
)

type LogEntry struct {
	Term    uint64
	Command string
}

type Node struct {
	mu sync.Mutex
	id    string
	peers []string // gRPC addresses of all other nodes

	// persistent state
	currentTerm uint64
	votedFor    string
	log         []LogEntry

	// volatile state
	commitIndex uint64
	lastApplied uint64

	// leader-only volatile state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// election state
	state           State
	electionTimeout time.Duration
	lastHeartbeat   time.Time

	// channel to notify when a command is committed
	applyCh chan LogEntry
}

func NewNode(id string, peers []string, applyCh chan LogEntry) *Node {
	n := &Node{
		id:            id,
		peers:         peers,
		currentTerm:   0,
		votedFor:      "",
		log:           make([]LogEntry, 0),
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make(map[string]uint64),
		matchIndex:    make(map[string]uint64),
		state:         Follower,
		applyCh:       applyCh,
	}
	n.resetElectionTimeout()
	return n
}


func (n *Node) resetElectionTimeout() {
	n.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	n.lastHeartbeat = time.Now()
}

func (n *Node) Start() {
	go n.electionLoop()
}


func (n *Node) electionLoop() {
	for {
		n.mu.Lock()
		state := n.state
		elapsed := time.Since(n.lastHeartbeat)
		timeout := n.electionTimeout
		n.mu.Unlock()

		switch state {
		case Follower, Candidate:
			if elapsed >= timeout {
				n.startElection()
			}
		case Leader:
			// leader sends heartbeats, doesn't wait for election timeout
			n.sendHeartbeats()
			time.Sleep(50 * time.Millisecond)
			continue
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// startElection transitions this node to Candidate,
// increments its term, votes for itself, and sends
// RequestVote RPCs to all peers.
func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.resetElectionTimeout()
	term := n.currentTerm
	lastLogIndex, lastLogTerm := n.lastLogInfo()
	peers := n.peers
	n.mu.Unlock()

	log.Printf("[%s] starting election for term %d", n.id, term)

	votes := 1 
	voteMu := sync.Mutex{}

	for _, peer := range peers {
		go func(peer string) {
			granted := n.callRequestVote(peer, term, lastLogIndex, lastLogTerm)
			if !granted {
				return
			}

			voteMu.Lock()
			votes++
			currentVotes := votes
			voteMu.Unlock()

			
			majority := (len(peers)+1)/2 + 1
			if currentVotes >= majority {
				n.becomeLeader(term)
			}
		}(peer)
	}
}

func (n *Node) becomeLeader(term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// only become leader if we're still a candidate in the same term
	// -- we might have already lost the election or seen a higher term
	if n.state != Candidate || n.currentTerm != term {
		return
	}

	n.state = Leader
	log.Printf("[%s] became leader for term %d", n.id, term)

	// initialize nextIndex for each peer to leader's last log index + 1
	for _, peer := range n.peers {
		n.nextIndex[peer] = uint64(len(n.log)) + 1
		n.matchIndex[peer] = 0
	}
}

// lastLogInfo returns the index and term of the last log entry.
// Called with n.mu held.
func (n *Node) lastLogInfo() (uint64, uint64) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return uint64(len(n.log)), last.Term
}

// stepDown reverts this node to follower if it sees a higher term.
// Called whenever any RPC response carries a term > currentTerm.
func (n *Node) stepDown(term uint64) {
	n.currentTerm = term
	n.state = Follower
	n.votedFor = ""
	n.resetElectionTimeout()
}
