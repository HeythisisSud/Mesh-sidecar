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
	ApplyCh chan LogEntry
	done chan struct{}
}

func NewNode(id string, peers []string) *Node {
	n := &Node{
		id:          id,
		peers:       peers,
		currentTerm: 0,
		votedFor:    "",
		log:         make([]LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		state:       Follower,
		ApplyCh:     make(chan LogEntry, 100),
		done:        make(chan struct{}),
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
		select {
		case <-n.done:
			return
		default:
		}

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

// add this to raft/raft.go
func (n *Node) Status() (state string, term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	switch n.state {
	case Leader:
		return "Leader", n.currentTerm
	case Candidate:
		return "Candidate", n.currentTerm
	default:
		return "Follower", n.currentTerm
	}
}


func (n *Node) Stop() {
	close(n.done)
}

// Submit proposes a new command to the Raft cluster.
// Only works if this node is the leader.
// Returns the log index the entry was assigned, and whether
// this node is actually the leader.
func (n *Node) Submit(command string) (uint64, bool) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return 0, false
	}

	entry := LogEntry{Term: n.currentTerm, Command: command}
	n.log = append(n.log, entry)
	index := uint64(len(n.log))
	term := n.currentTerm
	peers := n.peers
	commitIndex := n.commitIndex

	log.Printf("[%s] submitted command %q at index %d term %d",
		n.id, command, index, term)
	n.mu.Unlock()

	// confirmCh receives one message per peer that confirms replication
	confirmCh := make(chan bool, len(peers))

	for _, peer := range peers {
		go func(peer string) {
			success := n.callAppendEntries(peer, term, commitIndex, []LogEntry{entry})
			if success {
				n.mu.Lock()
				if index > n.matchIndex[peer] {
					n.matchIndex[peer] = index
					n.nextIndex[peer] = index + 1
				}
				n.mu.Unlock()
			}
			confirmCh <- success
		}(peer)
	}

	// wait for majority -- we already have 1 (ourselves)
	majority := (len(peers)+1)/2 + 1
	confirmed := 1
	responded := 0

	timeout := time.After(2 * time.Second)
	for confirmed < majority && responded < len(peers) {
		select {
		case ok := <-confirmCh:
			responded++
			if ok {
				confirmed++
			}
		case <-timeout:
			log.Printf("[%s] Submit timed out waiting for majority", n.id)
			return 0, false
		}
	}

	if confirmed < majority {
		return 0, false
	}

	n.mu.Lock()
	if n.currentTerm == term && index > n.commitIndex {
		n.commitIndex = index
		go n.applyCommitted()
	}
	n.mu.Unlock()

	log.Printf("[%s] committed command %q at index %d", n.id, command, index)
	return index, true
}