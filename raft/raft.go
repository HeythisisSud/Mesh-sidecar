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
	mu   sync.Mutex
	id   string
	peers []string

	currentTerm uint64
	votedFor    string
	log         []LogEntry

	commitIndex uint64
	lastApplied uint64

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	state           State
	electionTimeout time.Duration
	lastHeartbeat   time.Time

	ApplyCh  chan LogEntry
	done     chan struct{}
	stopOnce sync.Once
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

// Stop shuts down the node safely. Safe to call multiple times.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.done) })
}

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

	// Single-node cluster: win immediately with only the self-vote.
	if len(peers) == 0 {
		n.becomeLeader(term)
		return
	}

	votes := 1
	var voteMu sync.Mutex

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
	if n.state != Candidate || n.currentTerm != term {
		return
	}
	n.state = Leader
	log.Printf("[%s] became leader for term %d", n.id, term)
	for _, peer := range n.peers {
		n.nextIndex[peer] = uint64(len(n.log)) + 1
		n.matchIndex[peer] = 0
	}
}

// lastLogInfo MUST be called with n.mu held.
func (n *Node) lastLogInfo() (uint64, uint64) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return uint64(len(n.log)), last.Term
}

// stepDown MUST be called with n.mu held.
func (n *Node) stepDown(term uint64) {
	n.currentTerm = term
	n.state = Follower
	n.votedFor = ""
	n.resetElectionTimeout()
}

// Submit proposes a command. Returns (logIndex, true) on success.
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
	n.mu.Unlock()

	// Buffer == len(peers) so goroutines writing here never block even if
	// Submit returns early, preventing goroutine leaks.
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
			return 0, false
		}
	}
	if confirmed < majority {
		return 0, false
	}

	n.mu.Lock()
	if n.currentTerm == term && index > n.commitIndex {
		n.commitIndex = index
	}
	n.mu.Unlock()

	// Apply OUTSIDE the mutex — applyCommitted must not send to ApplyCh
	// while holding n.mu (would deadlock if channel is full).
	go n.applyCommitted()

	return index, true
}
