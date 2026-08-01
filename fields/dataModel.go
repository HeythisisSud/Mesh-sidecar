package fields

import (
	"net"
	"sync"
	"time"
)

type Node struct {
	ID       string
	BindAddr *net.UDPAddr
	Members  map[string]*MemberState
	mu       sync.RWMutex
	conn     *net.UDPConn

	pendingPings map[uint64]chan struct{}
	pendingMu    sync.Mutex
	counter      uint64

	gossipQueue    []string // shuffled member IDs, walked in order
	queuePos       int
	relayRequests  map[uint64]*net.UDPAddr // seq -> original requester
	relayMu        sync.Mutex
	pendingUpdates []Update
	updatesMu      sync.Mutex
}

type Update struct {
	MemberID    string
	Status      string
	Incarnation int
}

type MemberState struct {
	ID          string
	Addr        *net.UDPAddr
	Status      string // "alive", "suspect", "dead"
	Incarnation int    // version number, bumps on state change
	LastSeen    time.Time
}

type Message struct {
	MessageType    string // "PING", "ACK", etc.
	From           string // sender's own address, as a string
	Counter        uint64 // matches a PING to its ACK
	IndirectTarget string
	Updates        []Update
	Members        []MemberInfo
	JoinerID       string
}

type MemberInfo struct {
	ID          string
	Addr        string
	Status      string
	Incarnation int
}