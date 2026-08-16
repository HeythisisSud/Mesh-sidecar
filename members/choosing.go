package members

import (
	"encoding/json"
	"errors"
	"log"
	"math/rand"
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

	pendingJoin   map[uint64]chan struct{}
	pendingJoinMu sync.Mutex

	gossipQueue []string
	queuePos    int

	relayRequests map[uint64]*net.UDPAddr
	relayMu       sync.Mutex

	pendingUpdates []Update
	updatesMu      sync.Mutex

	Incarnation int
}

type Update struct {
	MemberID    string
	Status      string
	Incarnation int
}

type MemberState struct {
	ID          string
	Addr        *net.UDPAddr
	Status      string
	Incarnation int
	LastSeen    time.Time
}

const (
	StatusAlive   = "Alive"
	StatusSuspect = "Suspect"
	StatusConfirm = "Confirm"
)

type Message struct {
	MessageType    string
	From           string
	Counter        uint64
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

func NewNode(id string, bindAddr *net.UDPAddr, conn *net.UDPConn) *Node {
	return &Node{
		ID:            id,
		BindAddr:      bindAddr,
		Members:       make(map[string]*MemberState),
		conn:          conn,
		pendingPings:  make(map[uint64]chan struct{}),
		relayRequests: make(map[uint64]*net.UDPAddr),
		pendingJoin:   make(map[uint64]chan struct{}),
		Incarnation:   0,
	}
}

func (n *Node) Start() {
	go n.receiveLoop()
	go n.gossipLoop()
}

// SnapShot returns a point-in-time copy of membership. Uses RLock because it is read-only.
func (n *Node) SnapShot() []MemberState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	list := make([]MemberState, 0, len(n.Members))
	for _, v := range n.Members {
		list = append(list, *v)
	}
	return list
}

func (n *Node) gossipLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		target := n.pickNextMember()
		if target == nil {
			continue
		}
		n.pingMember(target)
	}
}

// drainUpdates removes and returns a copy of all pending updates.
// The returned slice is independent of pendingUpdates.
func (n *Node) drainUpdates() []Update {
	n.updatesMu.Lock()
	defer n.updatesMu.Unlock()
	if len(n.pendingUpdates) == 0 {
		return nil
	}
	out := make([]Update, len(n.pendingUpdates))
	copy(out, n.pendingUpdates)
	n.pendingUpdates = n.pendingUpdates[:0]
	return out
}

func (n *Node) pickNextMember() *MemberState {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.queuePos >= len(n.gossipQueue) {
		n.rebuildQueue()
		n.queuePos = 0
	}
	if len(n.gossipQueue) == 0 {
		return nil
	}
	id := n.gossipQueue[n.queuePos]
	n.queuePos++
	m, ok := n.Members[id]
	if !ok {
		return nil
	}
	return m
}

// rebuildQueue MUST be called with n.mu held.
// Lock ordering: n.mu -> updatesMu (consistent everywhere).
func (n *Node) rebuildQueue() {
	ids := make([]string, 0, len(n.Members))
	for id := range n.Members {
		if id == n.ID {
			continue
		}
		ids = append(ids, id)
	}
	rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	n.gossipQueue = ids

	n.updatesMu.Lock()
	n.pendingUpdates = n.pendingUpdates[:0]
	n.updatesMu.Unlock()
}

// buildUpdates MUST NOT be called while updatesMu is already held.
func (n *Node) buildUpdates(status, memberID string, incarnation int) {
	n.updatesMu.Lock()
	defer n.updatesMu.Unlock()
	n.pendingUpdates = append(n.pendingUpdates, Update{
		MemberID:    memberID,
		Status:      status,
		Incarnation: incarnation,
	})
}

func (n *Node) pingMember(target *MemberState) {
	seq := n.nextCounter()

	waitCh := make(chan struct{})
	n.pendingMu.Lock()
	n.pendingPings[seq] = waitCh
	n.pendingMu.Unlock()

	defer func() {
		n.pendingMu.Lock()
		delete(n.pendingPings, seq)
		n.pendingMu.Unlock()
	}()

	msg := Message{
		MessageType: "PING",
		From:        n.BindAddr.String(),
		Counter:     seq,
		Updates:     n.drainUpdates(),
	}
	mess, err := json.Marshal(msg)
	if err != nil {
		log.Println("marshal ping failed:", err)
		return
	}
	if _, err := n.conn.WriteToUDP(mess, target.Addr); err != nil {
		log.Println("send ping failed:", err)
		return
	}

	select {
	case <-waitCh:
		n.mu.Lock()
		if m, ok := n.Members[target.ID]; ok {
			m.Status = StatusAlive
			m.LastSeen = time.Now()
		}
		n.mu.Unlock()

	case <-time.After(500 * time.Millisecond):
		n.indirectPing(target, seq)

		select {
		case <-waitCh:
			n.mu.Lock()
			if m, ok := n.Members[target.ID]; ok {
				m.Status = StatusAlive
				m.LastSeen = time.Now()
			}
			n.mu.Unlock()

		case <-time.After(500 * time.Millisecond):
			n.mu.Lock()
			if m, ok := n.Members[target.ID]; ok {
				m.Status = StatusSuspect
				n.buildUpdates(StatusSuspect, m.ID, m.Incarnation)
			}
			n.mu.Unlock()

			select {
			case <-waitCh:
				n.mu.Lock()
				if m, ok := n.Members[target.ID]; ok {
					m.Status = StatusAlive
					m.LastSeen = time.Now()
					n.buildUpdates(StatusAlive, m.ID, m.Incarnation)
				}
				n.mu.Unlock()

			case <-time.After(3 * time.Second):
				n.mu.Lock()
				if m, ok := n.Members[target.ID]; ok {
					n.buildUpdates(StatusConfirm, m.ID, m.Incarnation)
					delete(n.Members, m.ID)
				}
				n.mu.Unlock()
				log.Printf("node %s confirmed dead (seq %d)\n", target.Addr, seq)
			}
		}
	}
}

func (n *Node) indirectPing(target *MemberState, seq uint64) {
	relays := n.pickRelays(target, 3)
	if len(relays) == 0 {
		return
	}
	updates := n.drainUpdates()
	for _, relay := range relays {
		msg := Message{
			MessageType:    "PING-REQ",
			From:           n.BindAddr.String(),
			Counter:        seq,
			IndirectTarget: target.Addr.String(),
			Updates:        updates,
		}
		mess, err := json.Marshal(msg)
		if err != nil {
			continue
		}
		go n.conn.WriteToUDP(mess, relay.Addr)
	}
}

func (n *Node) pickRelays(target *MemberState, count int) []*MemberState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	candidates := make([]*MemberState, 0, len(n.Members))
	for id, m := range n.Members {
		if id == n.ID || m.Addr.String() == target.Addr.String() {
			continue
		}
		candidates = append(candidates, m)
	}
	rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })
	if len(candidates) > count {
		candidates = candidates[:count]
	}
	return candidates
}

func (n *Node) nextCounter() uint64 {
	n.pendingMu.Lock()
	defer n.pendingMu.Unlock()
	n.counter++
	return n.counter
}

func (n *Node) receiveLoop() {
	buf := make([]byte, 65535)
	for {
		b, senderAddr, err := n.conn.ReadFromUDP(buf)
		if err != nil {
			log.Println("read failed:", err)
			return
		}
		n.handleMessage(buf[:b], senderAddr)
	}
}

func (n *Node) handleMessage(buf []byte, addr *net.UDPAddr) {
	var msg Message
	if err := json.Unmarshal(buf, &msg); err != nil {
		log.Println("unmarshal failed:", err)
		return
	}
	n.mergeUpdates(msg.Updates)
	switch msg.MessageType {
	case "PING":
		n.handlePing(msg, addr)
	case "ACK":
		n.handleAck(msg, addr)
	case "PING-REQ":
		n.handlePingReq(msg, addr)
	case "JOIN":
		n.handleJoin(msg)
	case "JOIN-ACK":
		n.handleJoinAck(msg)
	}
}

// mergeUpdates applies piggybacked updates. Lock ordering: n.mu -> updatesMu.
func (n *Node) mergeUpdates(updates []Update) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, u := range updates {
		// Self-refutation: bump incarnation and re-assert Alive.
		if u.MemberID == n.ID && (u.Status == StatusSuspect || u.Status == StatusConfirm) {
			n.Incarnation = u.Incarnation + 1
			n.buildUpdates(StatusAlive, n.ID, n.Incarnation)
			continue
		}

		existing, known := n.Members[u.MemberID]
		if !known {
			continue
		}

		// Confirm: delete and continue — never touch the deleted struct.
		if u.Status == StatusConfirm {
			delete(n.Members, u.MemberID)
			continue
		}

		if u.Status == StatusSuspect && u.Incarnation >= existing.Incarnation {
			existing.Status = u.Status
			existing.Incarnation = u.Incarnation
			continue
		}

		if u.Incarnation < existing.Incarnation {
			continue
		}
		if u.Incarnation == existing.Incarnation && u.Status == existing.Status {
			continue
		}

		existing.Status = u.Status
		existing.Incarnation = u.Incarnation
	}
}

func (n *Node) handlePingReq(msg Message, requesterAddr *net.UDPAddr) {
	targetAddr, err := net.ResolveUDPAddr("udp", msg.IndirectTarget)
	if err != nil {
		log.Println("bad target address:", err)
		return
	}
	n.relayMu.Lock()
	n.relayRequests[msg.Counter] = requesterAddr
	n.relayMu.Unlock()

	reply := Message{
		MessageType: "PING",
		From:        n.BindAddr.String(),
		Counter:     msg.Counter,
		Updates:     n.drainUpdates(),
	}
	mess, err := json.Marshal(reply)
	if err != nil {
		return
	}
	n.conn.WriteToUDP(mess, targetAddr)
}

func (n *Node) handlePing(msg Message, addr *net.UDPAddr) {
	reply := Message{
		MessageType: "ACK",
		From:        n.BindAddr.String(),
		Counter:     msg.Counter,
	}
	mess, err := json.Marshal(reply)
	if err != nil {
		return
	}
	n.conn.WriteToUDP(mess, addr)
}

func (n *Node) handleAck(msg Message, addr *net.UDPAddr) {
	n.relayMu.Lock()
	requesterAddr, isRelay := n.relayRequests[msg.Counter]
	if isRelay {
		delete(n.relayRequests, msg.Counter)
	}
	n.relayMu.Unlock()

	if isRelay {
		mess, _ := json.Marshal(msg)
		n.conn.WriteToUDP(mess, requesterAddr)
		return
	}

	n.pendingMu.Lock()
	waitCh, ok := n.pendingPings[msg.Counter]
	n.pendingMu.Unlock()
	if !ok {
		return
	}
	// Guard against double-close (relay + direct ACK for same seq).
	select {
	case <-waitCh:
	default:
		close(waitCh)
	}
}

func (n *Node) Join(peerAddr *net.UDPAddr) error {
	seq := n.nextCounter()

	waitCh := make(chan struct{})
	n.pendingJoinMu.Lock()
	n.pendingJoin[seq] = waitCh
	n.pendingJoinMu.Unlock()

	defer func() {
		n.pendingJoinMu.Lock()
		delete(n.pendingJoin, seq)
		n.pendingJoinMu.Unlock()
	}()

	msg := Message{
		MessageType: "JOIN",
		From:        n.BindAddr.String(),
		JoinerID:    n.ID,
		Counter:     seq,
	}
	mess, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if _, err := n.conn.WriteToUDP(mess, peerAddr); err != nil {
		return err
	}
	select {
	case <-waitCh:
		return nil
	case <-time.After(3 * time.Second):
		return errors.New("join timeout")
	}
}

// handleJoin releases n.mu before the blocking WriteToUDP call to avoid
// holding a lock across I/O. Lock ordering for updatesMu: only updatesMu
// is acquired at the end, n.mu is already released.
func (n *Node) handleJoin(msg Message) {
	joinerAddr, err := net.ResolveUDPAddr("udp", msg.From)
	if err != nil {
		log.Println("handleJoin: bad joiner address:", err)
		return
	}

	n.mu.Lock()
	n.Members[msg.JoinerID] = &MemberState{
		ID:          msg.JoinerID,
		Addr:        joinerAddr,
		Incarnation: 1,
		Status:      StatusAlive,
	}
	memberList := make([]MemberInfo, 0, len(n.Members)+1)
	for _, v := range n.Members {
		memberList = append(memberList, MemberInfo{
			ID:          v.ID,
			Addr:        v.Addr.String(),
			Status:      v.Status,
			Incarnation: v.Incarnation,
		})
	}
	memberList = append(memberList, MemberInfo{
		ID:          n.ID,
		Addr:        n.BindAddr.String(),
		Status:      StatusAlive,
		Incarnation: n.Incarnation,
	})
	n.mu.Unlock() // release before blocking I/O

	reply := &Message{
		MessageType: "JOIN-ACK",
		From:        n.BindAddr.String(),
		Members:     memberList,
		JoinerID:    n.ID,
		Counter:     msg.Counter,
	}
	value, err := json.Marshal(reply)
	if err != nil {
		log.Println("handleJoin: marshal failed:", err)
		return
	}
	if _, err := n.conn.WriteToUDP(value, joinerAddr); err != nil {
		log.Println("handleJoin: write failed:", err)
	}

	n.updatesMu.Lock()
	n.pendingUpdates = append(n.pendingUpdates, Update{
		MemberID:    msg.JoinerID,
		Status:      StatusAlive,
		Incarnation: 1,
	})
	n.updatesMu.Unlock()
}

// handleJoinAck: lock ordering n.mu first, pendingJoinMu second.
func (n *Node) handleJoinAck(msg Message) {
	n.mu.Lock()
	for _, v := range msg.Members {
		if v.ID == n.ID {
			continue
		}
		addr, err := net.ResolveUDPAddr("udp", v.Addr)
		if err != nil {
			log.Println("handleJoinAck: bad addr:", err)
			continue
		}
		n.Members[v.ID] = &MemberState{
			ID:          v.ID,
			Addr:        addr,
			Status:      v.Status,
			Incarnation: v.Incarnation,
		}
	}
	n.mu.Unlock()

	n.pendingJoinMu.Lock()
	waitCh, ok := n.pendingJoin[msg.Counter]
	if ok {
		delete(n.pendingJoin, msg.Counter)
		close(waitCh)
	}
	n.pendingJoinMu.Unlock()
}
