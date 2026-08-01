package members

import (
	"encoding/json"
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

	gossipQueue []string // shuffled member IDs, walked in order
	queuePos    int
	relayRequests map[uint64]*net.UDPAddr // seq -> original requester
	relayMu       sync.Mutex
	pendingUpdates []Update
	updatesMu      sync.Mutex
}

type Update struct {
	MemberID string
	Status string
	Incarnation int
}

type MemberState struct {
	ID   string
	Addr        *net.UDPAddr
	Status      string // "alive", "suspect", "dead"
	Incarnation int    // version number, bumps on state change
	LastSeen    time.Time
}


type Message struct {
	MessageType string // "PING", "ACK", etc.
	From        string // sender's own address, as a string
	Counter     uint64 // matches a PING to its ACK
	IndirectTarget string
	Updates []Update
	Members []MemberInfo
	JoinerID string
	
}


type MemberInfo struct {
	ID          string
	Addr        string
	Status      string
	Incarnation int
}


func NewNode(id string, bindAddr *net.UDPAddr, conn *net.UDPConn) *Node {
	return &Node{
		ID:           id,
		BindAddr:     bindAddr,
		Members:      make(map[string]*MemberState),
		conn:         conn,
		pendingPings: make(map[uint64]chan struct{}),
		relayRequests: make(map[uint64]*net.UDPAddr),
	}
}

func (n *Node) Start() {
	go n.receiveLoop()
	go n.gossipLoop()
}

// gossipLoop ticks once a second and pings ONE random member.
// This is the actual gossip round -- not a broadcast to everyone.

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


func (n *Node) drainUpdates() []Update {
	n.updatesMu.Lock()
	defer n.updatesMu.Unlock()

	
	out := n.pendingUpdates[:]
	
	return out
}

func (n *Node) pickNextMember() *MemberState {
	n.mu.Lock()
	defer n.mu.Unlock()

	// rebuild the queue if it's empty, stale, or we've reached the end
	if n.queuePos >= len(n.gossipQueue) {
		n.rebuildQueue()
		n.queuePos = 0
	}
	if len(n.gossipQueue) == 0 {
		return nil // still no one to ping
	}

	id := n.gossipQueue[n.queuePos]
	n.queuePos++
	return n.Members[id]
}

// rebuildQueue snapshots current members (excluding self) and shuffles them.
// Called with n.mu already held.
func (n *Node) rebuildQueue() {
	n.updatesMu.Lock()
	defer n.updatesMu.Unlock()
	ids := make([]string, 0, len(n.Members))
	for id := range n.Members {
		if id == n.ID {
			continue
		}
		ids = append(ids, id)
	}
	rand.Shuffle(len(ids), func(i, j int) {
		ids[i], ids[j] = ids[j], ids[i]
	})
	n.gossipQueue = ids
	clear(n.pendingUpdates)
}

// pingMember sends a PING to one specific member and waits (with a
// timeout) for its ACK. This is where indirect-ping / suspicion
// logic will eventually hook in on the timeout branch.

func (n *Node) buildUpdates (status string, memberId string, incarnation int){
	n.updatesMu.Lock()
	defer n.updatesMu.Unlock()
	n.pendingUpdates=append(n.pendingUpdates, Update{
		MemberID: memberId,
		Status: status,
		Incarnation: incarnation,
	})
}
func (n *Node) pingMember(target *MemberState) {
	seq := n.nextCounter()

	waitCh := make(chan struct{})
	n.pendingMu.Lock()
	n.pendingPings[seq] = waitCh
	n.pendingMu.Unlock()

	// always clean up the pending entry, whether we succeed or time out
	defer func() {
		n.pendingMu.Lock()
		delete(n.pendingPings, seq)
		n.pendingMu.Unlock()
	}()

	msg := Message{
		MessageType: "PING",
		From:        n.BindAddr.String(),
		Counter:     seq,
		Updates: n.drainUpdates(),
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
		// ACK arrived in time -- mark alive
		n.mu.Lock()
		target.Status = "Alive"
		target.LastSeen = time.Now()
		n.mu.Unlock()

	case <-time.After(500 * time.Millisecond):


		n.indirectPing(target, seq)

	// second wait -- same waitCh, same seq, fresh timeout
		select {
		case <-waitCh:
			n.mu.Lock()
			target.Status = "Alive"
			target.LastSeen = time.Now()
			
			n.mu.Unlock()

		case <-time.After(500 * time.Millisecond):
			n.mu.Lock()
			target.Status = "Suspect"
			n.buildUpdates(target.Status, target.ID, target.Incarnation)
	

			n.mu.Unlock()
			log.Printf("indirect ping also failed, marking %s suspect (seq %d)\n", target.Addr, seq)

			select {
			case<- waitCh:
				n.mu.Lock()
				target.Status = "Alive"
				target.LastSeen = time.Now()
				n.buildUpdates(target.Status, target.ID, target.Incarnation)
				n.mu.Unlock()

			case <-time.After(3*time.Second):
				n.mu.Lock()
				target.Status = "Confirm"
				n.buildUpdates(target.Status, target.ID, target.Incarnation)
				n.mu.Unlock()
				log.Printf("indirect ping also failed, marking %s suspect (seq %d)\n", target.Addr, seq)



			}
		}
}	}

func (n *Node) indirectPing(target *MemberState, seq uint64) {
	relays := n.pickRelays(target, 3)
	if len(relays) == 0 {
		return
	}

	for _, relay := range relays {
		msg := Message{
			MessageType: "PING-REQ",
			From:        n.BindAddr.String(),
			Counter:     seq,
			IndirectTarget: target.Addr.String(),
			Updates: n.drainUpdates(),
			 // new field: who the relay should ping
		}
		mess, err := json.Marshal(msg)
		if err != nil {
			continue
		}
		// fire-and-forget to each relay; we're already waiting on
		// the same waitCh/timeout that pingMember set up for seq
		go n.conn.WriteToUDP(mess, relay.Addr)
	}
}



// pickRelays grabs up to `count` random members, excluding
// ourselves and the target. Holds the lock only briefly.
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
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})
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
	buf := make([]byte, 1500)

	for {
		b, senderAddr, err := n.conn.ReadFromUDP(buf)
		if err != nil {
			log.Println("read failed:", err)
			continue 
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
	}
}


func (n *Node) mergeUpdates(updates []Update) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, u := range updates {
		if u.MemberID == n.ID && (u.Status == "Suspect" || u.Status == "Confirm") {
    		n.buildUpdates("Alive", n.ID, u.Incarnation+1)
		}
		existing, known := n.Members[u.MemberID]
		if !known {
			continue 
		}
		if u.Status=="Confirm" {
			delete(n.Members, u.MemberID)
			
			
		}

		if u.Status=="Suspect" && u.Incarnation>=existing.Incarnation{
			existing.Status = u.Status
			existing.Incarnation = u.Incarnation
			continue

		}
		
		if u.Incarnation < existing.Incarnation && u.Status!="Confirm" {
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

	// remember that this seq number is a relay job, and who to
	// forward the eventual ack back to
	n.relayMu.Lock()
	n.relayRequests[msg.Counter] = requesterAddr
	n.relayMu.Unlock()

		reply := Message{
		MessageType: "PING",
		From:        n.BindAddr.String(),
		Counter:     msg.Counter, // same seq -- this is what ties it all together
		Updates: n.drainUpdates(),
	}

	mess, err := json.Marshal(reply)
	if err != nil {
		log.Println("marshal ping failed:", err)
		return
	}

	// I am the relay -- I ping the TARGET, not the requester
	if _, err := n.conn.WriteToUDP(mess, targetAddr); err != nil {
		log.Println("send ping failed:", err)
		return
	}
}
func (n *Node) handlePing(msg Message, addr *net.UDPAddr) {
	reply := Message{
		MessageType: "ACK",
		From:        n.BindAddr.String(),
		Counter:     msg.Counter, // same seq number the ping carried
	}
	mess, err := json.Marshal(reply)
	if err != nil {
		log.Println("marshal ack failed:", err)
		return
	}

	// reply to whoever just sent us the packet -- NOT to any
	// address embedded in the message
	if _, err := n.conn.WriteToUDP(mess, addr); err != nil {
		log.Println("send ack failed:", err)
		return
	}
}

func (n *Node) handleAck(msg Message, addr *net.UDPAddr) {
	// case 1: this is a relay job -- forward the ack to whoever asked us
	n.relayMu.Lock()
	requesterAddr, isRelay := n.relayRequests[msg.Counter]
	if isRelay {
		delete(n.relayRequests, msg.Counter)
	}
	n.relayMu.Unlock()

	if isRelay {
		mess, err := json.Marshal(msg) // forward as-is, same seq
		if err != nil {
			return
		}
		n.conn.WriteToUDP(mess, requesterAddr)
		return
	}

	// case 2: this is our own ping -- existing logic
	n.pendingMu.Lock()
	waitCh, ok := n.pendingPings[msg.Counter]
	n.pendingMu.Unlock()
	if !ok {
		return
	}
	close(waitCh)
}


func (n *Node) Join (peerAddr *net.UDPAddr) error{
	reply:=Message{
		MessageType: "JOIN",
		From: n.BindAddr.String(),
		JoinerID: n.ID,	

	}

	mess, err := json.Marshal(reply)
	if err != nil {
		log.Println("marshal ack failed:", err)
		return err
	}

	
	
	if _, err := n.conn.WriteToUDP(mess, peerAddr); err != nil {
		log.Println("send ack failed:", err)
		return err
	}

	return nil




}



func (n *Node) handleJoin (msg Message){
	JoinerAddr, err:=net.ResolveUDPAddr("udp", msg.From)
	if err!=nil{
		log.Println("handle join failed")
	}
	

	update:= MemberState{
		ID: msg.JoinerID,
		Addr: JoinerAddr,
		Incarnation: 1,

	}
	n.Members[msg.From]= &update
	var member MemberInfo
	var members []MemberInfo
	for _, value:= range n.Members{
		member= MemberInfo{
			ID: value.ID,
			Status: value.Status,
			Incarnation: value.Incarnation,
			Addr: value.Addr.String(),
		}

		members = append(members, member)


	}

	reply:= &Message{
		MessageType: "JOIN-ACK",
		From: n.BindAddr.String(),
		Members: members,
	}
	value, err:=json.Marshal(reply)
	if err!=nil{
		log.Println(err)
	}

	if _,err:=n.conn.WriteToUDP(value, JoinerAddr); err!=nil{
		log.Println(err)
	}



}


func (n *Node) handleJoinAck (msg Message){
	for _, value := range msg.Members {
		addr, err:=net.ResolveUDPAddr("udp", value.Addr)
		if err!=nil{
			log.Println(err)
		}
    	n.Members[value.ID]=&MemberState{
			ID: value.ID,
			Addr: addr,
			Status: value.Status,
			Incarnation: value.Incarnation,
		}
}


}