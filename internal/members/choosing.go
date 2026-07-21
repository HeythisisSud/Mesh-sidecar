package members

import (
	"encoding/json"
	"net"
	"sync"
	"time"

	"github.com/heythisissud/mesh-sidecar/internal/models"
)
type Node struct {
	ID       string
	BindAddr string
	Members  map[string]*MemberState // your view of the cluster
	mu       sync.RWMutex
	conn     *net.UDPConn
}

type MemberState struct {
	Addr        *net.UDPAddr
	Status      string // "alive", "suspect", "dead"
	Incarnation int    // version number, bumps on state change
	LastSeen    time.Time
}


type Message struct {
	node    *Node
	messageType string
	counter int
}

func (n *Node) sendPing (message Message){
	
	mess, err:=json.Marshal(message)
	if err!=nil{
		return 
	}
	for _, value := range n.Members {

		go func() {
			
			_ , err:=n.conn.WriteToUDP(mess, value.Addr )
			if err!=nil{
			return;
		}
		}()



}


}


func (n *Node) recievePing(){
	buf := make([]byte, 1500)

	for{
		_,senderAddr, err:=n.conn.ReadFromUDP(buf);
		if err!=nil{
			return
		}
	}
}