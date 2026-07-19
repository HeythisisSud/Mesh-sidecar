package models

import (
	"net"
	"sync"
	"time"
)

type Node struct {
	ID       string
	BindAddr string
	Members  map[string]*MemberState // your view of the cluster
	mu       sync.RWMutex
	conn     *net.UDPConn
}

type MemberState struct {
	Addr        string
	Status      string // "alive", "suspect", "dead"
	Incarnation int    // version number, bumps on state change
	LastSeen    time.Time
}