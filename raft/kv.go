package raft

import (
	"fmt"
	"strings"
	"sync"
)

// KVStore is a simple key-value store driven by a Raft log.
// Every write goes through Raft -- the KVStore only applies
// entries that have been committed by a majority of nodes.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
	node *Node
}

func NewKVStore(node *Node) *KVStore {
	kv := &KVStore{
		data: make(map[string]string),
		node: node,
	}
	// start applying committed entries from the node's ApplyCh
	go kv.applyLoop()
	return kv
}

// applyLoop reads committed log entries from the node's ApplyCh
// and applies them to the local state machine.
func (kv *KVStore) applyLoop() {
	for entry := range kv.node.ApplyCh {
		kv.apply(entry.Command)
	}
}

// apply parses and executes a single command string.
// Command format: "SET key value" or "DEL key"
func (kv *KVStore) apply(command string) {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch strings.ToUpper(parts[0]) {
	case "SET":
		if len(parts) < 3 {
			return
		}
		kv.data[parts[1]] = parts[2]
	case "DEL":
		if len(parts) < 2 {
			return
		}
		delete(kv.data, parts[1])
	}
}

// Set proposes a SET command to the Raft cluster.
// Blocks until the entry is committed by a majority.
// Returns an error if this node is not the leader.
func (kv *KVStore) Set(key, value string) error {
	command := fmt.Sprintf("SET %s %s", key, value)
	_, ok := kv.node.Submit(command)
	if !ok {
		return fmt.Errorf("not the leader")
	}
	return nil
}

// Delete proposes a DEL command to the Raft cluster.
func (kv *KVStore) Delete(key string) error {
	command := fmt.Sprintf("DEL %s", key)
	_, ok := kv.node.Submit(command)
	if !ok {
		return fmt.Errorf("not the leader")
	}
	return nil
}

// Get reads the current value for a key from the local state.
// NOTE: reads are local and may be slightly stale if this node
// is a follower -- in a production system you'd route reads
// through the leader or use a read index to guarantee freshness.
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.data[key]
	return v, ok
}

// Snapshot returns a copy of the entire key-value store.
func (kv *KVStore) Snapshot() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	copy := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		copy[k] = v
	}
	return copy
}
