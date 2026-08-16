package raft

import (
	"fmt"
	"strings"
	"sync"
)

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

func (kv *KVStore) applyLoop() {
	for entry := range kv.node.ApplyCh {
		kv.apply(entry.Command)
	}
}

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

func (kv *KVStore) Set(key, value string) error {
	command := fmt.Sprintf("SET %s %s", key, value)
	_, ok := kv.node.Submit(command)
	if !ok {
		return fmt.Errorf("not the leader")
	}
	return nil
}

func (kv *KVStore) Delete(key string) error {
	command := fmt.Sprintf("DEL %s", key)
	_, ok := kv.node.Submit(command)
	if !ok {
		return fmt.Errorf("not the leader")
	}
	return nil
}


func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.data[key]
	return v, ok
}

func (kv *KVStore) Snapshot() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	copy := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		copy[k] = v
	}
	return copy
}
