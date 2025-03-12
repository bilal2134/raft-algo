package kvstore

import (
	"log"
	"sync"

	"raft-kv/raft"
)

// KVStore represents an in-memory key-value store.
type KVStore struct {
	mu    sync.Mutex
	store map[string]string
}

// NewKVStore creates and initializes a new KVStore.
func NewKVStore() *KVStore {
	return &KVStore{
		store: make(map[string]string),
	}
}

// Apply executes a command (Put, Append, Get) on the KVStore.
func (kv *KVStore) Apply(entry raft.LogEntry) raft.ApplyResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var result raft.ApplyResult
	switch entry.Command {
	case raft.Put:
		kv.store[entry.Key] = entry.Value
		log.Printf("[KVStore] Put key=%s, value=%s", entry.Key, entry.Value)
	case raft.Append:
		kv.store[entry.Key] += entry.Value
		log.Printf("[KVStore] Append key=%s, value=%s, result=%s", entry.Key, entry.Value, kv.store[entry.Key])
	case raft.Get:
		result.Value = kv.store[entry.Key]
		log.Printf("[KVStore] Get key=%s, result=%s", entry.Key, result.Value)
	default:
		log.Printf("[KVStore] Unknown command: %+v", entry)
	}
	result.Key = entry.Key
	return result
}
