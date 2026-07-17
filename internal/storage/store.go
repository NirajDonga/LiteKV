package storage

import (
	"hash/fnv"
	"sync"
	"time"
)

const shardCount = 32
const shardMaxSize = 1000
const defaultTTLSeconds = 3600

type Node struct {
	key       string
	value     string
	expiresAt int64
}

type Shard struct {
	data map[string]*Node
	mu   sync.RWMutex
}

type Store struct {
	shards []*Shard
}

func NewStore() *Store {
	store := &Store{
		shards: make([]*Shard, shardCount),
	}

	for i := 0; i < shardCount; i++ {
		store.shards[i] = &Shard{
			data: make(map[string]*Node),
		}
	}
	return store
}

func (s *Store) getShard(key string) *Shard {
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	shardIndex := hasher.Sum32() % uint32(shardCount)
	return s.shards[shardIndex]
}

func (s *Store) Get(key string) (string, bool) {
	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	node, ok := shard.data[key]
	if ok {
		if time.Now().Unix() > node.expiresAt {
			delete(shard.data, key)
			return "", false
		}
		return node.value, true
	}
	return "", false
}

func (s *Store) Set(key string, value string) {

	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	node, ok := shard.data[key]
	if ok {
		node.value = value
		return
	}

	node = &Node{
		key:       key,
		value:     value,
		expiresAt: time.Now().Unix() + defaultTTLSeconds,
	}

	shard.data[key] = node
}

func (s *Store) Delete(key string) bool {

	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	delete(shard.data, key)
	return true
}
