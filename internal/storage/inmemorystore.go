package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/therahulbhati/go-redis-clone/internal/domain"
)

type inMemoryStore struct {
	data map[string]Entry
	mu   sync.Mutex
}

type Entry struct {
	Value      string
	Expiration *time.Time
}

// NewInMemoryStore creates a new in-memory store.
func NewInMemoryStore() domain.Store {
	return &inMemoryStore{
		data: make(map[string]Entry),
	}
}

func (s *inMemoryStore) Set(key, value string, px time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var expirationPtr *time.Time
	if px > 0 {
		expiration := time.Now().Add(px)
		expirationPtr = &expiration
	}
	fmt.Println("expirationPtr: ", expirationPtr)
	s.data[key] = Entry{Value: value, Expiration: expirationPtr}

}

func (s *inMemoryStore) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]
	if !exists {
		return "", false
	}

	if entry.Expiration != nil && time.Now().After(*entry.Expiration) {
		delete(s.data, key)
		return "", false
	}

	return entry.Value, true
}
