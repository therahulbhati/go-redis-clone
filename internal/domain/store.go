package domain

import "time"

// Store defines the interface for the key-value store.
type Store interface {
	Set(key, value string, expiration time.Duration)
	Get(key string) (string, bool)
}
