package main

import (
	"fmt"
	"time"
)

type Storage struct {
	data map[string]Value
}

type Value struct {
	val        string
	expiryTime time.Time
}

func (v Value) IsExpired() bool {
	if v.expiryTime.IsZero() {
		return false
	}
	return v.expiryTime.Before(time.Now())
}

func NewStorage() *Storage {
	return &Storage{
		data: make(map[string]Value),
	}
}

func (s *Storage) Get(key string) (string, bool) {
	value, ok := s.data[key]
	if !ok {
		fmt.Printf("Error getting value from storage: %s not found.\n", key)
		return "", false
	}
	if value.IsExpired() {
		fmt.Printf("Error getting value from storage: %s, it expired.\n", key)
		return "", false
	}

	return value.val, true
}

func (s *Storage) Set(key string, value string, expiryDuration time.Duration) {
	if expiryDuration != 0 {
		fmt.Printf("Setting key %s with value %s with expiry %s\n", key, value, expiryDuration.String())
		s.data[key] = Value{val: value, expiryTime: time.Now().Add(expiryDuration)}
	} else {
		s.data[key] = Value{val: value}
	}

}
