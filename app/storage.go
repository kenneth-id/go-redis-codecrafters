package main

import "fmt"

type Storage struct {
	data map[string]Value
}

type Value struct {
	val string
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

	return value.val, true
}

func (s *Storage) Set(key string, value string) {
	s.data[key] = Value{val: value}
}
