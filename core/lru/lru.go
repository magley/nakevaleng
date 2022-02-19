// Package lru implements a basic Least Recently Used (LRU) cache.
package lru

import (
	"container/list"
	"fmt"
	"nakevaleng/core/record"
)

// LRU represents a Least Recently Used Cache implemented using a doubly-linked
// list for fast insertions and deletions, and a map for fast searching.
type LRU struct {
	Capacity int
	Order    list.List
	Data     map[string]*list.Element
}

// New returns a pointer to a new LRU object.
func New(capacity int) (*LRU, error) {
	err := ValidateParams(capacity)
	if err != nil {
		return nil, err
	}

	return &LRU{
		Capacity: capacity,
		Order:    list.List{},
		Data:     map[string]*list.Element{},
	}, nil
}

// ValidateParams is a helper function that returns an error representing  the validity of params
// passed to LRU's New.
func ValidateParams(capacity int) error {
	if capacity <= 0 {
		err := fmt.Errorf("capacity must be a positive number, but %d was given", capacity)
		return err
	}
	return nil
}

// Get returns the record stored in the LRU based on the passed key, as well as whether
// or not the record is present.
func (lru *LRU) Get(key string) (rec record.Record, isPresent bool) {
	el, exists := lru.Data[key]
	if !exists {
		return record.Record{}, false
	}

	lru.Order.MoveToFront(el)

	return el.Value.(record.Record), true
}

// Set inserts the passed record into the LRU. If the LRU is full, Set will remove the least
// recently used record so it can insert the new one.
func (lru *LRU) Set(rec record.Record) {
	key := string(rec.Key)

	if el, exists := lru.Data[key]; exists {
		el.Value = rec
		lru.Order.MoveToFront(el)
		return
	}

	if lru.Order.Len() == lru.Capacity {
		back := lru.Order.Back()
		lru.Order.Remove(back)
		backKey := string(back.Value.(record.Record).Key)
		delete(lru.Data, backKey)
	}

	newElement := lru.Order.PushFront(rec)
	lru.Data[key] = newElement
}

// Remove returns the record stored in the LRU based on the passed key, as well as whether
// or not the record was present. If the record with the passed key was found in the LRU,
// it will be removed.
func (lru *LRU) Remove(key string) (rec record.Record, wasPresent bool) {
	el, exists := lru.Data[key]
	if !exists {
		return record.Record{}, false
	}

	rec = el.Value.(record.Record)

	delete(lru.Data, string(rec.Key))
	lru.Order.Remove(el)

	return rec, true
}
