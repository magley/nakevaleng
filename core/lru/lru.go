package lru

import (
	"container/list"
	"fmt"
	"nakevaleng/core/record"
)

type Cache interface {
	Get(key string) (record.Record, bool)
	Set(rec record.Record)
}

type LRU struct {
	capacity int
	order    list.List
	data     map[string]*list.Element
}

// New creates an LRU of the given capacity.
//  capacity    Maximum size of the LRU
// Throws if the passed capacity is not a positive number.
func New(capacity int) LRU {
	if capacity <= 0 {
		fmt.Println("ERROR: Capacity must be a positive number, but ", capacity, " was given.")
		panic(nil)
	}

	return LRU{
		capacity: capacity,
		order:    list.List{},
		data:     map[string]*list.Element{},
	}
}

// Get retrieves the record stored in the LRU based on the passed key.
//  key        String representation of the record's key
//  returns    Record with the matching key
func (lru *LRU) Get(key string) (record.Record, bool) {
	el, exists := lru.data[key]
	if !exists {
		return record.Record{}, false
	}

	lru.order.MoveToFront(el)

	return el.Value.(record.Record), true
}

// Set inserts the passed record into the LRU.
// If the LRU is full, Set will remove the least recently used record so it can insert the new one.
//  rec    Record to be inserted into the LRU
func (lru *LRU) Set(rec record.Record) {
	key := string(rec.Key)

	if el, exists := lru.data[key]; exists {
		el.Value = rec
		lru.order.MoveToFront(el)
		return
	}

	if lru.order.Len() == lru.capacity {
		back := lru.order.Back()
		lru.order.Remove(back)
		backKey := string(back.Value.(record.Record).Key)
		delete(lru.data, backKey)
	}

	newElement := lru.order.PushFront(rec)
	lru.data[key] = newElement
}
