package lru

import (
	"container/list"
	"fmt"
	"nakevaleng/core/record"
)

type LRU struct {
	Capacity int
	Order    list.List
	Data     map[string]*list.Element
}

// New creates a pointer to an LRU object.
//  capacity    Maximum size of the LRU
//  returns     Pointer to an LRU object
// Throws if the passed capacity is not a positive number.
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

func ValidateParams(capacity int) error {
	if capacity <= 0 {
		err := fmt.Errorf("capacity must be a positive number, but %d was given", capacity)
		return err
	}
	return nil
}

// Get retrieves the record stored in the LRU based on the passed key.
//  key        String representation of the record's key
//  returns    Record with the matching key (empty record if no matching key is found) and a success flag
func (lru *LRU) Get(key string) (record.Record, bool) {
	el, exists := lru.Data[key]
	if !exists {
		return record.Record{}, false
	}

	lru.Order.MoveToFront(el)

	return el.Value.(record.Record), true
}

// Set inserts the passed record into the LRU.
// If the LRU is full, Set will remove the least recently used record so it can insert the new one.
//  rec    Record to be inserted into the LRU
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

// Removes the record with the passed key and returns it (as well as a success flag).
// If the record was successfully found and removed, it will return that record and true.
// Otherwise, it will return an empty record and false.
func (lru *LRU) Remove(key string) (record.Record, bool) {
	el, exists := lru.Data[key]
	if !exists {
		return record.Record{}, false
	}

	rec := el.Value.(record.Record)

	delete(lru.Data, string(rec.Key))
	lru.Order.Remove(el)

	return rec, true
}
