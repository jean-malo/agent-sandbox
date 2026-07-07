// Copyright 2026 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queue

import (
	"sync"
)

// SandboxKey uniquely identifies a sandbox in the queue.
type SandboxKey struct {
	Namespace string
	Name      string
	NodeName  string
}

// SandboxQueue defines the interface for managing a thread-safe,
// highly concurrent queue of adoptable warm pool sandboxes.
type SandboxQueue interface {
	Add(warmPoolName string, item SandboxKey)
	Get(warmPoolName string) (SandboxKey, bool)
	GetWithStrategy(warmPoolName string, pick func([]SandboxKey) (SandboxKey, bool)) (SandboxKey, bool)
	RemoveQueue(warmPoolName string)
	RemoveItem(warmPoolName string, item SandboxKey)
}

// SimpleSandboxQueue implements SandboxQueue using simple synchronized slices.
type SimpleSandboxQueue struct {
	// queues is a thread-safe dictionary from warm pool name to a synchronizedQueue
	queues sync.Map
}

// NewSimpleSandboxQueue initializes a new SimpleSandboxQueue.
func NewSimpleSandboxQueue() *SimpleSandboxQueue {
	return &SimpleSandboxQueue{}
}

// Add pushes an item to the specific warm pool's queue.
func (s *SimpleSandboxQueue) Add(warmPoolName string, item SandboxKey) {
	q, _ := s.queues.LoadOrStore(warmPoolName, newSynchronizedQueue())
	q.(*synchronizedQueue).Push(item)
}

// Get pops an item from the specific warm pool's queue.
func (s *SimpleSandboxQueue) Get(warmPoolName string) (SandboxKey, bool) {
	q, ok := s.queues.Load(warmPoolName)
	if !ok {
		return SandboxKey{}, false
	}
	return q.(*synchronizedQueue).Pop()
}

// GetWithStrategy pops an item from the specific warm pool's queue using a custom strategy.
func (s *SimpleSandboxQueue) GetWithStrategy(warmPoolName string, pick func([]SandboxKey) (SandboxKey, bool)) (SandboxKey, bool) {
	q, ok := s.queues.Load(warmPoolName)
	if !ok {
		return SandboxKey{}, false
	}
	return q.(*synchronizedQueue).PopWithStrategy(pick)
}

// RemoveItem deletes a specific sandbox from a warm pool's queue.
func (s *SimpleSandboxQueue) RemoveItem(warmPoolName string, item SandboxKey) {
	if q, ok := s.queues.Load(warmPoolName); ok {
		sq := q.(*synchronizedQueue)
		sq.Remove(item)
	}
}

// Remove deletes the item to prevent ghost pods from staying adoptable.
func (q *synchronizedQueue) Remove(key SandboxKey) {
	q.mu.Lock()
	defer q.mu.Unlock()

	uniqueID := sandboxKeyID(key)
	itemIndex, exists := q.index[uniqueID]
	if !exists {
		return
	}

	delete(q.index, uniqueID)
	q.items[itemIndex] = SandboxKey{}
	if itemIndex >= q.head {
		q.tombstones++
	}
	q.compactLocked()
}

// TODO(vicentefb): Implement queue cleanup mechanism.
// We should remove the queue from the sync.Map when the corresponding
// SandboxWarmPool is deleted to prevent memory leaks.
type synchronizedQueue struct {
	mu         sync.Mutex
	items      []SandboxKey
	head       int
	tombstones int
	index      map[string]int // Used for O(1) deduplication and removal by namespace/name.
}

func newSynchronizedQueue() *synchronizedQueue {
	return &synchronizedQueue{
		items: make([]SandboxKey, 0),
		index: make(map[string]int),
	}
}

func sandboxKeyID(key SandboxKey) string {
	return key.Namespace + "/" + key.Name
}

func emptySandboxKey(key SandboxKey) bool {
	return key.Namespace == "" && key.Name == ""
}

func (q *synchronizedQueue) compactLocked() {
	removed := q.head + q.tombstones
	if removed == 0 {
		return
	}
	if removed < 1024 && removed*2 < len(q.items) {
		return
	}

	compacted := make([]SandboxKey, 0, len(q.items)-q.head)
	q.index = make(map[string]int, len(q.index))
	for _, key := range q.items[q.head:] {
		if emptySandboxKey(key) {
			continue
		}
		q.index[sandboxKeyID(key)] = len(compacted)
		compacted = append(compacted, key)
	}
	q.items = compacted
	q.head = 0
	q.tombstones = 0
}

// Push adds an item to the queue if it isn't already present.
func (q *synchronizedQueue) Push(key SandboxKey) {
	q.mu.Lock()
	defer q.mu.Unlock()
	uniqueID := sandboxKeyID(key)
	if itemIndex, exists := q.index[uniqueID]; exists {
		q.items[itemIndex] = key
		return
	}
	q.index[uniqueID] = len(q.items)
	q.items = append(q.items, key)
}

// Pop removes and returns the first item from the queue.
func (q *synchronizedQueue) Pop() (SandboxKey, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.head < len(q.items) {
		item := q.items[q.head]
		q.items[q.head] = SandboxKey{}
		q.head++
		if emptySandboxKey(item) {
			continue
		}

		delete(q.index, sandboxKeyID(item))
		q.compactLocked()
		return item, true
	}

	q.compactLocked()
	return SandboxKey{}, false
}

// PopWithStrategy applies the strategy function to pick an item from the queue,
// removes it thread-safely, and returns it.
func (q *synchronizedQueue) PopWithStrategy(pick func([]SandboxKey) (SandboxKey, bool)) (SandboxKey, bool) {
	for {
		q.mu.Lock()
		if len(q.index) == 0 {
			q.mu.Unlock()
			return SandboxKey{}, false
		}

		// Snapshot the queue items
		snapshot := make([]SandboxKey, 0, len(q.index))
		for _, key := range q.items[q.head:] {
			if !emptySandboxKey(key) {
				snapshot = append(snapshot, key)
			}
		}
		q.mu.Unlock()

		key, ok := pick(snapshot)
		if !ok {
			return SandboxKey{}, false
		}

		q.mu.Lock()
		uniqueID := sandboxKeyID(key)
		// Verify the key is still present in the queue
		itemIndex, exists := q.index[uniqueID]
		if !exists {
			// The picked key was concurrently popped by another goroutine.
			// Unlock and retry snapshot and pick.
			q.mu.Unlock()
			continue
		}

		q.items[itemIndex] = SandboxKey{}
		delete(q.index, uniqueID)
		if itemIndex >= q.head {
			q.tombstones++
		}
		q.compactLocked()
		q.mu.Unlock()

		return key, true
	}
}

// RemoveQueue completely deletes a warm pool's queue from the sync.Map
// to prevent memory leaks when SandboxTemplates or WarmPools are deleted.
func (s *SimpleSandboxQueue) RemoveQueue(warmPoolName string) {
	s.queues.Delete(warmPoolName)
}
