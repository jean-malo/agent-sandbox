// Copyright 2026 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package queue

import (
	"strconv"
	"testing"
)

func TestSimpleSandboxQueue_BasicOperations(t *testing.T) {
	q := NewSimpleSandboxQueue()
	hash := "template-hash-1"

	key1 := SandboxKey{Namespace: "default", Name: "sb-1"}
	key2 := SandboxKey{Namespace: "default", Name: "sb-2"}

	// Test Add
	q.Add(hash, key1)
	q.Add(hash, key2)

	// Test Get (Should be FIFO)
	got1, ok1 := q.Get(hash)
	if !ok1 || got1 != key1 {
		t.Errorf("Expected %v, got %v (ok: %v)", key1, got1, ok1)
	}

	got2, ok2 := q.Get(hash)
	if !ok2 || got2 != key2 {
		t.Errorf("Expected %v, got %v (ok: %v)", key2, got2, ok2)
	}

	// Queue should now be empty
	_, ok3 := q.Get(hash)
	if ok3 {
		t.Errorf("Expected queue to be empty, but got an item")
	}
}

func TestSimpleSandboxQueue_RemoveItem_GhostPodFix(t *testing.T) {
	q := NewSimpleSandboxQueue()
	hash := "template-hash-1"

	key1 := SandboxKey{Namespace: "default", Name: "sb-1"}
	key2 := SandboxKey{Namespace: "default", Name: "sb-2"}
	key3 := SandboxKey{Namespace: "default", Name: "sb-3"}

	q.Add(hash, key1)
	q.Add(hash, key2)
	q.Add(hash, key3)

	// Simulate the Kubelet deleting the middle pod (Ghost Pod scenario)
	q.RemoveItem(hash, key2)

	// Ensure RemoveItem clears the indexed slot so stale keys cannot be popped.
	rawQueue, ok := q.queues.Load(hash)
	if !ok {
		t.Fatalf("Expected queue for %q to exist", hash)
	}
	sq := rawQueue.(*synchronizedQueue)
	if _, exists := sq.index[sandboxKeyID(key2)]; exists {
		t.Fatalf("Expected removed key %v to be absent from queue index", key2)
	}
	for _, item := range sq.items {
		if item == key2 {
			t.Fatalf("Expected removed key %v to be cleared from queue items", key2)
		}
	}

	// First pop should still be key1
	got1, _ := q.Get(hash)
	if got1 != key1 {
		t.Errorf("Expected %v, got %v", key1, got1)
	}

	// Second pop should be key3! (key2 was successfully removed)
	got3, _ := q.Get(hash)
	if got3 != key3 {
		t.Errorf("Expected %v to skip deleted item and return %v, but got %v", hash, key3, got3)
	}

	// Queue should now be empty
	_, hasItem := q.Get(hash)
	if hasItem {
		t.Errorf("Expected queue to be empty after Ghost Pod removal")
	}
}

func TestSynchronizedQueue_Deduplication(t *testing.T) {
	q := newSynchronizedQueue()
	key := SandboxKey{Namespace: "default", Name: "duplicate-sb"}

	// Push the exact same pod 3 times
	q.Push(key)
	q.Push(key)
	q.Push(key)

	// Verify it only stored it once.
	if len(q.items) != 1 {
		t.Errorf("Expected item length 1 due to O(1) deduplication, got %d", len(q.items))
	}

	// Verify the index also only has 1 item.
	if len(q.index) != 1 {
		t.Errorf("Expected index length 1, got %d", len(q.index))
	}
}

func TestSynchronizedQueue_CompactsMiddleRemovals(t *testing.T) {
	q := newSynchronizedQueue()
	keys := make([]SandboxKey, 2048)
	for i := range keys {
		keys[i] = SandboxKey{Namespace: "default", Name: "sb-" + strconv.Itoa(i)}
		q.Push(keys[i])
	}

	for i := 0; i < 1024; i++ {
		q.Remove(keys[i])
	}

	if q.head != 0 {
		t.Fatalf("Expected compacted queue head to reset to 0, got %d", q.head)
	}
	if q.tombstones != 0 {
		t.Fatalf("Expected compacted queue tombstones to reset to 0, got %d", q.tombstones)
	}
	if len(q.items) != len(q.index) {
		t.Fatalf("Expected compacted item count to match index count, got %d items and %d indexed keys", len(q.items), len(q.index))
	}
}

func TestSimpleSandboxQueue_RemoveQueue_MemoryLeakFix(t *testing.T) {
	q := NewSimpleSandboxQueue()
	hash := "template-hash-to-delete"
	key1 := SandboxKey{Namespace: "default", Name: "sb-1"}

	q.Add(hash, key1)

	// Simulate SandboxTemplate deletion
	q.RemoveQueue(hash)

	// Verify the entire queue was wiped from the sync.Map
	_, ok := q.Get(hash)
	if ok {
		t.Errorf("Expected queue to be completely removed, but it still existed")
	}
}

func TestSimpleSandboxQueue_GetWithStrategy(t *testing.T) {
	q := NewSimpleSandboxQueue()
	hash := "template-hash-1"

	key1 := SandboxKey{Namespace: "default", Name: "sb-1"}
	key2 := SandboxKey{Namespace: "default", Name: "sb-2"}
	key3 := SandboxKey{Namespace: "default", Name: "sb-3"}

	q.Add(hash, key1)
	q.Add(hash, key2)
	q.Add(hash, key3)

	// Custom strategy to pick key2 specifically
	pickKey2 := func(items []SandboxKey) (SandboxKey, bool) {
		for _, item := range items {
			if item.Name == "sb-2" {
				return item, true
			}
		}
		return SandboxKey{}, false
	}

	// Pop with strategy
	got, ok := q.GetWithStrategy(hash, pickKey2)
	if !ok || got != key2 {
		t.Errorf("Expected to pick %v, got %v (ok: %v)", key2, got, ok)
	}

	// First standard pop should be key1 (since key2 was removed)
	got1, _ := q.Get(hash)
	if got1 != key1 {
		t.Errorf("Expected first remaining item to be %v, got %v", key1, got1)
	}

	// Second standard pop should be key3
	got3, _ := q.Get(hash)
	if got3 != key3 {
		t.Errorf("Expected second remaining item to be %v, got %v", key3, got3)
	}

	// Queue should now be empty
	_, ok3 := q.Get(hash)
	if ok3 {
		t.Errorf("Expected queue to be empty, but got an item")
	}
}
