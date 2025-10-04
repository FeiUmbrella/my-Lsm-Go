package skiplist

import (
	"myLsm-Go/pkg/iterator"
	"sync"
)

// SkipListIterator 实现跳表的迭代器接口
type SkipListIterator struct {
	current *Node
	sl      *SkipList
	txnID   uint64
	closed  bool
	mu      sync.RWMutex
}

func NewSkipListIterator(cur *Node, sl *SkipList) *SkipListIterator {
	return &SkipListIterator{
		current: cur,
		sl:      sl,
		txnID:   0,
		closed:  false,
	}
}

// Valid 判断当前迭代器指向的 entry 是否有效
func (iter *SkipListIterator) Valid() bool {
	iter.mu.RLock()
	defer iter.mu.RUnlock()

	if iter.closed || iter.current == nil {
		return false
	}

	// 判断是否当前 entry 对于该事务是可见的
	if iter.txnID > 0 && iter.current.TxnID() > iter.txnID {
		// 不可见
		return false
	}

	return true
}
func (iter *SkipListIterator) Key() string {
	iter.mu.RLock()
	defer iter.mu.RUnlock()

	if !iter.Valid() {
		return ""
	}
	return iter.current.Key()
}
func (iter *SkipListIterator) Value() string {
	iter.mu.RLock()
	defer iter.mu.RUnlock()
	if !iter.Valid() {
		return ""
	}
	return iter.current.Value()
}
func (iter *SkipListIterator) TxnID() uint64 {
	iter.mu.RLock()
	defer iter.mu.RUnlock()

	if !iter.Valid() {
		return 0
	}
	return iter.current.TxnID()
}
func (iter *SkipListIterator) IsDeleted() bool {
	iter.mu.RLock()
	defer iter.mu.RUnlock()

	if !iter.Valid() {
		return false
	}
	return iter.current.IsDeleted()
}
func (iter *SkipListIterator) Entry() iterator.Entry {
	iter.mu.RLock()
	defer iter.mu.RUnlock()

	if !iter.Valid() {
		return iterator.Entry{}
	}
	return iter.current.Entry()
}
func (iter *SkipListIterator) Next() {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	if iter.closed || iter.current == nil {
		return
	}
	iter.current = iter.current.Next()
	// 跳过对于此次事务不可见的 entry
	for iter.current != nil && iter.txnID > 0 && iter.current.TxnID() > iter.txnID {
		iter.current = iter.current.Next()
	}
}
func (iter *SkipListIterator) Seek(target string) bool {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	if iter.closed {
		return false
	}

	cur := iter.sl.header

	for i := iter.sl.level - 1; i >= 0; i-- {
		for cur.forward[i] != nil && cur.forward[i].CompareKey(target) < 0 {
			cur = cur.forward[i]
		}
	}
	iter.current = cur.forward[0]
	// 跳过不可见的 entry
	for iter.current != nil && iter.txnID > 0 && iter.current.TxnID() > iter.txnID {
		iter.current = iter.current.Next()
	}
	// 是否找到的可见的 entry 是我们找的目标key
	if iter.current != nil && iter.current.CompareKey(target) == 0 {
		return true
	}
	return false
}
func (iter *SkipListIterator) SeekToFirst() {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	if iter.closed {
		return
	}

	iter.current = iter.sl.header.forward[0]

	// Skip entries that are not visible to this transaction
	for iter.current != nil && iter.txnID > 0 && iter.current.TxnID() > iter.txnID {
		iter.current = iter.current.Next()
	}
}
func (iter *SkipListIterator) SeekToLast() {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	if iter.closed {
		return
	}

	current := iter.sl.header

	// Navigate to the last node
	for i := iter.sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil {
			current = current.forward[i]
		}
	}

	iter.current = current

	// If this entry is not visible, move backwards to find a visible one
	// Note: This is a simplified implementation. In practice, you might want
	// to add backward pointers for more efficient backward traversal
	if iter.current != nil && iter.txnID > 0 && iter.current.TxnID() > iter.txnID {
		// For simplicity, we'll just seek to first and iterate to the last visible entry
		iter.current = iter.sl.header.forward[0]
		var lastVisible *Node

		for iter.current != nil {
			if iter.txnID == 0 || iter.current.TxnID() <= iter.txnID {
				lastVisible = iter.current
			}
			iter.current = iter.current.Next()
		}

		iter.current = lastVisible
	}
}
func (iter *SkipListIterator) GetType() iterator.IteratorType {
	return iterator.SkipListIteratorType
}
func (iter *SkipListIterator) Close() {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	iter.closed = true
	iter.current = nil
	iter.sl = nil
}

// SetTxnID 对于可见性检测得到的iter，设置事务ID
func (iter *SkipListIterator) SetTxnID(txnID uint64) {
	iter.mu.Lock()
	defer iter.mu.Unlock()
	iter.txnID = txnID
}
