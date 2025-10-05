package memtable

import (
	"container/heap"
	"myLsm-Go/pkg/iterator"
)

// MemTableIterator 指向MemTable整体的迭代器，可以遍历热表和所有冻结表
// 使用一个 小根堆 合并指向每个表的迭代器，每个表的迭代器可以遍历这个跳表的第0层的所有Entry
type MemTableIterator struct {
	memTable *MemTable
	txnID    uint64
	heap     *IteratorHeap
	current  *iterator.Entry // 表迭代器指向MemTable中某一Entry
	closed   bool
}

func (iter *MemTableIterator) Valid() bool {
	return !iter.closed && iter.current != nil
}

func (iter *MemTableIterator) Key() string {
	if !iter.Valid() {
		return ""
	}
	return iter.current.Key
}

func (iter *MemTableIterator) Value() string {
	if !iter.Valid() {
		return ""
	}
	return iter.current.Value
}

func (iter *MemTableIterator) TxnID() uint64 {
	if !iter.Valid() {
		return 0
	}
	return iter.current.TxnID
}

func (iter *MemTableIterator) IsDeleted() bool {
	if !iter.Valid() {
		return false
	}
	return iter.current.Value == ""
}

func (iter *MemTableIterator) Entry() iterator.Entry {
	if !iter.Valid() {
		return iterator.Entry{}
	}
	return *iter.current
}

func (iter *MemTableIterator) Next() {
	if !iter.Valid() {
		return
	}
	iter.advance()
}

// Seek positions the iterator at the first entry with key >= target
func (iter *MemTableIterator) Seek(target string) bool {
	if iter.closed {
		return false
	}
	// 需要关闭当前每个表的迭代器
	// 因为这些迭代器可能不在初始位置，需要从初始位置开始去找
	iter.closeAllIterators()
	iter.heap = NewIteratorHeap()

	// 重新初始每个表的迭代器为每个表的起始位置
	iter.seekToKey(target)
	iter.advance()

	// todo:Seek函数是找>=target的迭代器位置，这里判断条件应该为 iter.current.key >= target?
	if iter.Valid() && iter.current.Key == target {
		return true
	}
	return false
}

// closeAllIterators 关闭所有表的迭代器
func (iter *MemTableIterator) closeAllIterators() {
	for _, heapItem := range *iter.heap {
		heapItem.Iterator.Close()
	}
	iter.heap = &IteratorHeap{}
}

// seekToKey 初始化每个表的迭代器至每个表初始位置，然后移动至target key 位置
func (iter *MemTableIterator) seekToKey(target string) {
	// Add iterator for current table
	iter.memTable.currentMutex.RLock()
	currentIter := iter.memTable.currentTable.NewIterator(iter.txnID)
	currentIter.Seek(target)
	if currentIter.Valid() {
		heap.Push(iter.heap, &HeapItem{
			Iterator: currentIter,
			TableID:  0,
		})
	} else {
		currentIter.Close()
	}
	iter.memTable.currentMutex.RUnlock()

	// Add iterators for frozen tables
	iter.memTable.frozenMutex.RLock()
	for i, frozenTable := range iter.memTable.frozenTables {
		frozenIter := frozenTable.NewIterator(iter.txnID)
		frozenIter.Seek(target)
		if frozenIter.Valid() {
			heap.Push(iter.heap, &HeapItem{
				Iterator: frozenIter,
				TableID:  i + 1,
			})
		} else {
			frozenIter.Close()
		}
	}
	iter.memTable.frozenMutex.RUnlock()
}

func (iter *MemTableIterator) SeekToFirst() {
	if iter.closed {
		return
	}
	iter.closeAllIterators()
	iter.heap = NewIteratorHeap()
	iter.initializeIterators()
	iter.advance()
}

func (iter *MemTableIterator) SeekToLast() {
	if iter.closed {
		return
	}
	iter.SeekToFirst()
	var lastEntry *iterator.Entry
	for iter.Valid() {
		entry := iter.Entry()
		lastEntry = &entry
		iter.Next()
	}
	if lastEntry != nil {
		iter.current = lastEntry
	}
}

// GetType 返回迭代器类型
func (iter *MemTableIterator) GetType() iterator.IteratorType {
	return iterator.HeapIteratorType
}

func (iter *MemTableIterator) Close() {
	if iter.closed {
		return
	}
	iter.closeAllIterators()
	iter.closed = true
	iter.current = nil
}

// NewMemTableIterator creates a new iterator for the MemTable
func NewMemTableIterator(memTable *MemTable, txnID uint64) *MemTableIterator {
	iter := &MemTableIterator{
		memTable: memTable,
		txnID:    txnID,
		heap:     NewIteratorHeap(),
		closed:   false,
	}

	// Initialize iterators for all tables
	iter.initializeIterators()
	iter.advance()

	return iter
}

// initializeIterators 找到热表和所有冻结表的迭代器，并放进小根堆
func (iter *MemTableIterator) initializeIterators() {
	// 为热表添加迭代器，即底层跳表的第0层起始位置的迭代器
	iter.memTable.currentMutex.RLock()
	currentIter := iter.memTable.currentTable.NewIterator(iter.txnID) //热表对应跳表的迭代器
	currentIter.SeekToFirst()                                         // 移动到跳表0层起始位置
	if currentIter.Valid() {
		heap.Push(iter.heap, &HeapItem{
			Iterator: currentIter,
			TableID:  0,
		})
	} else {
		currentIter.Close()
	}
	iter.memTable.currentMutex.RUnlock()

	// 为冻结表添加迭代器，即底层跳表的第0层起始位置的迭代器
	iter.memTable.frozenMutex.RLock()
	for i, ft := range iter.memTable.frozenTables {
		forzenIter := ft.NewIterator(iter.txnID)
		forzenIter.SeekToFirst()
		if forzenIter.Valid() {
			heap.Push(iter.heap, &HeapItem{
				Iterator: forzenIter,
				TableID:  i + 1,
			})
		} else {
			forzenIter.Close()
		}
	}
	iter.memTable.frozenMutex.RUnlock()
}

// advance 移动MemTable迭代器到下一个有效的Entry
// 需要跳过重复的key以及在全局(热表+冻结表)key有序的基础上移动
// 即：下次移动的目标entry可能是另一张表迭代器的下一个位置
// 下一个位置是：key相同但txnID更大即更新 或 下个不同的 key2(此时这个key2的字典序大于key)
func (iter *MemTableIterator) advance() {
	iter.current = nil
	if iter.closed || iter.heap.Len() == 0 {
		return
	}

	var lastKey string
	var targetEntry *iterator.Entry
	var targetTxnID uint64

	// 从heap中拿出一张表的迭代器向后查找，当找到目标即下一个新的位置返回
	for iter.heap.Len() > 0 {
		item := heap.Pop(iter.heap).(*HeapItem)
		entry := item.Iterator.Entry()

		// 刚开始遍历 或 遍历到一个entry与上个key不同
		if targetEntry == nil || entry.Key != lastKey {
			if targetEntry != nil {
				// 此时移动到了目标位置
				iter.current = targetEntry
				heap.Push(iter.heap, item)
				return
			}
			targetEntry = &entry
			targetTxnID = entry.TxnID
			lastKey = entry.Key
		} else { // 相同key
			// 事务ID更大，且该事务ID对于MemTable的事务ID更小即对MemTable可见
			if entry.TxnID > targetTxnID && (iter.txnID == 0 || entry.TxnID <= iter.txnID) {
				targetEntry = &entry
				targetTxnID = entry.TxnID
			}
		}

		// 跳表迭代器向前遍历
		item.Iterator.Next()
		if item.Iterator.Valid() {
			heap.Push(iter.heap, item)
		} else {
			item.Iterator.Close()
		}
	}

	if targetEntry != nil {
		iter.current = targetEntry
	}
}

// HeapItem represents an item in the iterator heap
type HeapItem struct {
	Iterator iterator.Iterator
	TableID  int // 0 for current table, 1+ for frozen tables
}

// IteratorHeap implements a min-heap of iterators ordered by their current key
type IteratorHeap []*HeapItem

func NewIteratorHeap() *IteratorHeap {
	h := make(IteratorHeap, 0)
	heap.Init(&h)
	return &h
}

// Len returns the number of items in the heap
func (h IteratorHeap) Len() int { return len(h) }

// Less compares two heap items
func (h IteratorHeap) Less(i, j int) bool {
	keyI := h[i].Iterator.Key()
	keyJ := h[j].Iterator.Key()

	if keyI != keyJ {
		return keyI < keyJ
	}

	// Same key - prioritize by transaction ID (higher first), then table ID (lower first for newer tables)
	txnI := h[i].Iterator.TxnID()
	txnJ := h[j].Iterator.TxnID()

	if txnI != txnJ {
		return txnI > txnJ // Higher transaction ID first
	}

	return h[i].TableID < h[j].TableID // Lower table ID (newer) first
}

// Swap swaps two items in the heap
func (h IteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push adds an item to the heap
func (h *IteratorHeap) Push(x any) {
	*h = append(*h, x.(*HeapItem))
}

// Pop removes and returns the minimum item from the heap
func (h *IteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}
