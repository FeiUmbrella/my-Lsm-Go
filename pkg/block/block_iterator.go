package block

import (
	"myLsm-Go/pkg/iterator"
	"slices"
)

// BlockIterator 继承 Iterator 接口 实现 block 迭代器，同时支持MVCC
type BlockIterator struct {
	block      *Block
	index      int
	txnID      uint64
	closed     bool
	aggregated []iterator.Entry // 对于该迭代器的txnID可见的最新的每个key的entry
}

// NewBlockIterator creates a new block iterator
func NewBlockIterator(block *Block) *BlockIterator {
	iter := &BlockIterator{
		block:  block,
		index:  -1,
		txnID:  0,
		closed: false,
	}

	// Pre-aggregate MVCC entries
	iter.buildAggregatedEntries()

	return iter
}

// buildAggregatedEntries pre-aggregates all entries by key for MVCC
func (iter *BlockIterator) buildAggregatedEntries() {
	if iter.block.NumEntries() == 0 {
		iter.aggregated = []iterator.Entry{}
		return
	}

	// Group entries by key and keep the best version for each key based on txnID
	keyMap := make(map[string]*iterator.Entry)

	for i := 0; i < iter.block.NumEntries(); i++ {
		entry, err := iter.block.GetEntry(i)
		if err != nil {
			continue
		}

		// Check if this entry is visible to our transaction
		if iter.txnID > 0 && entry.TxnID > iter.txnID {
			continue // Skip entries from future transactions
		}

		// Keep the latest visible version of each key
		if existing, exists := keyMap[entry.Key]; !exists || entry.TxnID > existing.TxnID {
			keyMap[entry.Key] = &entry
		}
	}

	// Convert map to sorted slice
	iter.aggregated = make([]iterator.Entry, len(keyMap))
	keys := make([]string, 0)
	for k, _ := range keyMap {
		keys = append(keys, k)
	}

	// Sort by key
	slices.Sort(keys)
	for i, key := range keys {
		iter.aggregated[i] = *keyMap[key]
	}
}

// Valid returns true if the iterator is pointing to a valid entry
func (iter *BlockIterator) Valid() bool {
	return !iter.closed && iter.index >= 0 && iter.index < len(iter.aggregated)
}

// Key returns the key of the current entry
func (iter *BlockIterator) Key() string {
	if !iter.Valid() {
		return ""
	}
	return iter.aggregated[iter.index].Key
}

// Value returns the value of the current entry
func (iter *BlockIterator) Value() string {
	if !iter.Valid() {
		return ""
	}
	return iter.aggregated[iter.index].Value
}

// TxnID returns the transaction ID of the current entry
func (iter *BlockIterator) TxnID() uint64 {
	if !iter.Valid() {
		return 0
	}
	return iter.aggregated[iter.index].TxnID
}

// IsDeleted returns true if the current entry is a delete marker
func (iter *BlockIterator) IsDeleted() bool {
	if !iter.Valid() {
		return false
	}
	return iter.aggregated[iter.index].Value == ""
}

// Entry returns the current entry
func (iter *BlockIterator) Entry() iterator.Entry {
	if !iter.Valid() {
		return iterator.Entry{}
	}
	return iter.aggregated[iter.index]
}

// Next advances the iterator to the next key (with MVCC aggregation)
func (iter *BlockIterator) Next() {
	if iter.closed {
		return
	}

	// Move to next entry
	iter.index++
}

// seekToKey positions the iterator at the first entry with key >= target in aggregated entries
func (iter *BlockIterator) seekToKey(key string) int {
	// Binary search in aggregated entries
	left, right := 0, len(iter.aggregated)

	for left < right {
		mid := (left + right) / 2
		if iter.aggregated[mid].Key < key {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return left
}

// Seek positions the iterator at the first entry with key >= target
func (iter *BlockIterator) Seek(key string) bool {
	if iter.closed {
		return false
	}

	// Find the first entry >= key in aggregated entries
	iter.index = iter.seekToKey(key)

	// Return true if we found an exact match
	if iter.Valid() && iter.aggregated[iter.index].Key == key {
		return true
	}
	return false
}

// SeekToFirst positions the iterator at the first entry
func (iter *BlockIterator) SeekToFirst() {
	if iter.closed {
		return
	}

	iter.index = 0
}

// SeekToLast positions the iterator at the last entry
func (iter *BlockIterator) SeekToLast() {
	if iter.closed {
		return
	}

	iter.index = len(iter.aggregated) - 1
}

// GetType returns the iterator type
func (iter *BlockIterator) GetType() iterator.IteratorType {
	return iterator.SSTIteratorType
}

// Close releases any resources held by the iterator
func (iter *BlockIterator) Close() {
	iter.closed = true
	iter.block = nil
}

// SetTxnID sets the transaction ID for visibility checking
func (iter *BlockIterator) SetTxnID(txnID uint64) {
	if iter.txnID != txnID {
		iter.txnID = txnID
		// Re-aggregate entries with new transaction visibility
		iter.buildAggregatedEntries()
		// Reset position
		iter.index = -1
	}
}
