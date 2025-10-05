package memtable

import (
	"myLsm-Go/pkg/common"
	"myLsm-Go/pkg/config"
	"myLsm-Go/pkg/iterator"
	"myLsm-Go/pkg/skiplist"
	"sync"
)

// MemTable 是内存中的表结构
// 由热表和若干冻结表构成，底层结构都为跳表
type MemTable struct {
	currentTable *skiplist.SkipList   // 热表，可写可读
	frozenTables []*skiplist.SkipList // 冻结表，只读
	frozenBytes  int                  // 冻结表中总字节数

	currentMutex sync.RWMutex // 热表的锁
	frozenMutex  sync.RWMutex // 冻结表的锁
}

// New 创建一个 MEMTable 实例
func New() *MemTable {
	return &MemTable{
		currentTable: skiplist.New(),
		frozenTables: make([]*skiplist.SkipList, 0),
		frozenBytes:  0,
	}
}

// Put 添加 k-v 对到热表
func (mt *MemTable) Put(key, value string, txnID uint64) error {
	mt.currentMutex.Lock()
	defer mt.currentMutex.Unlock()

	mt.currentTable.Put(key, value, txnID)

	// 判断热表是否超过容量限制，如果超过则需要冻结并创建新的热表
	cfg := config.GetGlobalConfig()
	if mt.currentTable.SizeBytes() > int(cfg.GetPerMemSizeLimit()) {
		mt.frozenMutex.Lock()
		mt.freezeCurrentTableLocked()
		mt.frozenMutex.Unlock()
	}
	return nil
}

// PutBatch 一个操作同时添加多个k-v对
func (mt *MemTable) PutBatch(kvs []common.KVPair, txnID uint64) error {
	mt.currentMutex.Lock()
	defer mt.currentMutex.Unlock()

	// Add all entries to current table
	for _, kv := range kvs {
		mt.currentTable.Put(kv.Key, kv.Value, txnID)
	}

	// Check if current table size exceeds limit and needs to be frozen
	cfg := config.GetGlobalConfig()
	if mt.currentTable.SizeBytes() > int(cfg.GetPerMemSizeLimit()) {
		// Need to freeze current table
		mt.frozenMutex.Lock()
		mt.freezeCurrentTableLocked()
		mt.frozenMutex.Unlock()
	}

	return nil
}

// Delete 通过插入一个更新的txnID的空value的k-v对实现删除旧k-v
func (mt *MemTable) Delete(key string, txnID uint64) error {
	mt.currentMutex.Lock()
	defer mt.currentMutex.Unlock()

	// Delete by putting empty value (tombstone)
	mt.currentTable.Delete(key, txnID)

	// Check if current table size exceeds limit and needs to be frozen
	cfg := config.GetGlobalConfig()
	if mt.currentTable.SizeBytes() > int(cfg.GetPerMemSizeLimit()) {
		// Need to freeze current table
		mt.frozenMutex.Lock()
		mt.freezeCurrentTableLocked()
		mt.frozenMutex.Unlock()
	}

	return nil
}

// DeleteBatch 通过插入一批更新的txnID的空value的k-v对实现删除一批对应旧k-v
func (mt *MemTable) DeleteBatch(keys []string, txnID uint64) error {
	mt.currentMutex.Lock()
	defer mt.currentMutex.Unlock()

	// Delete all keys by putting empty values (tombstones)
	for _, key := range keys {
		mt.currentTable.Delete(key, txnID)
	}

	// Check if current table size exceeds limit and needs to be frozen
	cfg := config.GetGlobalConfig()
	if mt.currentTable.SizeBytes() > int(cfg.GetPerMemSizeLimit()) {
		// Need to freeze current table
		mt.frozenMutex.Lock()
		mt.freezeCurrentTableLocked()
		mt.frozenMutex.Unlock()
	}

	return nil
}

// Get 获取k-v
func (mt *MemTable) Get(key string, txnID uint64) (string, bool, error) {
	// 先检查热表
	mt.currentMutex.RLock()
	iter := mt.currentTable.Get(key, txnID)
	mt.currentMutex.RUnlock()

	if iter.Valid() {
		value := iter.Value()
		isDeleted := iter.IsDeleted()
		iter.Close()

		if isDeleted {
			return "", false, nil
		}
		return value, true, nil
	}
	iter.Close()

	// 热表没找到，找 frozen tables
	mt.frozenMutex.RLock()
	defer mt.frozenMutex.RUnlock()

	for _, ft := range mt.frozenTables {
		iter := ft.Get(key, txnID)
		if iter.Valid() {
			value := iter.Value()
			isDeleted := iter.IsDeleted()
			iter.Close()

			if isDeleted {
				return "", false, nil // Key was deleted
			}
			return value, true, nil
		}
		iter.Close()
	}

	return "", false, nil // key 不存在
}

// GetResult represents the result of a Get operation
type GetResult struct {
	Key   string
	Value string
	Found bool
}

// GetBatch retrieves multiple keys from the MemTable
func (mt *MemTable) GetBatch(keys []string, txnID uint64) ([]GetResult, error) {
	results := make([]GetResult, len(keys))

	// First check current table for all keys
	mt.currentMutex.RLock()
	for i, key := range keys {
		iter := mt.currentTable.Get(key, txnID)
		if iter.Valid() {
			value := iter.Value()
			isDeleted := iter.IsDeleted()
			results[i] = GetResult{
				Key:   key,
				Value: value,
				Found: !isDeleted,
			}
		} else {
			results[i] = GetResult{
				Key:   key,
				Found: false,
			}
		}
		iter.Close()
	}
	mt.currentMutex.RUnlock()

	// For keys not found in current table, check frozen tables
	mt.frozenMutex.RLock()
	defer mt.frozenMutex.RUnlock()

	for i, key := range keys {
		if results[i].Found {
			continue // Already found in current table
		}

		// Check frozen tables from newest to oldest
		for j := 0; j < len(mt.frozenTables); j++ {
			iter := mt.frozenTables[j].Get(key, txnID)
			if iter.Valid() {
				value := iter.Value()
				isDeleted := iter.IsDeleted()
				results[i] = GetResult{
					Key:   key,
					Value: value,
					Found: !isDeleted,
				}
				iter.Close()
				break
			}
			iter.Close()
		}
	}

	return results, nil
}

// freezeCurrentTableLocked 冻结热表，创建新热表
// 调用时必须持有热表和冻结表的锁
func (mt *MemTable) freezeCurrentTableLocked() {
	mt.frozenBytes += mt.currentTable.SizeBytes()
	// 热表一定要冻结在最前面，因为后面查询k-v时需要从最新的开始查
	mt.frozenTables = append([]*skiplist.SkipList{mt.currentTable}, mt.frozenTables...)
	// 创建新热表
	mt.currentTable = skiplist.New()
}

// FreezeCurrentTable explicitly freezes the current table
func (mt *MemTable) FreezeCurrentTable() {
	mt.currentMutex.Lock()
	mt.frozenMutex.Lock()
	mt.freezeCurrentTableLocked()
	mt.frozenMutex.Unlock()
	mt.currentMutex.Unlock()
}

// GetCurrentSize 返回热表内存大小
func (mt *MemTable) GetCurrentSize() int {
	mt.currentMutex.RLock()
	defer mt.currentMutex.RUnlock()
	return mt.currentTable.SizeBytes()
}

// GetFrozenSize 返回所有冻结表的内存大小
func (mt *MemTable) GetFrozenSize() int {
	mt.frozenMutex.RLock()
	defer mt.frozenMutex.RUnlock()
	return mt.frozenBytes
}

// GetTotalSize 返回热表和冻结表的内存总大小
func (mt *MemTable) GetTotalSize() int {
	return mt.GetCurrentSize() + mt.GetFrozenSize()
}

// GetCurrentTableSize 返回热表的元素个数
func (mt *MemTable) GetCurrentTableSize() int {
	mt.currentMutex.RLock()
	defer mt.currentMutex.RUnlock()
	return mt.currentTable.Size()
}

// GetFrozenTableCount 返回冻结表个数
func (mt *MemTable) GetFrozenTableCount() int {
	mt.frozenMutex.RLock()
	defer mt.frozenMutex.RUnlock()
	return len(mt.frozenTables)
}

// Clear 移除MemTable中的所有数据
func (mt *MemTable) Clear() {
	mt.currentMutex.Lock()
	mt.frozenMutex.Lock()
	defer mt.currentMutex.Unlock()
	defer mt.frozenMutex.Unlock()

	mt.currentTable.Clear()
	mt.frozenTables = make([]*skiplist.SkipList, 0)
	mt.frozenBytes = 0
}

// NewIterator creates a new iterator over all entries in the MemTable
func (mt *MemTable) NewIterator(txnID uint64) iterator.Iterator {
	return NewMemTableIterator(mt, txnID)
}
