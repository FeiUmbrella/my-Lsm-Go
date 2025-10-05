package memtable

import (
	"fmt"
	"myLsm-Go/pkg/config"
	"myLsm-Go/pkg/iterator"
)

// FlushResult 是flush MemTable 持久化到磁盘的结构
type FlushResult struct {
	Entries          []iterator.Entry
	MinTxnID         uint64
	MaxTxnID         uint64
	FirstKey         string
	LastKey          string
	FlushedSizeBytes int
	// 要持久化表中被删除k-v(即k/v = "")的txnID
	FlushedTxnIDs []uint64
}

// FlushOldest 持久化最后一个冻结表，如果不存在冻结表则尝试将热表变为冻结表持久化
func (mt *MemTable) FlushOldest() (*FlushResult, error) {
	mt.frozenMutex.Lock()
	defer mt.frozenMutex.Unlock()

	// 没有冻结表，看一下热表是否可以冻结
	if len(mt.frozenTables) == 0 {
		mt.currentMutex.Lock()
		if mt.currentTable.SizeBytes() == 0 { // 热表大小为0
			mt.currentMutex.Unlock()
			return nil, nil
		}
		// 冻结热表
		mt.freezeCurrentTableLocked()
		mt.currentMutex.Unlock()
	}

	oldestTable := mt.frozenTables[len(mt.frozenTables)-1]
	mt.frozenTables = mt.frozenTables[:len(mt.frozenTables)-1]
	mt.frozenBytes -= oldestTable.SizeBytes()

	entries := oldestTable.Flush() // 获取表中所有的entries
	if len(entries) == 0 {
		return &FlushResult{}, nil
	}

	res := &FlushResult{
		Entries:          entries,
		MinTxnID:         entries[0].TxnID,
		MaxTxnID:         entries[0].TxnID,
		FirstKey:         entries[0].Key,
		LastKey:          entries[len(entries)-1].Key,
		FlushedSizeBytes: oldestTable.SizeBytes(),
		FlushedTxnIDs:    make([]uint64, 0),
	}
	for _, entry := range entries {
		res.MinTxnID = min(res.MinTxnID, entry.TxnID)
		res.MaxTxnID = max(res.MaxTxnID, entry.TxnID)
		if entry.Key == "" || entry.Value == "" {
			res.FlushedTxnIDs = append(res.FlushedTxnIDs, entry.TxnID)
		}
	}
	return res, nil
}

// FlushAll 持久化所有的表(current + frozen)
func (mt *MemTable) FlushAll() ([]*FlushResult, error) {
	var res []*FlushResult

	// 冻结热表
	mt.currentMutex.Lock()
	mt.frozenMutex.Lock()
	if mt.currentTable.Size() > 0 {
		mt.freezeCurrentTableLocked()
	}
	mt.currentMutex.Unlock()
	mt.frozenMutex.Unlock()
	// 持久化
	for {
		result, err := mt.FlushOldest()
		if err != nil {
			return res, err
		}
		if result == nil {
			break // 已经全部持久化
		}
		res = append(res, result)
	}
	return res, nil
}

// CanFlush 如果存在冻结表，表明有表可以持久化那么返回true
func (mt *MemTable) CanFlush() bool {
	mt.frozenMutex.RLock()
	hasFrozen := len(mt.frozenTables) > 0
	mt.frozenMutex.RUnlock()

	// 有冻结表，那么一定可以持久化
	if hasFrozen {
		return true
	}

	// 仅有热表,不要盲目表示可持久化，先直接返回false
	// 如果热表达到表大小限制，会转为冻结表在下次进行持久化
	return false
}

// Empty 当MemTable为空 返回 true
func (mt *MemTable) Empty() bool {
	return (mt.currentTable == nil || mt.currentTable.Size() == 0) || len(mt.frozenTables) == 0
}

// EstimateFlushSizeBytes 返回被持久化的数据字节大小
// 返回最后一个冻结表字节大小，没有冻结表返回热表字节大小
func (mt *MemTable) EstimateFlushSizeBytes() int {
	mt.frozenMutex.RLock()
	frozenCount := len(mt.frozenTables)
	mt.frozenMutex.RUnlock()

	if frozenCount > 0 {
		mt.frozenMutex.RLock()
		oldestSize := mt.frozenTables[len(mt.frozenTables)-1].SizeBytes()
		mt.frozenMutex.RUnlock()
		return oldestSize
	}

	return mt.currentTable.SizeBytes()
}

// GetFlushableTableCount 返回可以被持久化的表的数量
func (mt *MemTable) GetFlushableTableCount() int {
	mt.frozenMutex.RLock()
	cnt := len(mt.frozenTables)
	mt.frozenMutex.RUnlock()

	mt.currentMutex.RLock()
	if mt.currentTable.Size() > 0 {
		cnt++
	}
	mt.currentMutex.RUnlock()
	return cnt
}

// ShouldFlush 根据配置文件判断当前MemTable是否需要持久化
func (mt *MemTable) ShouldFlush() bool {
	stats := mt.GetMemTableStats()

	// 判断是否有太多冻结表
	if stats.FrozenTablesCount > 3 {
		return true
	}

	// 检查总大小是否太大
	totalSizeLimit := mt.getTotalSizeLimit()
	return stats.TotalSize > totalSizeLimit
}

func (mt *MemTable) getTotalSizeLimit() int {
	cfg := config.GetGlobalConfig()
	return int(cfg.GetTotalMemSizeLimit())
}

// GetMemTableStats 返回MemTable当前信息
func (mt *MemTable) GetMemTableStats() MemTableStats {
	mt.currentMutex.RLock()
	curSize := mt.currentTable.SizeBytes()
	curEntries := mt.currentTable.Size()
	mt.currentMutex.RUnlock()

	mt.frozenMutex.RLock()
	frozenSize := mt.frozenBytes
	frozenEntries := len(mt.frozenTables)
	mt.frozenMutex.RUnlock()

	return MemTableStats{
		curSize,
		curEntries,
		frozenSize,
		frozenEntries,
		curSize + frozenSize,
		curEntries,
	}
}

// MemTableStats 是MemTable当前信息
type MemTableStats struct {
	CurrentTableSize    int // 热表占用内存大小
	CurrentTableEntries int // 热表 entry 个数
	FrozenTablesSize    int // 冻结表占用内存大小
	FrozenTablesCount   int // 冻结表 entry 个数
	TotalSize           int // 整个 MemTable 占用内存大小
	TotalEntries        int // 热表 entry 个数,不包括冻结表降低复杂性
}

// String returns a string representation of MemTableStats
func (stats MemTableStats) String() string {
	return fmt.Sprintf("MemTableStats{Current: %d bytes (%d entries), Frozen: %d bytes (%d tables), Total: %d bytes}",
		stats.CurrentTableSize, stats.CurrentTableEntries,
		stats.FrozenTablesSize, stats.FrozenTablesCount,
		stats.TotalSize)
}
