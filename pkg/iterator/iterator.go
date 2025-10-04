package iterator

import "errors"

// IteratorType 迭代器类型
type IteratorType int

// 各种迭代器类型
const (
	SkipListIteratorType IteratorType = iota
	SSTIteratorType
	HeapIteratorType
	MergeIteratorType
	SelectIteratorType
	ConcatIteratorType
)

// Entry 是存储具有事务id的k-v对的实体
type Entry struct {
	Key   string
	Value string
	TxnID uint64 // 事务ID
}

// Iterator 是所有类型的迭代器的接口
type Iterator interface {
	// Valid 如果迭代器指向有效 entry 返回 true
	Valid() bool

	// Key 返回当前 entry 的key
	Key() string

	// Value 返回当前 entry 的value
	Value() string

	// TxnID 返回当前 entry 的TxnID
	TxnID() uint64

	// IsDeleted 返回当前 entry 是否为删除标志
	IsDeleted() bool

	// Entry 返回当前 entry
	Entry() Entry

	// Next 让迭代器向前走一步
	Next()

	// Seek 寻找 key >= target 的第一个迭代器
	Seek(key string) bool

	// SeekToFirst 位于第一个 entry 的迭代器位置
	SeekToFirst()

	// SeekToLast 位于最后 entry 的迭代器位置
	SeekToLast()

	// GetType 返回迭代器类型
	GetType() IteratorType

	// Close 释放被迭代器指向的资源
	Close()
}

// 共同错误
var (
	ErrInvalidIterator = errors.New("invalid iterator")
	ErrIteratorClosed  = errors.New("iterator is closed")
)

// EmptyIterator 是一个不包含 entry 的迭代器,实现了Iterator的接口
type EmptyIterator struct {
	closed bool
}

func NewEmptyIterator() *EmptyIterator {
	return &EmptyIterator{
		closed: false,
	}
}
func (e *EmptyIterator) Valid() bool           { return false }
func (e *EmptyIterator) Key() string           { return "" }
func (e *EmptyIterator) Value() string         { return "" }
func (e *EmptyIterator) TxnID() uint64         { return 0 }
func (e *EmptyIterator) IsDeleted() bool       { return false }
func (e *EmptyIterator) Next()                 {}
func (e *EmptyIterator) Seek(key string) bool  { return false }
func (e *EmptyIterator) SeekToFirst()          {}
func (e *EmptyIterator) SeekToLast()           {}
func (e *EmptyIterator) GetType() IteratorType { return SkipListIteratorType }
func (e *EmptyIterator) Close()                { e.closed = true }
func (e *EmptyIterator) Entry() Entry          { return Entry{} }

func CompareKeys(a, b string) (c int) {
	if a < b {
		c = -1
	} else if a > b {
		c = 1
	} else {
		c = 0
	}
	return
}

// CompareEntries 比较两个entry那个更小，先比较key再比较transaction id，更大的 txn ID 优先
func CompareEntries(a, b Entry) int {
	cmp := CompareKeys(a.Key, b.Key)
	if cmp != 0 {
		return cmp
	}

	if a.TxnID == 0 || b.TxnID == 0 {
		return cmp
	}

	if a.TxnID > b.TxnID {
		return -1
	}
	if a.TxnID < b.TxnID {
		return 1
	}
	return 0
}
