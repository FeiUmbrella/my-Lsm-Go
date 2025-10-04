package skiplist

import (
	"math/rand"
	"myLsm-Go/pkg/iterator"
	"time"
)

const (
	// MaxLevel 跳表最大的层级
	MaxLevel = 16
	// Probability 初始化节点层数时，是否提升层级的概率
	Probability = 0.5
)

type SkipList struct {
	header    *Node // 不包含数据的头结点
	level     int   // 跳表当前的最高层级,[0, level-1]
	size      int   // 元素个数
	sizeBytes int
	rng       *rand.Rand // 随机数生成器
}

// New 创建一个跳表
func New() *SkipList {
	header := NewNode("", "", 0, MaxLevel)
	return &SkipList{
		header: header,
		level:  1,
		size:   0,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Put 插入/更新键值对
// 因为tranId的存在，由于tranId的不同，其实每次都是插入而非更新
func (sl *SkipList) Put(key string, value string, tranId uint64) {
	cur := sl.header
	tmpNode := NewNode(key, value, tranId, 1)
	updateNode := make([]*Node, MaxLevel) // 要插入的点

	for level := sl.level - 1; level >= 0; level-- {
		// 下个节点存在且<key
		for cur.forward[level] != nil && cur.forward[level].CompareNode(tmpNode) < 0 {
			cur = cur.forward[level]
		}
		updateNode[level] = cur
	}

	newLevel := sl.randomLevel()
	newNode := NewNode(key, value, tranId, newLevel)
	if newLevel > sl.level { // 新节点的层数高于当前跳表的层数
		for i := sl.level; i < newLevel; i++ {
			updateNode[i] = sl.header
		}
		sl.level = newLevel
	}

	// 将updateNode的下个节点指向newNode
	for level := range newLevel {
		// 更新双向指针
		newNode.forward[level] = updateNode[level].forward[level]
		if updateNode[level].forward[level] != nil {
			updateNode[level].forward[level].backward = newNode
		}
		updateNode[level].forward[level] = newNode
	}

	newNode.backward = updateNode[0]
	// todo: 这个地方在上面for循环中level=0时更新过了吧？
	//if newNode.forward[0] != nil {
	//	newNode.forward[0].backward = newNode
	//}

	sl.size++
	sl.sizeBytes += len(key) + len(value) + 8
}

// randomLevel 生成随机的层数
func (sl *SkipList) randomLevel() int {
	level := 1
	for sl.rng.Float32() < Probability && level < MaxLevel {
		level++
	}
	return level
}

// Get 获取某一txnID的key对
// txnID = 0: 找到最新版本的key对
// txnID != 0: 找到当前可见的最新key对
func (sl *SkipList) Get(key string, txnID uint64) *SkipListIterator {
	cur := sl.header
	tempNode := NewNode(key, "", txnID, 1)
	for level := sl.level - 1; level >= 0; level-- {
		for cur.forward[level] != nil && cur.forward[level].CompareNode(tempNode) < 0 {
			cur = cur.forward[level]
		}
	}

	cur = cur.forward[0]

	// 找到最新版本的k-v
	for cur != nil && cur.Key() == key {
		if txnID == 0 || cur.TxnID() <= txnID {
			iter := NewSkipListIterator(cur, sl)
			iter.SetTxnID(txnID)
			return iter
		}
		cur = cur.forward[0]
	}

	// 不存在key
	iter := NewSkipListIterator(nil, sl)
	iter.SetTxnID(txnID)
	return iter
}

// Remove 删除键值对
// LSM Tree 以仅追加写入的方式使用我们的SkipList，所以这个函数不会使用，这里实现仅为了跳表的完整性
func (sl *SkipList) Remove(key string, txnID uint64) {
	if sl.Get(key, txnID).current == nil {
		return
	}

	tempNode := NewNode(key, "", txnID, 1)
	cur := sl.header
	for level := sl.level - 1; level >= 0; level-- {
		for cur.forward[level] != nil && cur.forward[level].CompareNode(tempNode) < 0 {
			cur = cur.forward[level]
		}

		if cur.forward[level] != nil && cur.forward[level].CompareNode(tempNode) == 0 {
			if cur.forward[level].forward[level] != nil {
				cur.forward[level].forward[level].backward = cur
			}
			cur.forward[level] = cur.forward[level].forward[level]
		}
	}
	return
}

// Delete 插入一个(txnID,key,"")的k-v表示删除原有key
// 这个txnID是k=key的k-v对中最大的，出现在k为key的k-v对最前面
func (sl *SkipList) Delete(key string, txnID uint64) {
	sl.Put(key, "", txnID)
}

func (sl *SkipList) Size() int      { return sl.size }
func (sl *SkipList) SizeBytes() int { return sl.sizeBytes }
func (sl *SkipList) IsEmpty() bool  { return sl.size == 0 }
func (sl *SkipList) Clear() {
	sl.header = NewNode("", "", 0, MaxLevel)
	sl.level = 1
	sl.size = 0
	sl.sizeBytes = 0
}

// NewIterator 创造一个新的跳表迭代器
func (sl *SkipList) NewIterator(txnID uint64) iterator.Iterator {
	iter := NewSkipListIterator(sl.header.forward[0], sl)
	iter.SetTxnID(txnID)
	return iter
}

// Flush 以切片形式返回跳表中所有的entry
func (sl *SkipList) Flush() []iterator.Entry {

	entries := make([]iterator.Entry, 0, sl.size)
	current := sl.header.forward[0]

	for current != nil {
		entries = append(entries, current.Entry())
		current = current.forward[0]
	}

	return entries
}
