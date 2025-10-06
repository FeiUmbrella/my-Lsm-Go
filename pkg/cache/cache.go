package cache

import (
	"container/heap"
	"myLsm-Go/pkg/block"
	"sync"
	"time"
)

// BlockCacheKey represents a cache key for a block
type BlockCacheKey struct {
	SSTID   uint64 // SST file ID
	BlockID uint64 // Block index within the SST
}

// cacheEntry represents a cached item
type cacheEntry struct {
	key        BlockCacheKey
	block      *block.Block
	timestamps []int64 // 记录最近访问的时间戳列表

	// For heap implementation
	index int // index of the item in the heap
}

// BlockCache 实现LFU的block缓存
type BlockCache struct {
	capacity int
	maxFreq  int // 时间戳列表的最大长度
	cache    map[BlockCacheKey]*cacheEntry
	mutex    sync.RWMutex

	// For frequency based eviction
	freqHeap *entryHeap
}

// NewBlockCache creates a new block cache with the specified capacity
func NewBlockCache(capacity int) *BlockCache {
	if capacity <= 0 {
		panic("capacity must be positive")
	}
	bc := &BlockCache{
		capacity: capacity,
		maxFreq:  32, // 默认最大频率限制
		cache:    make(map[BlockCacheKey]*cacheEntry),
		freqHeap: &entryHeap{},
	}
	heap.Init(bc.freqHeap)
	return bc
}

// entryHeap implements heap.Interface and holds cache entries
type entryHeap []*cacheEntry

func (h entryHeap) Len() int { return len(h) }

// Less 弹出访问次数最小的，其次弹出最近一次访问时间最旧的
func (h entryHeap) Less(i, j int) bool {
	// We want Pop to give us the entry with minimum frequency
	// If frequencies are equal, prefer the least recently used
	if len(h[i].timestamps) == len(h[j].timestamps) {
		// 如果都为空，按索引排序
		if len(h[i].timestamps) == 0 {
			return h[i].index < h[j].index
		}
		// 优先驱逐最近访问时间更久远的
		return h[i].timestamps[len(h[i].timestamps)-1] < h[j].timestamps[len(h[j].timestamps)-1]
	}
	return len(h[i].timestamps) < len(h[j].timestamps)
}
func (h entryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *entryHeap) Push(x interface{}) {
	n := len(*h)
	entry := x.(*cacheEntry)
	entry.index = n
	*h = append(*h, entry)
}
func (h *entryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil   // avoid memory leak
	entry.index = -1 // for safety
	*h = old[0 : n-1]
	return entry
}

// Get 从缓存中取出指定SST的特定block
func (bc *BlockCache) Get(sstID, blockID uint64) *block.Block {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()
	key := BlockCacheKey{SSTID: sstID, BlockID: blockID}

	if chEntry, ok := bc.cache[key]; ok {
		// 由于map和小根堆保存的是地址，修改map中的block缓存也即修改小根堆中的
		chEntry.timestamps = append(chEntry.timestamps, time.Now().UnixNano())
		// 是否超过时间戳数组最大长度，移除最旧的访问时间戳
		if bc.capacity < len(chEntry.timestamps) {
			chEntry.timestamps = chEntry.timestamps[1:]
		}
		heap.Fix(bc.freqHeap, chEntry.index) // 在index处重排序
		return (*chEntry).block
	}
	return nil
}

// Put 向缓存中添加一个block缓存
func (bc *BlockCache) Put(sstID, blockID uint64, block *block.Block) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	key := BlockCacheKey{SSTID: sstID, BlockID: blockID}
	// 查找是否已经存在
	if chEntry, ok := bc.cache[key]; ok {
		chEntry.block = block
		chEntry.timestamps = append(chEntry.timestamps, time.Now().UnixNano())
		// 如果超过最大长度，移除最旧的时间戳
		if len(chEntry.timestamps) > bc.maxFreq {
			chEntry.timestamps = chEntry.timestamps[1:]
		}
		heap.Fix(bc.freqHeap, chEntry.index)
		return
	}

	// 不存在需要添加，如果容量满了需要弹出一个先
	if len(bc.cache) >= bc.capacity {
		bc.evict()
	}

	entry := &cacheEntry{
		key:        key,
		block:      block,
		timestamps: []int64{time.Now().UnixNano()},
	}
	bc.cache[key] = entry
	heap.Push(bc.freqHeap, entry) // Push 函数会给entry.index赋值
}

// Remove 从缓存中移除特定block
func (bc *BlockCache) Remove(sstID, blockID uint64) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	key := BlockCacheKey{SSTID: sstID, BlockID: blockID}
	if chEntry, ok := bc.cache[key]; ok {
		heap.Remove(bc.freqHeap, chEntry.index)
		delete(bc.cache, key)
	}
}

// Clear 将所有block从缓存移除
func (bc *BlockCache) Clear() {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	bc.cache = make(map[BlockCacheKey]*cacheEntry)
	bc.freqHeap = &entryHeap{}
	heap.Init(bc.freqHeap)
}

// evict 从缓存中置换出最不经常使用的block
func (bc *BlockCache) evict() {
	if bc.freqHeap.Len() == 0 {
		return
	}
	entry := bc.freqHeap.Pop().(*cacheEntry)
	delete(bc.cache, entry.key)
}

// Capacity returns the maximum capacity of the cache
func (bc *BlockCache) Capacity() int {
	return bc.capacity
}

// Stats returns cache statistics
func (bc *BlockCache) Stats() BlockCacheStats {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()

	return BlockCacheStats{
		Size:     len(bc.cache),
		Capacity: bc.capacity,
	}
}

// BlockCacheStats represents cache statistics
type BlockCacheStats struct {
	Size     int // 当前缓存中block个数
	Capacity int // 缓存最大容量
}
