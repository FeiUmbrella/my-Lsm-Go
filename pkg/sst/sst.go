package sst

import (
	"encoding/binary"
	"myLsm-Go/pkg/block"
	"myLsm-Go/pkg/cache"
	"myLsm-Go/pkg/config"
	"myLsm-Go/pkg/iterator"
	"myLsm-Go/pkg/utils"
	"os"
)

// SST represents a Sorted String Table file
// SST 文件结构:
// | Block Section | Meta Section | Bloom Filter | Meta Offset(4) | Bloom Offset(4) | Min TxnID(8) | Max TxnID(8) |
type SST struct {
	sstID       uint64             // SST file ID
	file        *os.File           // File handle
	filePath    string             // File path
	metaEntries []BlockMeta        // Block metadata
	metaOffset  uint32             // Offset of metadata section
	bloomOffset uint32             // Offset of bloom filter
	firstKey    string             // First key in SST
	lastKey     string             // Last key in SST
	bloomFilter *utils.BloomFilter // Bloom filter for keys
	blockCache  *cache.BlockCache  // Block cache
	minTxnID    uint64             // Minimum transaction ID
	maxTxnID    uint64             // Maximum transaction ID
	fileSize    int64              // Total file size
}

// OpenSST opens an SST file with a file manager (test compatibility)
func OpenSST(sstID uint64, filePath string, blockCache *cache.BlockCache, fileManager *utils.FileManager) (*SST, error) {
	return Open(sstID, filePath, blockCache)
}

// Open 打开一个存在的SST文件,将里面的二进制数据反序列化存储在SST结构体并返回
func Open(sstID uint64, filePath string, blockCache *cache.BlockCache) (*SST, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	fileSize := fileInfo.Size()
	if fileSize < 24 { // Min size: 4+4+8+8 = 24 bytes for offsets and transaction IDs
		file.Close()
		return nil, utils.ErrInvalidSSTFile
	}

	sst := &SST{
		sstID:      sstID,
		file:       file,
		filePath:   filePath,
		blockCache: blockCache,
		fileSize:   fileSize,
	}

	// 从后往前读出文件内容
	// Read footer: MetaOffset(4) + BloomOffset(4) + MinTxnID(8) + MaxTxnID(8)
	footerSize := int64(24)
	footer := make([]byte, footerSize)
	_, err = file.ReadAt(footer, fileSize-footerSize)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Parse footer
	sst.metaOffset = binary.LittleEndian.Uint32(footer[0:4])
	sst.bloomOffset = binary.LittleEndian.Uint32(footer[4:8])
	sst.minTxnID = binary.LittleEndian.Uint64(footer[8:16])
	sst.maxTxnID = binary.LittleEndian.Uint64(footer[16:24])

	// Validate offsets
	if int64(sst.metaOffset) >= fileSize || int64(sst.bloomOffset) >= fileSize {
		file.Close()
		return nil, utils.ErrCorruptedFile
	}

	// Read and decode metadata
	metaSize := sst.bloomOffset - sst.metaOffset
	metaData := make([]byte, metaSize)
	_, err = file.ReadAt(metaData, int64(sst.metaOffset))
	if err != nil {
		file.Close()
		return nil, err
	}

	sst.metaEntries, err = DecodeBlockMetas(metaData)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Set first and last keys
	if len(sst.metaEntries) > 0 {
		sst.firstKey = sst.metaEntries[0].FirstKey
		sst.lastKey = sst.metaEntries[len(sst.metaEntries)-1].LastKey
	}

	// Read bloom filter if it exists
	bloomSize := int64(sst.metaOffset) - int64(sst.bloomOffset)
	if bloomSize > 0 {
		bloomData := make([]byte, bloomSize)
		_, err = file.ReadAt(bloomData, int64(sst.bloomOffset))
		if err != nil {
			file.Close()
			return nil, err
		}

		sst.bloomFilter = utils.DeserializeBloomFilter(bloomData)
	}

	return sst, nil
}

// Close closes the SST file
func (sst *SST) Close() error {
	if sst.file != nil {
		return sst.file.Close()
	}
	return nil
}
func (sst *SST) MetaOffset() int64 {
	return int64(sst.metaOffset)
}

// ID returns the SST ID
func (sst *SST) ID() uint64 {
	return sst.sstID
}

// FirstKey returns the first key in the SST
func (sst *SST) FirstKey() string {
	return sst.firstKey
}

// LastKey returns the last key in the SST
func (sst *SST) LastKey() string {
	return sst.lastKey
}

// Size returns the file size
func (sst *SST) Size() int64 {
	return sst.fileSize
}

// NumBlocks returns the number of blocks in the SST
func (sst *SST) NumBlocks() int {
	return len(sst.metaEntries)
}

// GetSSTID returns the SST ID (alias for ID)
func (sst *SST) GetSSTID() uint64 {
	return sst.sstID
}

// GetFirstKey returns the first key in the SST (alias for FirstKey)
func (sst *SST) GetFirstKey() string {
	return sst.firstKey
}

// GetLastKey returns the last key in the SST (alias for LastKey)
func (sst *SST) GetLastKey() string {
	return sst.lastKey
}

// GetNumBlocks returns the number of blocks in the SST (alias for NumBlocks)
func (sst *SST) GetNumBlocks() int {
	return len(sst.metaEntries)
}

// TxnRange returns the transaction ID range
func (sst *SST) TxnRange() (uint64, uint64) {
	return sst.minTxnID, sst.maxTxnID
}

// ReadBlock reads a block by index
func (sst *SST) ReadBlock(blockIdx int) (*block.Block, error) {
	if blockIdx < 0 || blockIdx >= len(sst.metaEntries) {
		return nil, utils.ErrBlockNotFound
	}

	// 先去缓存中看是否有这个block
	if sst.blockCache != nil {
		if cached := sst.blockCache.Get(sst.sstID, uint64(blockIdx)); cached != nil {
			return cached, nil
		}
	}

	// 计算这个block的大小
	meta := sst.metaEntries[blockIdx]
	var blockSize uint32
	if blockIdx == len(sst.metaEntries)-1 {
		// Last block: size = metaOffset - block offset
		blockSize = sst.metaOffset - meta.Offset
	} else {
		// Other blocks: size = next block offset - current block offset
		blockSize = sst.metaEntries[blockIdx+1].Offset - meta.Offset
	}

	// Read block data
	blockData := make([]byte, blockSize)
	_, err := sst.file.ReadAt(blockData, int64(meta.Offset))
	if err != nil {
		return nil, err
	}

	// Decode block
	blk, err := block.NewBlock(blockData)
	if err != nil {
		return nil, err
	}

	// Cache the block
	if sst.blockCache != nil {
		sst.blockCache.Put(sst.sstID, uint64(blockIdx), blk)
	}

	return blk, nil
}

// FindBlockIndex finds the block index that might contain the key
func (sst *SST) FindBlockIndex(key string) int {
	// First check bloom filter (only for keys that should be in the SST)
	if sst.bloomFilter != nil && key >= sst.firstKey && key <= sst.lastKey && !sst.bloomFilter.Contains(key) {
		return -1 // Key definitely not in this SST
	}

	// If key is after the last key, it's not in any block
	if key > sst.lastKey {
		return -1
	}

	// If key is before the first key, it should go to the first block
	if key < sst.firstKey {
		return 0
	}

	// Binary search through block metadata
	left, right := 0, len(sst.metaEntries)
	for left < right {
		mid := (left + right) / 2
		meta := sst.metaEntries[mid]

		if key < meta.FirstKey {
			right = mid
		} else if key > meta.LastKey {
			left = mid + 1
		} else {
			return mid // Found the block that contains this key range
		}
	}

	if left < len(sst.metaEntries) {
		return left
	}
	return -1
}

// FindBlockIdx is an alias for FindBlockIndex (test compatibility)
func (sst *SST) FindBlockIdx(key string) int {
	return sst.FindBlockIndex(key)
}

// Get 在SST中查找包含key的block，返回指向这个block的SST Iterator
func (sst *SST) Get(key string, txnID uint64) (iterator.Iterator, error) {
	blockIdx := sst.FindBlockIndex(key)
	if blockIdx == -1 {
		return iterator.NewEmptyIterator(), nil
	}

	blk, err := sst.ReadBlock(blockIdx)
	if err != nil {
		return nil, err
	}

	blockIter := blk.NewIterator()
	blockIter.Seek(key)

	// Return iterator positioned at the key
	return &SSTIterator{
		sst:       sst,
		blockIdx:  blockIdx,
		blockIter: blockIter,
		txnID:     txnID,
	}, nil
}

// NewIterator creates a new iterator for the SST
func (sst *SST) NewIterator(txnID uint64) iterator.Iterator {
	return &SSTIterator{
		sst:       sst,
		blockIdx:  0,
		blockIter: nil, // Will be initialized on first access
		txnID:     txnID,
	}
}

// Delete deletes the SST file
func (sst *SST) Delete() error {
	if sst.file != nil {
		sst.file.Close()
		sst.file = nil
	}
	return os.Remove(sst.filePath)
}

// SSTBuilder 帮助构建SST
type SSTBuilder struct {
	blockBuilder *block.BlockBuilder
	metas        []BlockMeta
	data         []byte
	blockSize    int
	bloomFilter  *utils.BloomFilter
	firstKey     string
	lastKey      string
	minTxnID     uint64
	maxTxnID     uint64
	hasBloom     bool
}

// NewSSTBuilder creates a new SST builder
func NewSSTBuilder(blockSize int, hasBloom bool) *SSTBuilder {
	var bloomFilter *utils.BloomFilter
	if hasBloom {
		cfg := config.GetGlobalConfig()
		bloomFilter = utils.NewBloomFilter(
			cfg.BloomFilter.ExpectedSize,
			cfg.BloomFilter.ExpectedErrorRate,
		)
	}

	return &SSTBuilder{
		blockBuilder: block.NewBlockBuilder(blockSize),
		metas:        make([]BlockMeta, 0),
		data:         make([]byte, 0),
		blockSize:    blockSize,
		bloomFilter:  bloomFilter,
		minTxnID:     ^uint64(0), // Max uint64
		maxTxnID:     0,
		hasBloom:     hasBloom,
	}
}

// Add 添加k-v对到SST
func (builder *SSTBuilder) Add(key, value string, txnID uint64) error {
	if builder.firstKey == "" {
		builder.firstKey = key
	}
	builder.minTxnID = min(builder.minTxnID, txnID)
	builder.maxTxnID = max(builder.maxTxnID, txnID)

	// 将k-v标记到布隆过滤器
	if builder.bloomFilter != nil {
		builder.bloomFilter.Add(key)
	}

	// todo:当key和lastKey不相等时，forceFlush=false，导致blockBuilder创建新block
	// 这个lastKey什么时候会更新，所以 key != lastKey 意味着什么，从而导致需要创建一个新的block？
	forceFlush := key == builder.lastKey
	// 尝试将k-v添加到当前block
	err := builder.blockBuilder.Add(key, value, txnID, forceFlush)
	if err == nil { // 成功添加
		builder.lastKey = key
		return nil
	}

	// 当前block装满，处理当前block，然后创建新的block
	err = builder.finishBlock()
	if err != nil {
		return err
	}
	builder.firstKey = key
	builder.lastKey = key
	return builder.blockBuilder.Add(key, value, txnID, false)
}

// finishBlock 构建SST中的一个block，结束后创建新的一个block
func (builder *SSTBuilder) finishBlock() error {
	if builder.blockBuilder.IsEmpty() {
		// 不需要构建block
		return nil
	}

	blk := builder.blockBuilder.Build() // 构建一个block
	blockData := blk.Data()

	// 为这个block创建元信息
	meta := NewBlockMeta(
		uint32(len(builder.data)),
		builder.blockBuilder.FirstKey(),
		builder.blockBuilder.LastKey(),
	)
	builder.metas = append(builder.metas, meta)

	// 添加block data到SSTBuilder中
	builder.data = append(builder.data, blockData...)

	// 创建新的block builder
	builder.blockBuilder = block.NewBlockBuilder(builder.blockSize)
	return nil
}

// Build 构建SST文件
// Format:
// | Block Section | Meta Section | Bloom Filter | Meta Offset(4) | Bloom Offset(4) | Min TxnID(8) | Max TxnID(8) |
func (builder *SSTBuilder) Build(sstID uint64, filePath string, blockCache *cache.BlockCache) (*SST, error) {
	// 把残余的数据搞到block里
	if !builder.blockBuilder.IsEmpty() {
		if err := builder.finishBlock(); err != nil {
			return nil, err
		}
	}
	// 没有block的元数据就表明没有block存在
	if len(builder.metas) == 0 {
		return nil, utils.ErrEmptySST
	}

	// 创建SST file
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// 按照SST 的格式依次写入file
	// 1. data
	_, err = file.Write(builder.data)
	if err != nil {
		os.Remove(filePath) // 写入失败，删除这个脏文件
		return nil, err
	}

	// 记录metadata的起始位置
	metaOffset := uint32(len(builder.data))
	//2. metadata
	metaData := EncodeBlockMetas(builder.metas)
	if _, err = file.Write(metaData); err != nil {
		os.Remove(filePath)
		return nil, err
	}

	// 记录 bloom filter 的起始位置
	bloomOffset := uint32(len(builder.data) + len(metaData))
	// Write bloom filter
	if builder.bloomFilter != nil {
		bloomData := builder.bloomFilter.Serialize() // 将过滤器内容序列化并写入
		_, err = file.Write(bloomData)
		if err != nil {
			os.Remove(filePath)
			return nil, err
		}
	}

	// 3. Write footer
	footer := make([]byte, 24)
	binary.LittleEndian.PutUint32(footer[0:4], metaOffset)
	binary.LittleEndian.PutUint32(footer[4:8], bloomOffset)
	binary.LittleEndian.PutUint64(footer[8:16], builder.minTxnID)
	binary.LittleEndian.PutUint64(footer[16:24], builder.maxTxnID)

	_, err = file.Write(footer)
	if err != nil {
		os.Remove(filePath)
		return nil, err
	}

	// Sync file 将文件内容刷进磁盘
	err = file.Sync()
	if err != nil {
		os.Remove(filePath)
		return nil, err
	}

	// Open the built SST
	return Open(sstID, filePath, blockCache)
}

// SSTIterator SST的迭代器，指向某一个block
type SSTIterator struct {
	sst       *SST
	blockIdx  int
	blockIter iterator.Iterator
	txnID     uint64
}

// Valid returns true if the iterator is pointing to a valid entry
func (iter *SSTIterator) Valid() bool {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		return true
	}
	return false
}

// Key 返回SST迭代器所指block中block迭代器所指的entry的key
func (iter *SSTIterator) Key() string {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		return iter.blockIter.Key()
	}
	return ""
}

// Value 返回SST迭代器所指block中block迭代器所指的entry的value
func (iter *SSTIterator) Value() string {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		return iter.blockIter.Value()
	}
	return ""
}

// TxnID 返回SST迭代器所指block中block迭代器的事务ID
func (iter *SSTIterator) TxnID() uint64 {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		return iter.blockIter.TxnID()
	}
	return 0
}

// IsDeleted 返回SST迭代器所指block中block迭代器所指的entry是否为空（被删除）
func (iter *SSTIterator) IsDeleted() bool {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		return iter.blockIter.IsDeleted()
	}
	return false
}

// Entry 返回SST迭代器所指block中block迭代器所指的entry
func (iter *SSTIterator) Entry() iterator.Entry {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		return iter.blockIter.Entry()
	}
	return iterator.Entry{}
}

// Next 移动SST迭代器指向下一个block
func (iter *SSTIterator) Next() {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		iter.blockIter.Next()
		if !iter.blockIter.Valid() {
			// Move to next block
			iter.moveToNextBlock()
		}
	}
}

// moveToNextBlock moves to the next block
func (iter *SSTIterator) moveToNextBlock() {
	iter.blockIdx++
	if iter.blockIdx >= iter.sst.NumBlocks() {
		// No more blocks
		if iter.blockIter != nil {
			iter.blockIter.Close()
			iter.blockIter = nil
		}
		return
	}

	// Load next block
	if iter.blockIter != nil {
		iter.blockIter.Close()
	}

	blk, err := iter.sst.ReadBlock(iter.blockIdx)
	if err != nil {
		iter.blockIter = nil
		return
	}

	iter.blockIter = blk.NewIterator()
	iter.blockIter.SeekToFirst()
}

// Seek positions the iterator at the first entry with key >= target
func (iter *SSTIterator) Seek(key string) bool {
	blockIdx := iter.sst.FindBlockIndex(key)
	if blockIdx == -1 {
		// Key not found in any block
		if iter.blockIter != nil {
			iter.blockIter.Close()
			iter.blockIter = nil
		}
		iter.blockIdx = iter.sst.NumBlocks()
		return false
	}

	// key 是在当前SST迭代器指向的block的范围中，那么先将block里面的迭代器关闭
	// 后面找到对应key的entry，并创建新的 block 迭代器指向这个entry
	iter.blockIdx = blockIdx
	if iter.blockIter != nil {
		iter.blockIter.Close()
	}
	//读取对应block
	blk, err := iter.sst.ReadBlock(iter.blockIdx)
	if err != nil {
		iter.blockIter = nil
		return false
	}

	iter.blockIter = blk.NewIterator()
	found := iter.blockIter.Seek(key) // 在这个block中找到key的entry，并将block迭代器指向这个entry

	// 虽然key这个block的范围，但这个key是不存在的
	if !iter.blockIter.Valid() {
		iter.moveToNextBlock()
	}

	return found
}

// SeekToFirst 移动迭代器到第一个block，同时将block迭代器移动到第一个entry
func (iter *SSTIterator) SeekToFirst() {
	if iter.sst.NumBlocks() == 0 {
		return
	}

	iter.blockIdx = 0
	if iter.blockIter != nil {
		iter.blockIter.Close()
	}

	blk, err := iter.sst.ReadBlock(0)
	if err != nil {
		iter.blockIter = nil
		return
	}

	iter.blockIter = blk.NewIterator()
	iter.blockIter.SeekToFirst()
}

// SeekToLast 移动迭代器到最后一个block，同时将block迭代器移动到最后一个entry
func (iter *SSTIterator) SeekToLast() {
	if iter.sst.NumBlocks() == 0 {
		return
	}

	iter.blockIdx = iter.sst.NumBlocks() - 1
	if iter.blockIter != nil {
		iter.blockIter.Close()
	}

	blk, err := iter.sst.ReadBlock(iter.blockIdx)
	if err != nil {
		iter.blockIter = nil
		return
	}

	iter.blockIter = blk.NewIterator()
	iter.blockIter.SeekToLast()
}

// GetType returns the iterator type
func (iter *SSTIterator) GetType() iterator.IteratorType {
	return iterator.SSTIteratorType
}

// Close releases resources held by the iterator
func (iter *SSTIterator) Close() {
	if iter.blockIter != nil {
		iter.blockIter.Close()
		iter.blockIter = nil
	}
}
