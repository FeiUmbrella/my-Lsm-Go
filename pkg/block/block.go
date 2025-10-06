package block

import (
	"encoding/binary"
	"fmt"
	"math"
	"myLsm-Go/pkg/iterator"
)

// Block SST文件的基本I/O单位
type Block struct {
	data      []byte           // raw block data, Format: [[Raw Entry 1]...[Raw Entry 2]]+[[offset 1]...[offset n]]+[entries_num:2]
	offsets   []uint16         // offsets of each kv pair
	entries   []iterator.Entry // 为了快速查找而缓存的entry
	blockSize int              // block 容量限制
}

// BlockBuilder 帮助构建block
type BlockBuilder struct {
	data      []byte // Raw Entry Format: [txnID:8B][keyLen:2B][key][valueLen:2B][value]
	offsets   []uint16
	blockSize int // block 容量限制
	firstKey  string
	lastKey   string
}

func NewBlockBuilder(blockSize int) *BlockBuilder {
	return &BlockBuilder{
		data:      make([]byte, 0),
		offsets:   make([]uint16, 0),
		blockSize: blockSize,
	}
}

// Add 先将 entry 添加到 blockbuilder中
// todo:这里的forceFlush参数是什么意思？
func (bb *BlockBuilder) Add(key, value string, txnID uint64, forceFlush bool) error {
	// Calculate the size needed for this entry
	entrySize := 8 + 2 + len(key) + 2 + len(value) // txnID + flags + keyLen + key + valueLen + value

	// Check if adding this entry would exceed the block size
	if !forceFlush && len(bb.data)+entrySize+len(bb.offsets)*2+2 > bb.blockSize && len(bb.offsets) > 0 {
		return fmt.Errorf("block size limit exceeded")
	}

	// Record the offset of this entry
	bb.offsets = append(bb.offsets, uint16(len(bb.data)))

	// Encode the entry
	// Format: [txnID:8][keyLen:2][key][valueLen:2][value]
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, txnID)
	bb.data = append(bb.data, buf...)

	// Key length and key
	binary.LittleEndian.PutUint16(buf[:2], uint16(len(key)))
	bb.data = append(bb.data, buf[:2]...)
	bb.data = append(bb.data, []byte(key)...)

	// Value length and value
	binary.LittleEndian.PutUint16(buf[:2], uint16(len(value)))
	bb.data = append(bb.data, buf[:2]...)
	bb.data = append(bb.data, []byte(value)...)

	// Update first and last keys
	if bb.firstKey == "" {
		bb.firstKey = key
	}
	bb.lastKey = key

	return nil
}

// EstimatedSize returns the estimated size of the current block
func (bb *BlockBuilder) EstimatedSize() int {
	return len(bb.data) + len(bb.offsets)*2 + 2 // data + offsets + offset count
}

func (bb *BlockBuilder) DataSize() int {
	return len(bb.data)
}

// IsEmpty returns true if the block builder is empty
func (bb *BlockBuilder) IsEmpty() bool {
	return len(bb.offsets) == 0
}

// FirstKey returns the first key in the block
func (bb *BlockBuilder) FirstKey() string {
	return bb.firstKey
}

// LastKey returns the last key in the block
func (bb *BlockBuilder) LastKey() string {
	return bb.lastKey
}

// Build 返回一个block
func (bb *BlockBuilder) Build() *Block {
	if len(bb.offsets) == 0 {
		// Empty block: just the number of entries (0)
		emptyData := make([]byte, 2)
		binary.LittleEndian.PutUint16(emptyData, 0) // 0 entries
		return &Block{
			data:      emptyData,
			offsets:   []uint16{},
			entries:   []iterator.Entry{},
			blockSize: bb.blockSize,
		}
	}

	// Build the final data with offsets at the end
	finalData := make([]byte, 0, len(bb.data)+len(bb.offsets)*2+2) // format: [entries]+[offsets]+[entriesNum]
	finalData = append(finalData, bb.data...)

	// Append offsets
	for _, offset := range bb.offsets {
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, offset)
		finalData = append(finalData, buf...)
	}

	// Append number of entries
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, uint16(len(bb.offsets)))
	finalData = append(finalData, buf...)

	// Parse entries for caching
	entries, _ := parseEntries(finalData, bb.offsets)

	return &Block{
		data:      finalData,
		offsets:   bb.offsets,
		entries:   entries,
		blockSize: bb.blockSize,
	}
}

// parseEntries 根据 offsets 将raw data 解码为 entry
func parseEntries(data []byte, offsets []uint16) ([]iterator.Entry, error) {
	entries := make([]iterator.Entry, len(offsets))

	// 拿到每个 entry 的起始位置 offset
	for i, offset := range offsets {
		if int(offset)+8+1+2 > len(data) {
			return nil, fmt.Errorf("invalid offset")
		}

		pos := int(offset)

		// Read transaction ID
		txnID := binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8

		// Read key length and key
		keyLen := binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		if pos+int(keyLen) > len(data) {
			return nil, fmt.Errorf("invalid key length")
		}

		key := string(data[pos : pos+int(keyLen)])
		pos += int(keyLen)

		// Read value length and value
		if pos+2 > len(data) {
			return nil, fmt.Errorf("invalid value length position")
		}

		valueLen := binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		if pos+int(valueLen) > len(data) {
			return nil, fmt.Errorf("invalid value length")
		}

		value := string(data[pos : pos+int(valueLen)])

		entries[i] = iterator.Entry{
			Key:   key,
			Value: value,
			TxnID: txnID,
		}
	}

	return entries, nil
}

// NewBlock 通过 raw data 创建 block
func NewBlock(data []byte) (*Block, error) {
	// 最后两个B是entry 的个数，是固定字段，当小于2B表明这个 raw data 非法
	// byte = uint8
	if len(data) < 2 {
		return nil, fmt.Errorf("invalid block data: too short")
	}

	// 拿到entry个数
	numEntries := binary.LittleEndian.Uint16(data[len(data)-2:])
	// 判断长度是否满足offsets+entryNum
	if len(data) < int(numEntries)*2+2 {
		return nil, fmt.Errorf("invalid block data: insufficient data for offsets")
	}

	// 读取offsets，每个offset是2B固定长度
	offsets := make([]uint16, numEntries)
	offsetStart := len(data) - 2 - int(numEntries)*2

	for i := 0; i < int(numEntries); i++ {
		offset := offsetStart + i*2
		offsets[i] = binary.LittleEndian.Uint16(data[offset : offset+2])
	}

	// Parse entries
	actualData := data[:offsetStart]
	entries, err := parseEntries(actualData, offsets)
	if err != nil {
		return nil, err
	}

	return &Block{
		data:    data,
		offsets: offsets,
		entries: entries,
	}, nil
}

// NumEntries returns the number of entries in the block
func (b *Block) NumEntries() int {
	return len(b.offsets)
}

// GetEntry returns the entry at the specified index
func (b *Block) GetEntry(index int) (iterator.Entry, error) {
	if index < 0 || index >= len(b.entries) {
		return iterator.Entry{}, fmt.Errorf("index out of range")
	}
	return b.entries[index], nil
}

// Data returns the raw block data
func (b *Block) Data() []byte {
	return b.data
}

// Size returns the size of the block in bytes
func (b *Block) Size() int {
	return len(b.data)
}

// FirstKey returns the first key in the block
func (b *Block) FirstKey() string {
	if len(b.entries) == 0 {
		return ""
	}
	return b.entries[0].Key
}

// LastKey returns the last key in the block
func (b *Block) LastKey() string {
	if len(b.entries) == 0 {
		return ""
	}
	return b.entries[len(b.entries)-1].Key
}

// FindEntry finds the index of the first entry with key >= target
// Returns -1 if no such entry exists
func (b *Block) FindEntry(key string) int {
	// Binary search
	left, right := 0, len(b.entries)

	for left < right {
		mid := (left + right) / 2
		if iterator.CompareKeys(b.entries[mid].Key, key) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}

	if left >= len(b.entries) {
		return -1
	}

	return left
}

// GetNumEntries returns the number of entries in the block (alias for NumEntries)
func (b *Block) GetNumEntries() int {
	return len(b.offsets)
}

// GetValue searches for a key and returns its value with MVCC support
// Returns (value, found, error)
func (b *Block) GetValue(key string, txnID uint64) (string, bool) {
	// Binary search
	left, right := 0, len(b.entries)-1
	if txnID == 0 {
		txnID = math.MaxUint64
	} // txnID=0:找最大的txn，txnID!=0:找<=txnID最大的。转化为一种情况
	for left < right {
		mid := (left + right + 1) / 2
		if b.entries[mid].Key < key || (b.entries[mid].Key == key && b.entries[mid].TxnID <= txnID) {
			left = mid
		} else {
			right = mid - 1
		}
	}

	if left >= len(b.entries) {
		return "", false
	}
	return b.entries[left].Value, true
}

// NewIterator creates a new iterator for this block
func (b *Block) NewIterator() iterator.Iterator {
	return NewBlockIterator(b)
}
