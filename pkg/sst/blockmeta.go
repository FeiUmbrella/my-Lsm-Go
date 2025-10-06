package sst

import (
	"encoding/binary"
	"hash/crc32"
	"myLsm-Go/pkg/utils"
)

// BlockMeta 是一个block在sst文件中的元信息
type BlockMeta struct {
	Offset   uint32 // Block offset in the SST file
	FirstKey string // First key in the block
	LastKey  string // Last key in the block
}

func NewBlockMeta(offset uint32, firstKey, lastKey string) BlockMeta {
	return BlockMeta{offset, firstKey, lastKey}
}

// EncodeBlockMetas 将BlockMeta切片编码为二进制
// 每个BlockMeta的格式为:
// | offset(4) | first_key_len(2) | first_key | last_key_len(2) | last_key |
// 最终二进制数据格式为:
// | num_entries(4) | meta_entry1 | ... | meta_entryN | checksum(4) |
// checksum 是完整性校验位
func EncodeBlockMetas(metas []BlockMeta) []byte {
	if len(metas) == 0 {
		res := make([]byte, 8) // 4 for count + 4 for checksum
		binary.LittleEndian.PutUint32(res[:4], 0)
		checksum := crc32.ChecksumIEEE(res[:4])
		binary.LittleEndian.PutUint32(res[4:], checksum)
		return res
	}

	// 计算需要的总字节数
	totalSize := 4 // num_entries
	for _, meta := range metas {
		totalSize += 4
		totalSize += 2 + len(meta.FirstKey)
		totalSize += 2 + len(meta.LastKey)
	}
	totalSize += 4 // checksum

	res := make([]byte, totalSize)
	pos := 0

	binary.LittleEndian.PutUint32(res[pos:pos+4], uint32(len(metas)))
	pos += 4

	// 将每个blockMeta写入
	for _, meta := range metas {
		// Write offset
		binary.LittleEndian.PutUint32(res[pos:pos+4], meta.Offset)
		pos += 4

		// Write first key length and first key
		firstKeyLen := uint16(len(meta.FirstKey))
		binary.LittleEndian.PutUint16(res[pos:pos+2], firstKeyLen)
		pos += 2
		copy(res[pos:pos+len(meta.FirstKey)], meta.FirstKey)
		pos += len(meta.FirstKey)

		// Write last key length and last key
		lastKeyLen := uint16(len(meta.LastKey))
		binary.LittleEndian.PutUint16(res[pos:pos+2], lastKeyLen)
		pos += 2
		copy(res[pos:pos+len(meta.LastKey)], meta.LastKey)
		pos += len(meta.LastKey)
	}
	// Calculate checksum of the metadata (excluding the checksum itself)
	checksum := crc32.ChecksumIEEE(res[0:pos])
	binary.LittleEndian.PutUint32(res[pos:pos+4], checksum)

	return res
}

// DecodeBlockMetas 从二进制数据解码出blockMeta
func DecodeBlockMetas(data []byte) ([]BlockMeta, error) {
	if len(data) < 8 {
		return nil, utils.ErrInvalidMetadata
	}

	pos := 0

	// Read number of entries
	numEntries := binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4

	if numEntries == 0 {
		// Verify checksum for empty metadata
		expectedChecksum := binary.LittleEndian.Uint32(data[4:8])
		actualChecksum := crc32.ChecksumIEEE(data[0:4])
		if expectedChecksum != actualChecksum {
			return nil, utils.ErrInvalidChecksum
		}
		return []BlockMeta{}, nil
	}

	metas := make([]BlockMeta, numEntries)

	// Read each meta entry
	for i := uint32(0); i < numEntries; i++ {
		if pos+4 > len(data) {
			return nil, utils.ErrInvalidMetadata
		}

		// Read offset
		offset := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4

		// Read first key
		if pos+2 > len(data) {
			return nil, utils.ErrInvalidMetadata
		}
		firstKeyLen := binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		if pos+int(firstKeyLen) > len(data) {
			return nil, utils.ErrInvalidMetadata
		}
		firstKey := string(data[pos : pos+int(firstKeyLen)])
		pos += int(firstKeyLen)

		// Read last key
		if pos+2 > len(data) {
			return nil, utils.ErrInvalidMetadata
		}
		lastKeyLen := binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		if pos+int(lastKeyLen) > len(data) {
			return nil, utils.ErrInvalidMetadata
		}
		lastKey := string(data[pos : pos+int(lastKeyLen)])
		pos += int(lastKeyLen)

		metas[i] = BlockMeta{
			Offset:   offset,
			FirstKey: firstKey,
			LastKey:  lastKey,
		}
	}

	// Verify checksum
	if pos+4 > len(data) {
		return nil, utils.ErrInvalidMetadata
	}
	expectedChecksum := binary.LittleEndian.Uint32(data[pos : pos+4])
	actualChecksum := crc32.ChecksumIEEE(data[0:pos])
	if expectedChecksum != actualChecksum {
		return nil, utils.ErrInvalidChecksum
	}

	return metas, nil
}

// Size 返回 BlockMeta 编码后二进制数据的长度即字节数
func (bm *BlockMeta) Size() int {
	return 4 + 2 + len(bm.FirstKey) + 2 + len(bm.LastKey)
}

// Contains checks if a key falls within this block's range
func (bm *BlockMeta) Contains(key string) bool {
	return key >= bm.FirstKey && key <= bm.LastKey
}
