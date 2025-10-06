package utils

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// BloomFilter 布隆过滤器
type BloomFilter struct {
	bitset    []byte
	size      uint32 // size of the bit array
	numHashes uint32 // number of hash functions
	numItems  uint32 // number of items added
}

// NewBloomFilter creates a new Bloom filter with the specified parameters
func NewBloomFilter(expectedItems int, falsePositiveRate float64) *BloomFilter {
	if expectedItems <= 0 {
		expectedItems = 1000
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		falsePositiveRate = 0.01
	}

	// Calculate optimal bit array size and number of hash functions
	size := uint32(-float64(expectedItems) * math.Log(falsePositiveRate) / (math.Log(2) * math.Log(2)))
	numHashes := uint32(float64(size) * math.Log(2) / float64(expectedItems))

	// Ensure minimum values
	if size == 0 {
		size = 1
	}
	if numHashes == 0 {
		numHashes = 1
	}

	// Round up to the nearest byte
	sizeBytes := (size + 7) / 8

	return &BloomFilter{
		bitset:    make([]byte, sizeBytes),
		size:      size,
		numHashes: numHashes,
		numItems:  0,
	}
}

// NewBloomFilterFromData creates a Bloom filter from existing data
func NewBloomFilterFromData(data []byte, numHashes uint32) *BloomFilter {
	return &BloomFilter{
		bitset:    data,
		size:      uint32(len(data) * 8),
		numHashes: numHashes,
		numItems:  0, // We don't track this for deserialized filters
	}
}

// Contains 检查key是否可能存在过滤器中
// 如果key绝对不存在的话返回 false
// 如果key可能存在的话返回 true
func (bf *BloomFilter) Contains(key string) bool {
	hashes := bf.getHashes([]byte(key))

	for i := uint32(0); i < bf.numHashes; i++ {
		bit := hashes[i] % bf.size
		byteIndex := bit / 8
		bitIndex := bit % 8

		if (bf.bitset[byteIndex] & (1 << bitIndex)) == 0 {
			return false
		}
	}

	return true
}

// Add 添加一个key到过滤器中
func (bf *BloomFilter) Add(key string) {
	hashes := bf.getHashes([]byte(key))

	for i := range bf.numHashes {
		bit := hashes[i] % bf.size // 取hash值的后size位
		// bit = 15 表明要将下标为14的bit置1，这个bit在第2个Byte中的第7个bit
		byteIndex := bit / 8
		bitIndex := bit % 8
		bf.bitset[byteIndex] |= 1 << bitIndex
	}
	bf.numItems++
}

func (bf *BloomFilter) getHashes(key []byte) []uint32 {
	h1, h2 := bf.hash1(key), bf.hash2(key)
	hashes := make([]uint32, bf.numHashes)
	for i := range hashes {
		hashes[i] = h1 + (uint32(i) * h2)
	}
	return hashes
}

// hash1 哈希函数1
// fnv-1a(Fowler-Noll-Vo):是一个快速、非加密的哈希函数,具有良好的分布特性
func (bf *BloomFilter) hash1(key []byte) uint32 {
	h := fnv.New32a() // 创建FNV-1a哈希器
	h.Write(key)      // 写入要哈希的数据
	return h.Sum32()  // 返回32位哈希值
}

// hash2 哈希函数2
// hash = hash * 33 + byte
func (bf *BloomFilter) hash2(key []byte) uint32 {
	var hash uint32 = 5318 // 初始种子
	for _, b := range key {
		hash = ((hash << 5) + hash) + uint32(b)
	}
	return hash
}

// Serialize 将当前布隆过滤器的内容序列化并返回
// Format: [size:4][numHashes:4][numItems:4][bitset...]
func (bf *BloomFilter) Serialize() []byte {
	result := make([]byte, 12+len(bf.bitset))

	binary.LittleEndian.PutUint32(result[0:4], bf.size)
	binary.LittleEndian.PutUint32(result[4:8], bf.numHashes)
	binary.LittleEndian.PutUint32(result[8:12], bf.numItems)
	copy(result[12:], bf.bitset)

	return result
}

// DeserializeBloomFilter 将二进制数据反序列化为布隆过滤器内容
func DeserializeBloomFilter(data []byte) *BloomFilter {
	if len(data) < 12 {
		return nil
	}

	size := binary.LittleEndian.Uint32(data[0:4])
	numHashes := binary.LittleEndian.Uint32(data[4:8])
	numItems := binary.LittleEndian.Uint32(data[8:12])
	bitset := make([]byte, len(data)-12)
	copy(bitset, data[12:])

	return &BloomFilter{
		bitset:    bitset,
		size:      size,
		numHashes: numHashes,
		numItems:  numItems,
	}
}
