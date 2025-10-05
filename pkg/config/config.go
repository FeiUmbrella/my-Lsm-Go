package config

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"sync"
)

// Config represents the configuration for the LSM-tree storage engine
type Config struct {
	// LSM Core Configuration
	LSM struct {
		Core struct {
			// Total memory table size limit (bytes)
			TotalMemSizeLimit int64 `toml:"LSM_TOL_MEM_SIZE_LIMIT"`
			// Per-memory table size limit (bytes)
			PerMemSizeLimit int64 `toml:"LSM_PER_MEM_SIZE_LIMIT"`
			// Block size for SST files (bytes)
			BlockSize int `toml:"LSM_BLOCK_SIZE"`
			// SST level size ratio
			SSTLevelRatio int `toml:"LSM_SST_LEVEL_RATIO"`
		} `toml:"core"`

		Cache struct {
			// Block cache capacity (number of blocks)
			BlockCacheCapacity int `toml:"LSM_BLOCK_CACHE_CAPACITY"`
			// LRU-K K value for cache
			BlockCacheK int `toml:"LSM_BLOCK_CACHE_K"`
		} `toml:"cache"`
	} `toml:"lsm"`

	mu sync.RWMutex
}

var (
	globalConfig *Config // 全局的配置信息实例
	configOnce   sync.Once
)

// DefaultConfig 返回具有默认值的 config
func DefaultConfig() *Config {
	return &Config{
		LSM: struct {
			Core struct {
				TotalMemSizeLimit int64 `toml:"LSM_TOL_MEM_SIZE_LIMIT"`
				PerMemSizeLimit   int64 `toml:"LSM_PER_MEM_SIZE_LIMIT"`
				BlockSize         int   `toml:"LSM_BLOCK_SIZE"`
				SSTLevelRatio     int   `toml:"LSM_SST_LEVEL_RATIO"`
			} `toml:"core"`
			Cache struct {
				BlockCacheCapacity int `toml:"LSM_BLOCK_CACHE_CAPACITY"`
				BlockCacheK        int `toml:"LSM_BLOCK_CACHE_K"`
			} `toml:"cache"`
		}{
			Core: struct {
				TotalMemSizeLimit int64 `toml:"LSM_TOL_MEM_SIZE_LIMIT"`
				PerMemSizeLimit   int64 `toml:"LSM_PER_MEM_SIZE_LIMIT"`
				BlockSize         int   `toml:"LSM_BLOCK_SIZE"`
				SSTLevelRatio     int   `toml:"LSM_SST_LEVEL_RATIO"`
			}{
				TotalMemSizeLimit: 64 * 1024 * 1024, // 64MB
				PerMemSizeLimit:   4 * 1024 * 1024,  // 4MB
				BlockSize:         32 * 1024,        // 32KB
				SSTLevelRatio:     4,
			},
			Cache: struct {
				BlockCacheCapacity int `toml:"LSM_BLOCK_CACHE_CAPACITY"`
				BlockCacheK        int `toml:"LSM_BLOCK_CACHE_K"`
			}{
				BlockCacheCapacity: 1024,
				BlockCacheK:        8,
			},
		},
	}
}

// LoadFromFile 加载配置信息从TOML文件
func LoadFromFile(filename string) (*Config, error) {
	config := DefaultConfig()

	if _, err := toml.DecodeFile(filename, config); err != nil {
		return nil, fmt.Errorf("failed to decode config file %s: %w", filename, err)
	}

	return config, nil
}

// LoadFromString loads configuration from a TOML string
func LoadFromString(data string) (*Config, error) {
	config := DefaultConfig()

	if _, err := toml.Decode(data, config); err != nil {
		return nil, fmt.Errorf("failed to decode config string: %w", err)
	}

	return config, nil
}

// GetGlobalConfig returns the global configuration instance
func GetGlobalConfig() *Config {
	configOnce.Do(func() {
		globalConfig = DefaultConfig()
	})
	return globalConfig
}

// SetGlobalConfig sets the global configuration instance
func SetGlobalConfig(config *Config) {
	globalConfig = config
}

// InitGlobalConfig initializes the global configuration from a file
func InitGlobalConfig(filename string) error {
	config, err := LoadFromFile(filename)
	if err != nil {
		return err
	}

	SetGlobalConfig(config)
	return nil
}

// GetPerMemSizeLimit returns the per-memory table size limit
func (c *Config) GetPerMemSizeLimit() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.LSM.Core.PerMemSizeLimit
}

// GetTotalMemSizeLimit returns the total memory size limit
func (c *Config) GetTotalMemSizeLimit() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.LSM.Core.TotalMemSizeLimit
}
