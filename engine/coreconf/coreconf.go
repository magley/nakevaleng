// Package coreconf implements a configuration structure used for supplying the
// entire program with valid parameters.
package coreconf

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"nakevaleng/core/lru"
	"nakevaleng/core/lsmtree"
	"nakevaleng/core/skiplist"
	"nakevaleng/core/wal"
	"nakevaleng/ds/tokenbucket"
	"os"
	"strconv"

	"gopkg.in/yaml.v2"
)

const (
	_FLUSH_CAPACITY  = 1 << 0
	_FLUSH_THRESHOLD = 1 << 1
)

// Default values for the configuration.
const (
	PATH                    = "data/"
	WAL_PATH                = "data/log/"
	DBNAME                  = "nakevaleng"
	SKIPLIST_LVL            = 3
	SKIPLIST_LVL_MAX        = 5
	MEMTABLE_CAPACITY       = 10
	MEMTABLE_THRESHOLD      = "2 KB" // The space is important!
	MEMTABLE_FLUSH_STRATEGY = _FLUSH_CAPACITY | _FLUSH_THRESHOLD
	CACHE_CAPACITY          = 5
	SUMMARY_PAGE_SIZE       = 3
	LSM_LVL_MAX             = 4
	LSM_RUN_MAX             = 4
	TOKENBUCKET_TOKENS      = 100
	TOKENBUCKET_INTERVAL    = 1
	WAL_MAX_RECS_IN_SEG     = 5
	WAL_LWM_IDX             = 2
	WAL_BUFFER_CAPACITY     = 5
	INTERNAL_START          = "$"
)

// CoreConfig is a data structure storing all modifiable-on-disk settings for the database engine.
type CoreConfig struct {
	Path    string `yaml:"path"`
	WalPath string `yaml:"wal_path"`
	DBName  string `yaml:"db_name"`

	SkiplistLevel         int    `yaml:"skiplist_level"`
	SkiplistLevelMax      int    `yaml:"skiplist_level_max"`
	MemtableCapacity      int    `yaml:"memtable_capacity"`
	MemtableThreshold     string `yaml:"memtable_threshold"`
	MemtableFlushStrategy int    `yaml:"memtable_flush_strategy"`
	CacheCapacity         int    `yaml:"cache_capacity"`
	SummaryPageSize       int    `yaml:"summary_page_size"`
	LsmLvlMax             int    `yaml:"lsm_lvl_max"`
	LsmRunMax             int    `yaml:"lsm_run_max"`
	TokenBucketTokens     int    `yaml:"token_bucket_tokens"`
	TokenBucketInterval   int64  `yaml:"token_bucket_interval"`
	WalMaxRecsInSeg       int    `yaml:"wal_max_recs_in_seg"`
	WalLwmIdx             int    `yaml:"wal_lwm_idx"`
	WalBufferCapacity     int    `yaml:"wal_buffer_capacity"`

	InternalStart string `yaml:"internal_start"`
}

// ShouldFlushByCapacity returns whether or not the Memtable should flush
// by capacity.
func (conf CoreConfig) ShouldFlushByCapacity() bool {
	return (conf.MemtableFlushStrategy & _FLUSH_CAPACITY) != 0
}

// ShouldFlushByThreshold returns whether or not the Memtable should flush
// by threshold.
func (conf CoreConfig) ShouldFlushByThreshold() bool {
	return (conf.MemtableFlushStrategy & _FLUSH_THRESHOLD) != 0
}

// GetDefault returns a config object with the default values for all parameters.
func GetDefault() CoreConfig {
	var config CoreConfig
	config.Path = PATH
	config.WalPath = WAL_PATH
	config.DBName = DBNAME
	config.SkiplistLevel = SKIPLIST_LVL
	config.SkiplistLevelMax = SKIPLIST_LVL_MAX
	config.MemtableCapacity = MEMTABLE_CAPACITY
	config.MemtableThreshold = MEMTABLE_THRESHOLD
	config.MemtableFlushStrategy = MEMTABLE_FLUSH_STRATEGY
	config.CacheCapacity = CACHE_CAPACITY
	config.SummaryPageSize = SUMMARY_PAGE_SIZE
	config.LsmLvlMax = LSM_LVL_MAX
	config.LsmRunMax = LSM_RUN_MAX
	config.TokenBucketTokens = TOKENBUCKET_TOKENS
	config.TokenBucketInterval = TOKENBUCKET_INTERVAL
	config.WalMaxRecsInSeg = WAL_MAX_RECS_IN_SEG
	config.WalLwmIdx = WAL_LWM_IDX
	config.WalBufferCapacity = WAL_BUFFER_CAPACITY
	config.InternalStart = INTERNAL_START
	return config
}

// LoadConfig reads the YAML file at filePath and returns a config object and
// an error indicating whether or not the config object was made successfully.
func LoadConfig(filePath string) (*CoreConfig, error) {
	config := GetDefault()

	configData, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Println("Config file at", filePath, "is not available for reading. Using defaults")
	} else {
		err = yaml.UnmarshalStrict(configData, &config)
		if err != nil {
			log.Println("Config file at", filePath, "is not valid. Using defaults. Error is:\n", err)
		} else {
			err := config.validate()
			if err != nil {
				return nil, err
			}
		}
	}

	return &config, nil
}

func (conf *CoreConfig) validate() error {
	err := os.MkdirAll(conf.Path, 0777)
	if err != nil {
		err := fmt.Errorf("path \"%s\" is not valid", conf.Path)
		return err
	}

	err = os.MkdirAll(conf.WalPath, 0777)
	if err != nil {
		err := fmt.Errorf("path \"%s\" is not valid", conf.WalPath)
		return err
	}

	err = skiplist.ValidateParams(conf.SkiplistLevel, conf.SkiplistLevelMax)
	if err != nil {
		err := fmt.Errorf("skiplist config: %s", err.Error())
		return err
	}

	if conf.MemtableCapacity <= 0 {
		err := fmt.Errorf("memtable config: capacity must be a positive number, but %d was given", conf.MemtableCapacity)
		return err
	}

	err = lru.ValidateParams(conf.CacheCapacity)
	if err != nil {
		err := fmt.Errorf("lru config: %s", err.Error())
		return err
	}

	err = lsmtree.ValidateParams(conf.SummaryPageSize, 1, conf.LsmLvlMax, conf.LsmRunMax)
	if err != nil {
		err := fmt.Errorf("lsm config: %s", err.Error())
		return err
	}

	err = tokenbucket.ValidateParams(conf.TokenBucketTokens, conf.TokenBucketInterval)
	if err != nil {
		err := fmt.Errorf("tokenbucket config: %s", err.Error())
		return err
	}

	err = wal.ValidateParams(conf.WalMaxRecsInSeg, conf.WalLwmIdx, conf.WalBufferCapacity)
	if err != nil {
		err := fmt.Errorf("wal config: %s", err.Error())
		return err
	}

	if conf.InternalStart == "" {
		return errors.New("internal start cannot be an empty string")
	}

	return nil
}

// Dump writes the config object to filePath.
func (conf CoreConfig) Dump(filePath string) {
	configData, err := yaml.Marshal(conf)
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile(filePath, configData, 0644)
	if err != nil {
		log.Println("Can't dump at", filePath)
	}
}

// MemtableThresholdBytes parses the config's memtable threshold
// parameter and returns it as an uint64.
func (conf *CoreConfig) MemtableThresholdBytes() (uint64, error) {
	isNum := func(s rune) bool {
		return s >= '0' && s <= '9'
	}

	// Parse

	strBucket := 0
	parts := [2]string{
		"", // Number
		"", // Unit of memory
	}

	for _, ch := range conf.MemtableThreshold {
		if ch == ' ' {
			continue
		}
		if strBucket == 0 && !isNum(ch) {
			strBucket = 1
		}

		parts[strBucket] += string(ch)
	}

	// Parse

	// How many units
	howMany, err := strconv.Atoi(parts[0])
	if err != nil {
		conf.MemtableThreshold = GetDefault().MemtableThreshold
		fallback, _ := conf.MemtableThresholdBytes()
		return fallback, err
	}
	unit := parts[1]

	// Each unit gets an exponent
	exponent := make(map[string]int)
	exponent["B"] = 0
	exponent["KB"] = 1
	exponent["MB"] = 2
	exponent["GB"] = 3

	// Bad unit
	exp, ok := exponent[unit]
	if !ok {
		conf.MemtableThreshold = GetDefault().MemtableThreshold
		fallback, _ := conf.MemtableThresholdBytes()
		return fallback, fmt.Errorf("Bad unit: %s", unit)
	}

	// Convert to bytes
	m := uint64(1)
	for i := 0; i < exp; i++ {
		m *= 1024
	}

	return uint64(howMany) * m, nil
}
