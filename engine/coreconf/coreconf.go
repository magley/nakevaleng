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
	"strconv"
	"strings"

	"gopkg.in/yaml.v2"
)

const (
	_FLUSH_CAPACITY  = 1 << 0
	_FLUSH_THRESHOLD = 1 << 1
)

// default values
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

func (cfg CoreConfig) ShouldFlushByCapacity() bool {
	return (cfg.MemtableFlushStrategy & _FLUSH_CAPACITY) != 0
}

func (cfg CoreConfig) ShouldFlushByThreshold() bool {
	return (cfg.MemtableFlushStrategy & _FLUSH_THRESHOLD) != 0
}

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

func (core *CoreConfig) validate() error {
	err := skiplist.ValidateParams(core.SkiplistLevel, core.SkiplistLevelMax)
	if err != nil {
		err := fmt.Errorf("skiplist config: %s", err.Error())
		return err
	}

	if core.MemtableCapacity <= 0 {
		err := fmt.Errorf("memtable config: capacity must be a positive number, but %d was given", core.MemtableCapacity)
		return err
	}

	err = lru.ValidateParams(core.CacheCapacity)
	if err != nil {
		err := fmt.Errorf("lru config: %s", err.Error())
		return err
	}

	err = lsmtree.ValidateParams(core.SummaryPageSize, 1, core.LsmLvlMax, core.LsmRunMax)
	if err != nil {
		err := fmt.Errorf("lsm config: %s", err.Error())
		return err
	}

	err = tokenbucket.ValidateParams(core.TokenBucketTokens, core.TokenBucketInterval)
	if err != nil {
		err := fmt.Errorf("tokenbucket config: %s", err.Error())
		return err
	}

	err = wal.ValidateParams(core.WalMaxRecsInSeg, core.WalLwmIdx, core.WalBufferCapacity)
	if err != nil {
		err := fmt.Errorf("wal config: %s", err.Error())
		return err
	}

	if core.InternalStart == "" {
		return errors.New("internal start cannot be an empty string")
	}

	return nil
}

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

func (cfg *CoreConfig) MemtableThresholdBytes() uint64 {
	// Parse
	parts := strings.Split(cfg.MemtableThreshold, " ")
	if len(parts) != 2 {
		cfg.MemtableThreshold = GetDefault().MemtableThreshold
		return cfg.MemtableThresholdBytes()
	}

	// How many units
	howMany, err := strconv.Atoi(parts[0])
	if err != nil {
		panic(err)
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
		cfg.MemtableThreshold = GetDefault().MemtableThreshold
		return cfg.MemtableThresholdBytes()
	}

	// Convert to bytes
	m := uint64(1)
	for i := 0; i < exp; i++ {
		m *= 1024
	}
	return uint64(howMany) * m
}
