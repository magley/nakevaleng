package coreconf

import (
	"io/ioutil"
	"log"
	"strconv"
	"strings"

	"gopkg.in/yaml.v2"
)

const (
	_FLUSH_CAPACITY  = 1 << 0
	_FLUSH_THRESHOLD = 1 << 1
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
	config.Path = "data/"
	config.WalPath = "data/log/"
	config.DBName = "nakevaleng"
	config.SkiplistLevel = 3
	config.SkiplistLevelMax = 5
	config.MemtableCapacity = 10
	config.MemtableThreshold = "2 KB" // The space is important!
	config.MemtableFlushStrategy = _FLUSH_CAPACITY | _FLUSH_THRESHOLD
	config.CacheCapacity = 5
	config.SummaryPageSize = 3
	config.LsmLvlMax = 4
	config.LsmRunMax = 4
	config.TokenBucketTokens = 100
	config.TokenBucketInterval = 1
	config.WalMaxRecsInSeg = 5
	config.WalLwmIdx = 2
	config.WalBufferCapacity = 5
	config.InternalStart = "$"
	return config
}

func LoadConfig(filePath string) CoreConfig {
	config := GetDefault()

	configData, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Println("Config file at", filePath, "is not available for reading. Using defaults")
	} else {
		err = yaml.UnmarshalStrict(configData, &config)
		if err != nil {
			log.Println("Config file at", filePath, "is not valid. Using defaults. Error is:\n", err)
		}
	}
	return config
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
