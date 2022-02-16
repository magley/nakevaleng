package coreconf

import (
	"fmt"
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

type CoreConfig struct {
	Path    string `yaml:"path"`
	WalPath string `yaml:"wal_path"`
	DBName  string `yaml:"db_name"`

	SkiplistLevel       int   `yaml:"skiplist_level"`
	SkiplistLevelMax    int   `yaml:"skiplist_level_max"`
	MemtableCapacity    int   `yaml:"memtable_capacity"`
	CacheCapacity       int   `yaml:"cache_capacity"`
	SummaryPageSize     int   `yaml:"summary_page_size"`
	LsmLvlMax           int   `yaml:"lsm_lvl_max"`
	LsmRunMax           int   `yaml:"lsm_run_max"`
	TokenBucketTokens   int   `yaml:"token_bucket_tokens"`
	TokenBucketInterval int64 `yaml:"token_bucket_interval"`
	WalMaxRecsInSeg     int   `yaml:"wal_max_recs_in_seg"`
	WalLwmIdx           int   `yaml:"wal_lwm_idx"`
	WalBufferCapacity   int   `yaml:"wal_buffer_capacity"`

	InternalStart string `yaml:"internal_start"`
}

func LoadConfig(filePath string) CoreConfig {
	var config CoreConfig
	config.Path = "data/"
	config.WalPath = "data/log/"
	config.DBName = "nakevaleng"
	config.SkiplistLevel = 3
	config.SkiplistLevelMax = 5
	config.MemtableCapacity = 10
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

func main() {
	config := LoadConfig("conf.yaml")
	fmt.Println(config)
	//config.Dump("confDUMP.yaml")
}
