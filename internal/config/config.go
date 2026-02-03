package config

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

type BloomConfig struct {
	Bits          uint64        `yaml:"bits"`
	Hashes        uint32        `yaml:"hashes"`
	Windows       int           `yaml:"windows"`
	WindowSeconds time.Duration `yaml:"window_seconds"`
}

type DedupConfig struct {
	BadgerValueLogFileSize int64         `yaml:"badger_value_log_file_size"`
	GCInterval             time.Duration `yaml:"gc_interval"`
}

type RetryConfig struct {
	Initial time.Duration `yaml:"initial"`
	Max     time.Duration `yaml:"max"`
	Factor  float64       `yaml:"factor"`
	Jitter  float64       `yaml:"jitter"`
}

type Config struct {
	LogListURL        string        `yaml:"log_list_url"`
	DataDir           string        `yaml:"data_dir"`
	RequestTimeout    time.Duration `yaml:"request_timeout"`
	PollInterval      time.Duration `yaml:"poll_interval"`
	BatchSize         int           `yaml:"batch_size"`
	ParserWorkers     int           `yaml:"parser_workers"`
	RawChannelSize    int           `yaml:"raw_channel_size"`
	ParsedChannelSize int           `yaml:"parsed_channel_size"`
	OutputChannelSize int           `yaml:"output_channel_size"`
	AllowlistFile     string        `yaml:"allowlist_file"`
	IncludeAllLogs    bool          `yaml:"include_all_logs"`
	OnlySubdomains    bool          `yaml:"only_subdomains"`
	ExcludeLogURLSubs []string      `yaml:"exclude_log_url_substrings"`
	AllowedOperators  []string      `yaml:"allowed_operators"`
	DedupWorkers      int           `yaml:"dedup_workers"`
	DedupBatchSize    int           `yaml:"dedup_batch_size"`
	DedupFlush        time.Duration `yaml:"dedup_flush_interval"`
	PSLCacheSize      int           `yaml:"psl_cache_size"`
	Bloom             BloomConfig   `yaml:"bloom"`
	Dedup             DedupConfig   `yaml:"dedup"`
	Retry             RetryConfig   `yaml:"retry"`
}

func Default() Config {
	return Config{
		LogListURL:        "https://www.gstatic.com/ct/log_list/v3/log_list.json",
		DataDir:           "./data",
		RequestTimeout:    10 * time.Second,
		PollInterval:      5 * time.Second,
		BatchSize:         256,
		ParserWorkers:     8,
		RawChannelSize:    4096,
		ParsedChannelSize: 4096,
		OutputChannelSize: 4096,
		AllowlistFile:     "",
		IncludeAllLogs:    true,
		OnlySubdomains:    true,
		ExcludeLogURLSubs: []string{"example.com"},
		AllowedOperators:  nil,
		DedupWorkers:      4,
		DedupBatchSize:    500,
		DedupFlush:        time.Second,
		PSLCacheSize:      100000,
		Bloom: BloomConfig{
			Bits:          10_485_760,
			Hashes:        7,
			Windows:       6,
			WindowSeconds: 600 * time.Second,
		},
		Dedup: DedupConfig{
			BadgerValueLogFileSize: 256 * 1024 * 1024,
			GCInterval:             10 * time.Minute,
		},
		Retry: RetryConfig{
			Initial: 500 * time.Millisecond,
			Max:     30 * time.Second,
			Factor:  2.0,
			Jitter:  0.2,
		},
	}
}

func Load() (Config, string, error) {
	configPath := flag.String("config", "", "Path to YAML config file")
	dataDir := flag.String("data-dir", "", "Override data directory")
	allowlist := flag.String("allowlist", "", "Override allowlist file")
	batchSize := flag.Int("batch-size", 0, "Override batch size")
	workers := flag.Int("workers", 0, "Override parser workers")
	allLogs := flag.Bool("all-logs", false, "Include all public logs (not just usable)")
	onlySubs := flag.Bool("only-subdomains", false, "Emit only subdomains (exclude apex)")
	flag.Parse()

	cfg := Default()
	path := *configPath
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return Config{}, "", err
		}
		if err := yaml.Unmarshal(b, &cfg); err != nil {
			return Config{}, "", err
		}
	}

	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}
	if *allowlist != "" {
		cfg.AllowlistFile = *allowlist
	}
	if *batchSize > 0 {
		cfg.BatchSize = *batchSize
	}
	if *workers > 0 {
		cfg.ParserWorkers = *workers
	}
	if *allLogs {
		cfg.IncludeAllLogs = true
	}
	if *onlySubs {
		cfg.OnlySubdomains = true
	}

	if cfg.LogListURL == "" {
		return Config{}, "", errors.New("log_list_url is required")
	}
	cfg.DataDir = filepath.Clean(cfg.DataDir)
	return cfg, path, nil
}
