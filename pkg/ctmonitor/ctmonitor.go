package ctmonitor

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"github.com/K0ngS3ng/CertStreamerPro/internal/allowlist"
	"github.com/K0ngS3ng/CertStreamerPro/internal/ctlog"
	"github.com/K0ngS3ng/CertStreamerPro/internal/dedup"
	"github.com/K0ngS3ng/CertStreamerPro/internal/pipeline"
	"github.com/K0ngS3ng/CertStreamerPro/internal/store"
	"github.com/K0ngS3ng/CertStreamerPro/internal/tailer"
	"github.com/K0ngS3ng/CertStreamerPro/internal/util"
)

type BloomConfig struct {
	Bits          uint64
	Hashes        uint32
	Windows       int
	WindowSeconds time.Duration
}

type DedupConfig struct {
	BadgerValueLogFileSize int64
	GCInterval             time.Duration
}

type RetryConfig struct {
	Initial time.Duration
	Max     time.Duration
	Factor  float64
	Jitter  float64
}

type Config struct {
	LogListURL        string
	DataDir           string
	RequestTimeout    time.Duration
	PollInterval      time.Duration
	BatchSize         int
	ParserWorkers     int
	RawChannelSize    int
	ParsedChannelSize int
	OutputChannelSize int
	AllowlistFile     string
	AllowRoots        []string
	IncludeAllLogs    bool
	OnlySubdomains    bool
	ExcludeLogURLSubs []string
	AllowedOperators  []string
	DedupWorkers      int
	DedupBatchSize    int
	DedupFlush        time.Duration
	PSLCacheSize      int
	Bloom             BloomConfig
	Dedup             DedupConfig
	Retry             RetryConfig
}

type Event struct {
	Domain    string
	LogURL    string
	Observed  time.Time
	SCTime    time.Time
	LatencyMS int64
}

type Monitor struct {
	Events <-chan Event
	cancel context.CancelFunc
	close  func()
}

type Option func(*options)

type options struct {
	logger *log.Logger
}

func WithLogger(l *log.Logger) Option {
	return func(o *options) {
		o.logger = l
	}
}

func DefaultConfig() Config {
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
		AllowRoots:        nil,
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

func Start(ctx context.Context, cfg Config, opts ...Option) (*Monitor, error) {
	ctx, cancel := context.WithCancel(ctx)
	if cfg.LogListURL == "" {
		cancel()
		return nil, errors.New("log list url is required")
	}
	if cfg.DataDir == "" {
		// DataDir is no longer required in memory-only mode.
	}

	opt := options{logger: log.New(os.Stderr, "", log.LstdFlags)}
	for _, o := range opts {
		o(&opt)
	}
	logger := opt.logger
	util.SetPSLCacheSize(cfg.PSLCacheSize)

	memStore := store.OpenMemory()

	allow, err := allowlist.Load(cfg.AllowlistFile)
	if err != nil {
		_ = memStore.Close()
		cancel()
		return nil, err
	}
	if len(cfg.AllowRoots) > 0 {
		allow = allowlist.FromRoots(cfg.AllowRoots)
	}

	bloom := dedup.NewBloom(cfg.Bloom.Bits, cfg.Bloom.Hashes, cfg.Bloom.Windows, cfg.Bloom.WindowSeconds)
	deduper := dedup.NewDeduper(memStore, bloom)

	pipe := pipeline.New(cfg.RawChannelSize, cfg.ParsedChannelSize, cfg.OutputChannelSize, allow, cfg.OnlySubdomains)
	pipe.Logger = logger
	pipe.Start(ctx, cfg.ParserWorkers, deduper)
	go deduper.RunSharded(ctx, pipe.DedupCh, pipe.OutputCh, cfg.DedupWorkers, cfg.DedupBatchSize, cfg.DedupFlush, logger.Printf)
	go dedup.RunPendingEmitter(ctx, memStore, pipe.OutputCh, logger.Printf)

	client := ctlog.DefaultHTTPClient(cfg.RequestTimeout)
	logs, err := ctlog.Discover(ctx, client, cfg.LogListURL, cfg.IncludeAllLogs, cfg.ExcludeLogURLSubs, cfg.AllowedOperators)
	if err != nil {
		_ = memStore.Close()
		cancel()
		return nil, err
	}
	logger.Printf("discovered %d usable logs", len(logs))

	for _, lg := range logs {
		lg := lg
		t := &tailer.Tailer{
			Log:       lg,
			Client:    client,
			Store:     store,
			BatchSize: cfg.BatchSize,
			Poll:      cfg.PollInterval,
			Retry: util.Backoff{
				Initial: cfg.Retry.Initial,
				Max:     cfg.Retry.Max,
				Factor:  cfg.Retry.Factor,
				Jitter:  cfg.Retry.Jitter,
			},
			RawOut:     pipe.RawCh,
			Logf:       logger.Printf,
			DropOnFull: true,
		}
		go t.Run(ctx)
	}

	events := make(chan Event, cfg.OutputChannelSize)
	stop := func() {
		cancel()
		_ = memStore.Close()
	}
	go fanOut(ctx, pipe.OutputCh, events, logger)

	return &Monitor{Events: events, cancel: cancel, close: stop}, nil
}

func (m *Monitor) Close() {
	if m != nil && m.close != nil {
		m.close()
	}
}

func fanOut(ctx context.Context, in <-chan dedup.Event, out chan<- Event, logger *log.Logger) {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-in:
			if !ok {
				return
			}
			e := Event{Domain: ev.Domain, LogURL: ev.LogURL, Observed: ev.Observed, SCTime: ev.SCTime}
			if !e.SCTime.IsZero() {
				lat := e.Observed.Sub(e.SCTime)
				if lat < 0 {
					lat = 0
				}
				e.LatencyMS = lat.Milliseconds()
			}
			select {
			case out <- e:
				continue
			default:
				if logger != nil {
					logger.Printf("events channel full, dropping domain: %s", e.Domain)
				}
			}
		}
	}
}
