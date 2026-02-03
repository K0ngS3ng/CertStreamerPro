package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/K0ngS3ng/CertStreamerPro/internal/allowlist"
	"github.com/K0ngS3ng/CertStreamerPro/internal/config"
	"github.com/K0ngS3ng/CertStreamerPro/internal/ctlog"
	"github.com/K0ngS3ng/CertStreamerPro/internal/dedup"
	"github.com/K0ngS3ng/CertStreamerPro/internal/pipeline"
	"github.com/K0ngS3ng/CertStreamerPro/internal/store"
	"github.com/K0ngS3ng/CertStreamerPro/internal/tailer"
	"github.com/K0ngS3ng/CertStreamerPro/internal/util"
)

func main() {
	randSeed()
	cfg, cfgPath, err := config.Load()
	logger := log.New(os.Stderr, "", log.LstdFlags)
	if err != nil {
		logger.Printf("config error: %v", err)
		os.Exit(1)
	}
	if cfgPath != "" {
		logger.Printf("using config: %s", cfgPath)
	}
	util.SetPSLCacheSize(cfg.PSLCacheSize)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	_ = cfg.DataDir
	memStore := store.OpenMemory()
	defer memStore.Close()

	allow, err := allowlist.Load(cfg.AllowlistFile)
	if err != nil {
		logger.Printf("allowlist error: %v", err)
		os.Exit(1)
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
		logger.Printf("log discovery error: %v", err)
		os.Exit(1)
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

	pipe.Emit(ctx)
	<-ctx.Done()
}

func randSeed() {
	// Best-effort seed for backoff jitter.
	rand.Seed(time.Now().UnixNano())
}
