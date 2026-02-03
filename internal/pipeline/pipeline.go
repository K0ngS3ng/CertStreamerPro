package pipeline

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/K0ngS3ng/CertStreamerPro/internal/allowlist"
	"github.com/K0ngS3ng/CertStreamerPro/internal/dedup"
	"github.com/K0ngS3ng/CertStreamerPro/internal/output"
	"github.com/K0ngS3ng/CertStreamerPro/internal/parser"
	"github.com/K0ngS3ng/CertStreamerPro/internal/util"
)

type Pipeline struct {
	RawCh     chan parser.RawEntry
	ParsedCh  chan parser.DomainEvent
	DedupCh   chan dedup.Event
	OutputCh  chan dedup.Event
	Allowlist *allowlist.Allowlist
	OnlySubs  bool
	Logger    *log.Logger
}

func New(rawSize, parsedSize, outputSize int, allow *allowlist.Allowlist, onlySubs bool) *Pipeline {
	if rawSize < 1 {
		rawSize = 1024
	}
	if parsedSize < 1 {
		parsedSize = 1024
	}
	if outputSize < 1 {
		outputSize = 1024
	}
	return &Pipeline{
		RawCh:     make(chan parser.RawEntry, rawSize),
		ParsedCh:  make(chan parser.DomainEvent, parsedSize),
		DedupCh:   make(chan dedup.Event, parsedSize),
		OutputCh:  make(chan dedup.Event, outputSize),
		Allowlist: allow,
		OnlySubs:  onlySubs,
		Logger:    log.New(os.Stderr, "", log.LstdFlags),
	}
}

func (p *Pipeline) Start(ctx context.Context, workers int, deduper *dedup.Deduper) {
	logf := func(format string, args ...any) {
		if p.Logger != nil {
			p.Logger.Printf(format, args...)
		}
	}
	parser.RunWorkers(ctx, workers, p.RawCh, p.ParsedCh, logf)

	go p.normalize(ctx, logf)
}

func (p *Pipeline) normalize(ctx context.Context, logf func(string, ...any)) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-p.ParsedCh:
			if !ok {
				return
			}
			norm, ok := util.NormalizeDomain(ev.Domain)
			if !ok {
				continue
			}
			if p.OnlySubs && !util.IsSubdomain(norm) {
				continue
			}
			if p.Allowlist != nil && !p.Allowlist.Match(norm) {
				continue
			}
			out := dedup.Event{Domain: norm, LogURL: ev.LogURL, Observed: ev.Observed, SCTime: ev.SCTime}
			select {
			case <-ctx.Done():
				return
			case p.DedupCh <- out:
				continue
			default:
				if logf != nil {
					logf("dedup channel full, backpressuring: %s", norm)
				}
				select {
				case <-ctx.Done():
					return
				case p.DedupCh <- out:
				}
			}
		}
	}
}

func (p *Pipeline) Emit(ctx context.Context) {
	enc := output.NewJSONEncoder(os.Stdout)
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-p.OutputCh:
			if !ok {
				return
			}
			out := output.EventOutput{
				Domain:     ev.Domain,
				LogURL:     ev.LogURL,
				ObservedAt: ev.Observed.UTC().Format(time.RFC3339Nano),
			}
			if !ev.SCTime.IsZero() {
				out.SCTimestamp = ev.SCTime.UTC().Format(time.RFC3339Nano)
				lat := ev.Observed.Sub(ev.SCTime)
				if lat < 0 {
					lat = 0
				}
				out.LatencyMS = lat.Milliseconds()
			}
			_ = enc.Encode(out)
		}
	}
}
