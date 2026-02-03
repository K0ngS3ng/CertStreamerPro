package parser

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"time"
)

type RawEntry struct {
	LogURL    string
	LeafInput string
	ExtraData string
	Observed  time.Time
}

type DomainEvent struct {
	Domain   string
	LogURL   string
	Observed time.Time
	SCTime   time.Time
}

func RunWorkers(ctx context.Context, workers int, in <-chan RawEntry, out chan<- DomainEvent, logf func(string, ...any)) {
	if workers < 1 {
		workers = 1
	}
	limiter := newLogLimiter(5 * time.Second)
	for i := 0; i < workers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case entry, ok := <-in:
					if !ok {
						return
					}
					parseEntry(ctx, entry, out, logf, limiter)
				}
			}
		}()
	}
}

func parseEntry(ctx context.Context, entry RawEntry, out chan<- DomainEvent, logf func(string, ...any), limiter *logLimiter) {
	cert, sctTime, err := parseCertificate(entry.LeafInput, entry.ExtraData)
	if err != nil {
		if logf != nil && limiter.Allow() {
			logf("parse cert error: %v", err)
		}
		return
	}
	emitDomains(ctx, entry, cert, sctTime, out)
}

func emitDomains(ctx context.Context, entry RawEntry, cert *x509.Certificate, sctTime time.Time, out chan<- DomainEvent) {
	push := func(name string) {
		select {
		case <-ctx.Done():
			return
		case out <- DomainEvent{Domain: name, LogURL: entry.LogURL, Observed: entry.Observed, SCTime: sctTime}:
			return
		default:
			return
		}
	}
	if cert.Subject.CommonName != "" {
		push(cert.Subject.CommonName)
	}
	for _, dns := range cert.DNSNames {
		if dns == "" {
			continue
		}
		push(dns)
	}
}

func parseCertificate(leafInput, extraData string) (*x509.Certificate, time.Time, error) {
	entryType, cert, sctTime, err := parseFromLeaf(leafInput)
	if err == nil && cert != nil {
		return cert, sctTime, nil
	}
	if entryType == 1 || cert == nil {
		if extraData == "" {
			return nil, sctTime, errors.New("no parsable certificate")
		}
		c, err := parseFromExtra(extraData)
		return c, sctTime, err
	}
	return nil, sctTime, errors.New("no parsable certificate")
}

func parseFromLeaf(leafInput string) (uint16, *x509.Certificate, time.Time, error) {
	buf, err := base64.StdEncoding.DecodeString(leafInput)
	if err != nil {
		return 0, nil, time.Time{}, err
	}
	if len(buf) < 12 {
		return 0, nil, time.Time{}, errors.New("leaf too short")
	}
	pos := 0
	version := buf[pos]
	pos++
	leafType := buf[pos]
	pos++
	_ = version
	_ = leafType
	if len(buf) < pos+8 {
		return 0, nil, time.Time{}, errors.New("leaf missing timestamp")
	}
	tsMillis := binary.BigEndian.Uint64(buf[pos : pos+8])
	sctTime := time.Unix(0, int64(tsMillis)*int64(time.Millisecond))
	pos += 8
	if len(buf) < pos+2 {
		return 0, nil, sctTime, errors.New("leaf missing entry type")
	}
	entryType := binary.BigEndian.Uint16(buf[pos : pos+2])
	pos += 2
	if entryType != 0 {
		return entryType, nil, sctTime, errors.New("non-x509 entry")
	}
	if len(buf) < pos+3 {
		return entryType, nil, sctTime, errors.New("leaf missing cert length")
	}
	certLen := int(buf[pos])<<16 | int(buf[pos+1])<<8 | int(buf[pos+2])
	pos += 3
	if certLen <= 0 || len(buf) < pos+certLen {
		return entryType, nil, sctTime, errors.New("invalid cert length")
	}
	der := buf[pos : pos+certLen]
	cert, err := x509.ParseCertificate(der)
	return entryType, cert, sctTime, err
}

func parseFromExtra(extraData string) (*x509.Certificate, error) {
	buf, err := base64.StdEncoding.DecodeString(extraData)
	if err != nil {
		return nil, err
	}
	return parseTLSCertChain(buf)
}

func parseTLSCertChain(buf []byte) (*x509.Certificate, error) {
	if len(buf) < 3 {
		return nil, errors.New("extra_data too short")
	}
	pos := 0
	totalLen := int(buf[pos])<<16 | int(buf[pos+1])<<8 | int(buf[pos+2])
	pos += 3
	if totalLen <= 0 || totalLen > len(buf)-pos {
		totalLen = len(buf) - pos
	}
	end := pos + totalLen
	for pos+3 <= end {
		certLen := int(buf[pos])<<16 | int(buf[pos+1])<<8 | int(buf[pos+2])
		pos += 3
		if certLen <= 0 || pos+certLen > end || pos+certLen > len(buf) {
			break
		}
		der := buf[pos : pos+certLen]
		pos += certLen
		if cert, err := x509.ParseCertificate(der); err == nil {
			return cert, nil
		}
	}
	return nil, errors.New("no valid cert in chain")
}

type logLimiter struct {
	interval time.Duration
	lastUnix atomic.Int64
}

func newLogLimiter(interval time.Duration) *logLimiter {
	return &logLimiter{interval: interval}
}

func (l *logLimiter) Allow() bool {
	if l == nil {
		return true
	}
	now := time.Now().UnixNano()
	last := l.lastUnix.Load()
	if time.Duration(now-last) >= l.interval {
		l.lastUnix.Store(now)
		return true
	}
	return false
}
