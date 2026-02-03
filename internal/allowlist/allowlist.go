package allowlist

import (
	"bufio"
	"os"
	"strings"

	"github.com/K0ngS3ng/CertStreamerPro/internal/util"
)

type Allowlist struct {
	roots map[string]struct{}
}

func Load(path string) (*Allowlist, error) {
	if path == "" {
		return &Allowlist{roots: nil}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	roots := make(map[string]struct{})
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		norm, ok := util.NormalizeDomain(line)
		if !ok {
			continue
		}
		root, err := util.EffectiveTLDPlusOneCached(norm)
		if err != nil {
			continue
		}
		roots[root] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return &Allowlist{roots: roots}, nil
}

func FromRoots(roots []string) *Allowlist {
	if len(roots) == 0 {
		return &Allowlist{roots: nil}
	}
	out := make(map[string]struct{})
	for _, r := range roots {
		norm, ok := util.NormalizeDomain(r)
		if !ok {
			continue
		}
		root, err := util.EffectiveTLDPlusOneCached(norm)
		if err != nil {
			continue
		}
		out[root] = struct{}{}
	}
	return &Allowlist{roots: out}
}

func (a *Allowlist) Match(domain string) bool {
	if a == nil || a.roots == nil || len(a.roots) == 0 {
		return true
	}
	root, err := util.EffectiveTLDPlusOneCached(domain)
	if err != nil {
		return false
	}
	_, ok := a.roots[root]
	return ok
}
