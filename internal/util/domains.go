package util

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"golang.org/x/net/idna"
)

func NormalizeDomain(in string) (string, bool) {
	s := strings.TrimSpace(in)
	if s == "" {
		return "", false
	}
	s = strings.TrimSuffix(s, ".")
	s = strings.ToLower(s)
	for strings.HasPrefix(s, "*.") {
		s = strings.TrimPrefix(s, "*.")
	}
	if s == "" {
		return "", false
	}
	ascii, err := idna.Lookup.ToASCII(s)
	if err != nil {
		return "", false
	}
	unicode, err := idna.Lookup.ToUnicode(ascii)
	if err == nil && unicode != "" {
		s = unicode
	} else {
		s = ascii
	}
	return s, true
}

func HashDomain(domain string) string {
	sum := sha256.Sum256([]byte(domain))
	return hex.EncodeToString(sum[:])
}

func IsSubdomain(domain string) bool {
	root, err := EffectiveTLDPlusOneCached(domain)
	if err != nil {
		return false
	}
	return domain != root
}
