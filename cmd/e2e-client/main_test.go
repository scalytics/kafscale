package main

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestParseCSVAddrs(t *testing.T) {
	addrs := parseCSVAddrs("  a:1, b:2 ,,c:3, ")
	if len(addrs) != 3 {
		t.Fatalf("expected 3 addrs got %d", len(addrs))
	}
	if strings.Join(addrs, "|") != "a:1|b:2|c:3" {
		t.Fatalf("unexpected addrs: %v", addrs)
	}
}

func TestProbeAddrs(t *testing.T) {
	calls := make(map[string]int)
	dial := func(addr string, _ time.Duration) error {
		calls[addr]++
		if addr == "bad:1" {
			return errors.New("dial failed")
		}
		return nil
	}
	if err := probeAddrs([]string{"good:1"}, 10*time.Millisecond, 2, 1*time.Millisecond, dial); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := probeAddrs([]string{"bad:1"}, 1*time.Millisecond, 2, 1*time.Millisecond, dial); err == nil {
		t.Fatalf("expected error")
	}
	if calls["bad:1"] < 2 {
		t.Fatalf("expected retries, got %d", calls["bad:1"])
	}
}
