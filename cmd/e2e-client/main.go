package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	mode := strings.ToLower(envOrDefault("KAFSCALE_E2E_MODE", "produce"))
	brokerAddr := strings.TrimSpace(os.Getenv("KAFSCALE_E2E_BROKER_ADDR"))
	topic := strings.TrimSpace(os.Getenv("KAFSCALE_E2E_TOPIC"))
	count := parseEnvInt("KAFSCALE_E2E_COUNT", 1)
	timeout := time.Duration(parseEnvInt("KAFSCALE_E2E_TIMEOUT_SEC", 40)) * time.Second

	switch mode {
	case "produce":
		if brokerAddr == "" || topic == "" {
			log.Fatalf("KAFSCALE_E2E_BROKER_ADDR and KAFSCALE_E2E_TOPIC are required")
		}
		if count <= 0 {
			log.Fatalf("KAFSCALE_E2E_COUNT must be > 0")
		}
		client, err := kgo.NewClient(
			kgo.SeedBrokers(brokerAddr),
			kgo.AllowAutoTopicCreation(),
		)
		if err != nil {
			log.Fatalf("create producer client: %v", err)
		}
		defer client.Close()
		if err := produceMessages(context.Background(), client, topic, count); err != nil {
			log.Fatalf("produce: %v", err)
		}
		log.Printf("produced %d messages to %s", count, topic)
	case "consume":
		if brokerAddr == "" || topic == "" {
			log.Fatalf("KAFSCALE_E2E_BROKER_ADDR and KAFSCALE_E2E_TOPIC are required")
		}
		if count <= 0 {
			log.Fatalf("KAFSCALE_E2E_COUNT must be > 0")
		}
		client, err := kgo.NewClient(
			kgo.SeedBrokers(brokerAddr),
			kgo.ConsumeTopics(topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		)
		if err != nil {
			log.Fatalf("create consumer client: %v", err)
		}
		defer client.Close()
		if err := consumeMessages(context.Background(), client, topic, count, timeout); err != nil {
			log.Fatalf("consume: %v", err)
		}
		log.Printf("consumed %d messages from %s", count, topic)
	case "probe":
		addrs := parseCSVAddrs(os.Getenv("KAFSCALE_E2E_ADDRS"))
		if len(addrs) == 0 {
			log.Fatalf("KAFSCALE_E2E_ADDRS is required for probe mode")
		}
		retries := parseEnvInt("KAFSCALE_E2E_PROBE_RETRIES", 5)
		if retries < 1 {
			retries = 1
		}
		sleep := time.Duration(parseEnvInt("KAFSCALE_E2E_PROBE_SLEEP_MS", 200)) * time.Millisecond
		if err := probeAddrs(addrs, 2*time.Second, retries, sleep, dialAddr); err != nil {
			log.Fatalf("probe: %v", err)
		}
		log.Printf("probed %d addresses", len(addrs))
	default:
		log.Fatalf("unknown KAFSCALE_E2E_MODE %q", mode)
	}
}

func produceMessages(ctx context.Context, client *kgo.Client, topic string, count int) error {
	for i := 0; i < count; i++ {
		msg := fmt.Sprintf("restart-%d", i)
		res := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(msg)})
		if err := res.FirstErr(); err != nil {
			return err
		}
	}
	return nil
}

func consumeMessages(ctx context.Context, client *kgo.Client, topic string, count int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	received := 0
	for received < count {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %d messages (got %d)", count, received)
		}
		fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		fetches := client.PollFetches(fetchCtx)
		cancel()
		if errs := fetches.Errors(); len(errs) > 0 {
			if !allTransientFetchErrors(errs) {
				return fmt.Errorf("fetch errors: %+v", errs)
			}
			continue
		}
		fetches.EachRecord(func(record *kgo.Record) {
			received++
		})
	}
	return nil
}

func allTransientFetchErrors(errs []kgo.FetchError) bool {
	for _, fetchErr := range errs {
		err := fetchErr.Err
		if err == nil {
			continue
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			continue
		}
		return false
	}
	return true
}

type dialFunc func(addr string, timeout time.Duration) error

func dialAddr(addr string, timeout time.Duration) error {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return err
	}
	return conn.Close()
}

func probeAddrs(addrs []string, timeout time.Duration, retries int, sleep time.Duration, dial dialFunc) error {
	for _, addr := range addrs {
		var lastErr error
		for attempt := 0; attempt < retries; attempt++ {
			if err := dial(addr, timeout); err == nil {
				lastErr = nil
				break
			} else {
				lastErr = err
			}
			time.Sleep(sleep)
		}
		if lastErr != nil {
			return fmt.Errorf("probe %s failed: %w", addr, lastErr)
		}
	}
	return nil
}

func parseCSVAddrs(raw string) []string {
	parts := strings.Split(raw, ",")
	addrs := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		addrs = append(addrs, part)
	}
	return addrs
}

func envOrDefault(name, fallback string) string {
	if val := strings.TrimSpace(os.Getenv(name)); val != "" {
		return val
	}
	return fallback
}

func parseEnvInt(name string, fallback int) int {
	val := strings.TrimSpace(os.Getenv(name))
	if val == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}
	return parsed
}
