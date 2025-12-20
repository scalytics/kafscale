// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package console

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type promMetricsClient struct {
	url    string
	client *http.Client
}

func NewPromMetricsClient(url string) MetricsProvider {
	return &promMetricsClient{
		url: url,
		client: &http.Client{
			Timeout: 3 * time.Second,
		},
	}
}

func (c *promMetricsClient) Snapshot(ctx context.Context) (*MetricsSnapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics request failed: %s", resp.Status)
	}
	var (
		state      string
		latencyMS  int
		produceRPS float64
		fetchRPS   float64
	)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "kafscale_s3_health_state"):
			if val, ok := parsePromSample(line); ok && val == 1 {
				if parsedState, ok := parseStateLabel(line); ok {
					state = parsedState
				}
			}
		case strings.HasPrefix(line, "kafscale_s3_latency_ms_avg"):
			if val, ok := parsePromSample(line); ok {
				latencyMS = int(val)
			}
		case strings.HasPrefix(line, "kafscale_produce_rps"):
			if val, ok := parsePromSample(line); ok {
				produceRPS = val
			}
		case strings.HasPrefix(line, "kafscale_fetch_rps"):
			if val, ok := parsePromSample(line); ok {
				fetchRPS = val
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return &MetricsSnapshot{
		S3State:     state,
		S3LatencyMS: latencyMS,
		ProduceRPS:  produceRPS,
		FetchRPS:    fetchRPS,
	}, nil
}

func parsePromSample(line string) (float64, bool) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return 0, false
	}
	last := parts[len(parts)-1]
	val, err := strconv.ParseFloat(last, 64)
	if err != nil {
		return 0, false
	}
	return val, true
}

func parseStateLabel(line string) (string, bool) {
	start := strings.Index(line, `state="`)
	if start == -1 {
		return "", false
	}
	start += len(`state="`)
	end := strings.Index(line[start:], `"`)
	if end == -1 {
		return "", false
	}
	return line[start : start+end], true
}
