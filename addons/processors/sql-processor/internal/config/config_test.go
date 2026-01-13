// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	data := []byte("s3:\n  bucket: test-bucket\n")
	path := writeTempConfig(t, data)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Server.Listen == "" || cfg.Server.MetricsListen == "" {
		t.Fatalf("expected server defaults")
	}
	if cfg.Query.DefaultLimit == 0 || cfg.Query.MaxUnbounded == 0 {
		t.Fatalf("expected query defaults")
	}
	if cfg.Metadata.Snapshot.Key == "" {
		t.Fatalf("expected snapshot key default")
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	data := []byte("s3:\n  bucket: test-bucket\n")
	path := writeTempConfig(t, data)
	t.Setenv("KAFSQL_SERVER_LISTEN", ":5555")
	t.Setenv("KAFSQL_QUERY_DEFAULT_LIMIT", "123")
	t.Setenv("KAFSQL_METADATA_SNAPSHOT_KEY", "/custom/snapshot")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Server.Listen != ":5555" {
		t.Fatalf("expected server listen override, got %q", cfg.Server.Listen)
	}
	if cfg.Query.DefaultLimit != 123 {
		t.Fatalf("expected default limit override, got %d", cfg.Query.DefaultLimit)
	}
	if cfg.Metadata.Snapshot.Key != "/custom/snapshot" {
		t.Fatalf("expected snapshot key override, got %q", cfg.Metadata.Snapshot.Key)
	}
}

func TestSchemaValidation(t *testing.T) {
	data := []byte("s3:\n  bucket: test-bucket\nmetadata:\n  topics:\n    - name: orders\n      schema:\n        columns:\n          - name: amount\n            type: decimal\n            path: $.amount\n")
	path := writeTempConfig(t, data)
	if _, err := Load(path); err == nil {
		t.Fatalf("expected schema validation error")
	}
}

func writeTempConfig(t *testing.T, data []byte) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
