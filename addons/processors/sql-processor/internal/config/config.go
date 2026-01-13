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
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config defines the processor configuration schema.
type Config struct {
	S3       S3Config     `yaml:"s3"`
	Server   ServerConfig `yaml:"server"`
	Metadata MetaConfig   `yaml:"metadata"`
	Query    QueryConfig  `yaml:"query"`

	Mappings []Mapping    `yaml:"mappings"`
	Offsets  OffsetConfig `yaml:"offsets"`
}

type S3Config struct {
	Bucket    string `yaml:"bucket"`
	Namespace string `yaml:"namespace"`
	Endpoint  string `yaml:"endpoint"`
	Region    string `yaml:"region"`
	PathStyle bool   `yaml:"path_style"`
}

type OffsetConfig struct {
	Backend string `yaml:"backend"`
}

type Mapping struct {
	Topic               string `yaml:"topic"`
	Sink                string `yaml:"sink"`
	Mode                string `yaml:"mode"`
	CreateTableIfAbsent bool   `yaml:"create_table_if_missing"`
}

type ServerConfig struct {
	Listen         string `yaml:"listen"`
	MaxConnections int    `yaml:"max_connections"`
	ServerVersion  string `yaml:"server_version"`
	ClientEncoding string `yaml:"client_encoding"`
	MetricsListen  string `yaml:"metrics_listen"`
}

type MetaConfig struct {
	Discovery string         `yaml:"discovery"`
	Etcd      EtcdConfig     `yaml:"etcd"`
	Snapshot  SnapshotConfig `yaml:"snapshot"`
	Topics    []TopicConfig  `yaml:"topics"`
}

type EtcdConfig struct {
	Endpoints []string `yaml:"endpoints"`
}

type SnapshotConfig struct {
	Key        string `yaml:"key"`
	TTLSeconds int    `yaml:"ttl_seconds"`
}

type QueryConfig struct {
	DefaultLimit     int  `yaml:"default_limit"`
	RequireTimeBound bool `yaml:"require_time_bound"`
	MaxUnbounded     int  `yaml:"max_unbounded_scan"`
}

type TopicConfig struct {
	Name       string       `yaml:"name"`
	Partitions []int32      `yaml:"partitions"`
	Schema     SchemaConfig `yaml:"schema"`
}

type SchemaConfig struct {
	Columns []SchemaColumn `yaml:"columns"`
}

type SchemaColumn struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
	Path string `yaml:"path"`
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}

	applyDefaults(&cfg)
	applyEnvOverrides(&cfg)

	if cfg.S3.Bucket == "" {
		return Config{}, fmt.Errorf("s3.bucket is required")
	}
	if err := validateSchema(cfg.Metadata.Topics); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.Server.Listen == "" {
		cfg.Server.Listen = ":5432"
	}
	if cfg.Server.MaxConnections == 0 {
		cfg.Server.MaxConnections = 100
	}
	if cfg.Server.ServerVersion == "" {
		cfg.Server.ServerVersion = "15.0"
	}
	if cfg.Server.ClientEncoding == "" {
		cfg.Server.ClientEncoding = "UTF8"
	}
	if cfg.Server.MetricsListen == "" {
		cfg.Server.MetricsListen = ":9090"
	}
	if cfg.Query.DefaultLimit == 0 {
		cfg.Query.DefaultLimit = 1000
	}
	if cfg.Query.MaxUnbounded == 0 {
		cfg.Query.MaxUnbounded = 1000
	}
	if cfg.Metadata.Snapshot.Key == "" {
		cfg.Metadata.Snapshot.Key = "/kafscale/metadata/snapshot"
	}
}

func applyEnvOverrides(cfg *Config) {
	setString(&cfg.S3.Bucket, "KAFSQL_S3_BUCKET")
	setString(&cfg.S3.Namespace, "KAFSQL_S3_NAMESPACE")
	setString(&cfg.S3.Endpoint, "KAFSQL_S3_ENDPOINT")
	setString(&cfg.S3.Region, "KAFSQL_S3_REGION")
	setBool(&cfg.S3.PathStyle, "KAFSQL_S3_PATH_STYLE")

	setString(&cfg.Server.Listen, "KAFSQL_SERVER_LISTEN")
	setInt(&cfg.Server.MaxConnections, "KAFSQL_SERVER_MAX_CONNECTIONS")
	setString(&cfg.Server.ServerVersion, "KAFSQL_SERVER_VERSION")
	setString(&cfg.Server.ClientEncoding, "KAFSQL_CLIENT_ENCODING")
	setString(&cfg.Server.MetricsListen, "KAFSQL_METRICS_LISTEN")

	setString(&cfg.Metadata.Discovery, "KAFSQL_METADATA_DISCOVERY")
	setCSV(&cfg.Metadata.Etcd.Endpoints, "KAFSQL_METADATA_ETCD_ENDPOINTS")
	setInt(&cfg.Metadata.Snapshot.TTLSeconds, "KAFSQL_METADATA_SNAPSHOT_TTL_SECONDS")
	setString(&cfg.Metadata.Snapshot.Key, "KAFSQL_METADATA_SNAPSHOT_KEY")

	setInt(&cfg.Query.DefaultLimit, "KAFSQL_QUERY_DEFAULT_LIMIT")
	setBool(&cfg.Query.RequireTimeBound, "KAFSQL_QUERY_REQUIRE_TIME_BOUND")
	setInt(&cfg.Query.MaxUnbounded, "KAFSQL_QUERY_MAX_UNBOUNDED")
}

func validateSchema(topics []TopicConfig) error {
	for _, topic := range topics {
		for _, col := range topic.Schema.Columns {
			if col.Name == "" || col.Path == "" || col.Type == "" {
				return fmt.Errorf("invalid schema column for topic %s", topic.Name)
			}
			if !isSupportedSchemaType(col.Type) {
				return fmt.Errorf("unsupported schema type %q for topic %s", col.Type, topic.Name)
			}
		}
	}
	return nil
}

func isSupportedSchemaType(value string) bool {
	switch strings.ToLower(value) {
	case "string", "int", "long", "double", "boolean", "timestamp":
		return true
	default:
		return false
	}
}

func setString(target *string, envKey string) {
	if val, ok := os.LookupEnv(envKey); ok {
		*target = val
	}
}

func setInt(target *int, envKey string) {
	if val, ok := os.LookupEnv(envKey); ok {
		parsed, err := strconv.Atoi(val)
		if err == nil {
			*target = parsed
		}
	}
}

func setBool(target *bool, envKey string) {
	if val, ok := os.LookupEnv(envKey); ok {
		parsed, err := strconv.ParseBool(val)
		if err == nil {
			*target = parsed
		}
	}
}

func setCSV(target *[]string, envKey string) {
	if val, ok := os.LookupEnv(envKey); ok {
		parts := strings.Split(val, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" {
				out = append(out, trimmed)
			}
		}
		if len(out) > 0 {
			*target = out
		}
	}
}
