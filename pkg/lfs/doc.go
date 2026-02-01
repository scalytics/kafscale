// Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

/*
Package lfs provides Large File Support (LFS) for Kafka messages.

LFS enables storing large payloads (up to 5GB) in S3 while keeping small
envelope pointers in Kafka topics. This implements the "Claim Check" pattern.

# Overview

When a Kafka producer sends a message with the LFS_BLOB header, the LFS proxy:
  1. Uploads the payload to S3
  2. Computes SHA256 checksum
  3. Creates a JSON envelope with metadata
  4. Forwards the envelope (not the payload) to Kafka

Consumers receive the envelope and can use this package to transparently
fetch the original payload from S3.

# Envelope Format

The LFS envelope is a JSON object stored as the Kafka message value:

	{
	  "kfs_lfs": 1,
	  "bucket": "kafscale-lfs",
	  "key": "default/topic/lfs/2026/02/01/obj-uuid",
	  "size": 10485760,
	  "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	  "content_type": "application/octet-stream",
	  "created_at": "2026-02-01T12:00:00Z",
	  "proxy_id": "lfs-proxy-0"
	}

# Consumer Usage

Basic usage with franz-go:

	// Create S3 client
	s3Client, err := lfs.NewS3Client(ctx, lfs.S3Config{
	    Bucket:   "kafscale-lfs",
	    Region:   "us-east-1",
	    Endpoint: "http://minio:9000",  // optional
	})
	if err != nil {
	    log.Fatal(err)
	}

	// Create LFS consumer
	consumer := lfs.NewConsumer(s3Client)

	// Process Kafka records
	for _, record := range kafkaRecords {
	    // Unwrap automatically fetches LFS blobs from S3
	    data, err := consumer.Unwrap(ctx, record.Value)
	    if err != nil {
	        log.Error("failed to unwrap", "error", err)
	        continue
	    }
	    // data contains the original payload (or unchanged if not LFS)
	    processData(data)
	}

# Record Wrapper

For lazy resolution with caching, use the Record wrapper:

	s3Client, _ := lfs.NewS3Client(ctx, config)
	consumer := lfs.NewConsumer(s3Client)

	for _, kafkaRecord := range records {
	    rec := lfs.NewRecord(kafkaRecord.Value, consumer,
	        lfs.WithStreamFetcher(s3Client),  // enables ValueStream()
	    )

	    // Check if this is an LFS record
	    if rec.IsLFS() {
	        // Get size without fetching
	        size, _ := rec.Size()
	        fmt.Printf("LFS blob size: %d\n", size)
	    }

	    // Lazy fetch with caching (second call uses cache)
	    data, err := rec.Value(ctx)
	    if err != nil {
	        log.Error("resolve failed", "error", err)
	        continue
	    }
	    processData(data)
	}

# Streaming Large Files

For memory-efficient processing of large files:

	rec := lfs.NewRecord(value, nil,
	    lfs.WithStreamFetcher(s3Client),
	)

	reader, size, err := rec.ValueStream(ctx)
	if err != nil {
	    log.Fatal(err)
	}
	defer reader.Close()

	// Stream directly to output
	io.Copy(outputFile, reader)

	// Close validates checksum
	if err := reader.Close(); err != nil {
	    log.Error("checksum validation failed", "error", err)
	}

# Checksum Validation

By default, fetched blobs are validated against the SHA256 checksum
stored in the envelope. This can be disabled for performance:

	consumer := lfs.NewConsumer(s3Client,
	    lfs.WithChecksumValidation(false),
	)

# Error Handling

The package defines specific error types for common failures:

	data, err := consumer.Unwrap(ctx, value)
	if err != nil {
	    var checksumErr *lfs.ChecksumError
	    if errors.As(err, &checksumErr) {
	        log.Error("data corruption detected",
	            "expected", checksumErr.Expected,
	            "actual", checksumErr.Actual,
	        )
	    }

	    var lfsErr *lfs.LfsError
	    if errors.As(err, &lfsErr) {
	        log.Error("LFS operation failed",
	            "operation", lfsErr.Op,
	            "error", lfsErr.Err,
	        )
	    }
	}

# Detection

Use IsLfsEnvelope for fast detection without parsing:

	if lfs.IsLfsEnvelope(value) {
	    // This is an LFS envelope
	    env, _ := lfs.DecodeEnvelope(value)
	    fmt.Printf("Blob stored at: s3://%s/%s\n", env.Bucket, env.Key)
	}
*/
package lfs
