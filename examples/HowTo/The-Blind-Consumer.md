<!--
Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
This project is supported and financed by Scalytics, Inc. (www.scalytics.io).

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# The Blind Consumer: Debugging End-to-End LFS Flow

This article documents common issues when the LFS consumer receives no records despite successful HTTP produces. These "blind consumer" scenarios can be frustrating because the producer reports success, but the consumer sees nothing.

## Symptoms

```
Produced envelope: key=default/lfs-demo-topic/lfs/2026/02/03/obj-abc123... sha256=...
No records resolved within timeout.
```

The HTTP produce returns a valid envelope with S3 key and SHA256, but the Kafka consumer polls indefinitely without receiving any records.

## Root Causes

### 1. Empty Upload Error (Java SDK)

**Symptom**: LFS proxy returns `400 Bad Request` with `"code":"empty_upload"`.

**Root Cause**: Java's `HttpClient.BodyPublishers.ofInputStream()` uses chunked transfer encoding and doesn't set `Content-Length`. The proxy may receive an empty body or the stream may be consumed incorrectly.

**Fix**: Read the InputStream into a byte array first, then use `BodyPublishers.ofByteArray()`:

```java
// Before (broken)
HttpRequest.BodyPublishers.ofInputStream(() -> payload)

// After (working)
byte[] data = payload.readAllBytes();
HttpRequest.BodyPublishers.ofByteArray(data)
```

**Location**: [LfsProducer.java:68-90](../lfs-client-sdk/java/src/main/java/org/kafscale/lfs/LfsProducer.java#L68-L90)

### 2. S3 EntityTooSmall Error

**Symptom**: LFS proxy returns `502 Bad Gateway` with S3 error `EntityTooSmall: Your proposed upload is smaller than the minimum allowed object size`.

**Root Cause**: S3 multipart upload requires a minimum of 5MB per part. If the LFS proxy always uses multipart upload (even for small files), S3 rejects parts smaller than 5MB.

**Fix**: Use `PutObject` for files smaller than 5MB, multipart upload for larger files:

```go
// Check if data fits in one chunk and is smaller than minMultipartChunkSize (5MB)
if !hasMoreData && int64(firstN) < minMultipartChunkSize {
    // Use simple PutObject
    _, err := u.api.PutObject(ctx, &s3.PutObjectInput{...})
} else {
    // Use multipart upload for larger files
    createResp, err := u.api.CreateMultipartUpload(ctx, ...)
}
```

**Location**: [s3.go:225-255](../cmd/lfs-proxy/s3.go#L225-L255)

### 3. Kafka Broker Advertised Address Mismatch

**Symptom**: HTTP produce succeeds (S3 blob created), but Kafka consumer gets no records. Direct Kafka producer times out with `Topic not present in metadata after 60000 ms`.

**Root Cause**: The KafscaleCluster is configured with an internal advertised address (e.g., `127.0.0.1:39092`) that clients cannot reach through port-forward (`localhost:9092`).

When a Kafka client connects:
1. Client connects to `localhost:9092` (via port-forward)
2. Broker returns metadata: "connect to `127.0.0.1:39092`"
3. Client tries to connect to `127.0.0.1:39092` - nothing listening there
4. Client times out waiting for metadata

**Diagnosis**:

```bash
# Check advertised address in etcd metadata
kubectl -n kafscale-demo exec kafscale-etcd-0 -- \
  etcdctl get /kafscale/metadata/snapshot | grep -o '"Brokers":\[[^]]*\]'

# If it shows wrong port (e.g., 39092 instead of 9092):
# "Brokers":[{"NodeID":0,"Host":"127.0.0.1","Port":39092,"Rack":null}]
```

**Fix**: Patch the KafscaleCluster to advertise the correct address:

```bash
kubectl -n kafscale-demo patch kafscalecluster kafscale --type=merge \
  -p '{"spec":{"brokers":{"advertisedHost":"localhost","advertisedPort":9092}}}'

# Restart broker to pick up new config
kubectl -n kafscale-demo rollout restart statefulset/kafscale-broker
kubectl -n kafscale-demo rollout status statefulset/kafscale-broker --timeout=120s

# Restart port-forward
pkill -f "port-forward.*9092"
kubectl -n kafscale-demo port-forward svc/kafscale-broker 9092:9092 &
```

**Verify fix**:

```bash
# Should now show localhost:9092
kubectl -n kafscale-demo exec kafscale-etcd-0 -- \
  etcdctl get /kafscale/metadata/snapshot | grep -o '"Brokers":\[[^]]*\]'
# "Brokers":[{"NodeID":0,"Host":"localhost","Port":9092,"Rack":null}]
```

## Debugging Checklist

When the consumer sees no records, check in this order:

### 1. Verify S3 Blob Exists

```bash
kubectl -n kafscale-demo exec deployment/minio -- sh -c \
  'mc alias set local http://localhost:9000 minioadmin minioadmin && mc ls local/kafscale/ --recursive'
```

If blobs exist but no Kafka records, the issue is between S3 upload success and Kafka produce.

### 2. Check Broker Connectivity

```bash
# Test direct Kafka connection
nc -z localhost 9092 && echo "Port reachable" || echo "Port not reachable"
```

### 3. Verify Metadata Advertised Address

```bash
kubectl -n kafscale-demo exec kafscale-etcd-0 -- \
  etcdctl get /kafscale/metadata/snapshot | python3 -m json.tool | grep -A5 '"Brokers"'
```

The `Host` and `Port` must match what clients use to connect (typically `localhost:9092` for local development).

### 4. Test Direct Kafka Produce

Create a simple test producer to isolate Kafka issues from LFS proxy:

```java
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");

try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
    Future<RecordMetadata> future = producer.send(
        new ProducerRecord<>("lfs-demo-topic", "test", "message"));
    RecordMetadata meta = future.get(15, TimeUnit.SECONDS);
    System.out.println("SUCCESS: offset=" + meta.offset());
}
```

### 5. Test Direct Kafka Consume

```java
Properties props = new Properties();
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + System.currentTimeMillis());
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
    consumer.subscribe(List.of("lfs-demo-topic"));
    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(10));
    System.out.println("Found " + records.count() + " records");
}
```

## Architecture Reference

```
┌─────────────────┐     HTTP POST      ┌─────────────────┐
│  Java Client    │ ─────────────────► │  LFS Proxy      │
│  (LfsProducer)  │    /lfs/produce    │                 │
└─────────────────┘                    └────────┬────────┘
                                                │
                    ┌───────────────────────────┼───────────────────────────┐
                    │                           │                           │
                    ▼                           ▼                           │
            ┌───────────────┐           ┌───────────────┐                   │
            │   MinIO/S3    │           │    Kafka      │                   │
            │  (blob data)  │           │  (pointer)    │ ◄── Issue here!   │
            └───────────────┘           └───────┬───────┘                   │
                    ▲                           │                           │
                    │                           ▼                           │
                    │                   ┌───────────────┐                   │
                    │ ◄─────────────────│  LfsConsumer  │                   │
                    │    fetch blob     │               │                   │
                    │                   └───────────────┘                   │
                    └───────────────────────────────────────────────────────┘
```

The "blind consumer" issue occurs when:
- S3 upload succeeds (blob stored)
- Kafka produce fails silently (due to connectivity/metadata issues)
- Consumer polls Kafka but finds no records
- Consumer cannot resolve blob because it never received the pointer

## Prevention

1. **For Java SDK users**: Always use `BodyPublishers.ofByteArray()` instead of `ofInputStream()` for HTTP requests that need proper Content-Length.

2. **For S3 uploads**: Implement size-based routing - use `PutObject` for small files, multipart for large files.

3. **For local development**: When starting the demo stack, ensure the advertised address matches the port-forward configuration:

```bash
# Start with correct advertised address for local dev
KAFSCALE_DEMO_ADVERTISED_HOST=localhost \
KAFSCALE_DEMO_ADVERTISED_PORT=9092 \
make lfs-demo
```

4. **Always verify end-to-end**: Don't trust HTTP 200 alone. Verify the consumer can actually retrieve the data:

```
Produced envelope: key=... sha256=...
Resolved record: isEnvelope=true payloadBytes=524288  ◄── This confirms success
```
