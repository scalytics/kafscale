# LFS Proxy Data Flow (Write + Read)

This manual describes the end-to-end data flow for the LFS proxy: how large blobs are written to S3 and how consumers read them back. It covers the Kafka write path (LFS header rewrite), the HTTP write path, and the consumer read path.

## Components

- **LFS Proxy**: Kafka protocol proxy that rewrites records with `LFS_BLOB` headers into pointer envelopes and uploads payloads to S3.
- **S3-Compatible Storage**: AWS S3 or MinIO.
- **Kafka Broker**: Receives pointer records (JSON envelope).
- **Consumer SDK** (`pkg/lfs`): Detects envelopes and fetches objects from S3.

## Object Key Format

Objects are stored with a predictable prefix:

```
<namespace>/<topic>/lfs/YYYY/MM/DD/obj-<uuid>
```

- `namespace` comes from `KAFSCALE_S3_NAMESPACE` (defaults to `default`).
- `topic` is the Kafka topic name.
- The timestamp is UTC.
- `<uuid>` is generated per object.

Implementation reference: `cmd/lfs-proxy/handler.go` (`buildObjectKey`).

## Write Path A: Kafka Produce With `LFS_BLOB` Header

This is the primary path when producers speak Kafka protocol directly.

1) **Client sends Kafka Produce**  
   A producer sends a Produce request containing records. Records intended for LFS include a header `LFS_BLOB` whose value is either:
   - empty string (no checksum enforcement), or
   - a hex SHA-256 checksum to validate the payload.
   Optionally, a `LFS_BLOB_ALG` header may specify the checksum algorithm (planned: sha256/md5/crc32/none).

2) **Proxy accepts Kafka connection**  
   `lfs-proxy` listens on `KAFSCALE_LFS_PROXY_ADDR` (default `:9092`) and parses Kafka frames.  
   Implementation: `cmd/lfs-proxy/handler.go` (`listenAndServe`, `handleConnection`).

3) **Produce request is parsed**  
   The proxy parses the Produce request, locates records, and rewrites them.  
   Implementation: `cmd/lfs-proxy/handler.go` (`handleProduce`, `rewriteProduceRecords`).

4) **Record inspection and LFS detection**  
   Each record is scanned for a `LFS_BLOB` header. If missing, the record is passed through unchanged.  
   Implementation: `cmd/lfs-proxy/handler.go` (`rewriteProduceRecords`).

5) **Blob size enforcement**  
   The payload size is checked against `KAFSCALE_LFS_PROXY_MAX_BLOB_SIZE`. Oversized blobs are rejected.  
   Implementation: `cmd/lfs-proxy/handler.go`.

6) **Upload to S3**  
   The record value (payload) is uploaded to S3 using multipart upload if needed.  
   - S3 config: bucket, region, endpoint, credentials, path style  
   - SHA-256 is computed during upload.  
   Implementation: `cmd/lfs-proxy/s3.go` (`Upload`, `UploadStream`).

7) **Checksum validation (optional)**  
   If the `LFS_BLOB` header contains a checksum, it is compared to the computed SHA-256. Mismatches return an error.  
   Implementation: `cmd/lfs-proxy/handler.go`.

8) **Pointer envelope creation**  
   The record value is replaced with an LFS envelope JSON:
   ```
   {
     "kfs_lfs": 1,
     "bucket": "...",
     "key": "...",
     "size": ...,
     "sha256": "...",
     "content_type": "...",
     "original_headers": {...},
     "created_at": "...",
     "proxy_id": "..."
   }
   ```
   The `LFS_BLOB` header is removed.  
   Implementation: `pkg/lfs/envelope.go`, `cmd/lfs-proxy/handler.go`.

9) **Forward rewritten Produce to Kafka**  
   The proxy connects to a broker (`KAFSCALE_LFS_PROXY_BACKENDS` or metadata from etcd) and forwards the rewritten Produce request.  
   Implementation: `cmd/lfs-proxy/handler.go` (`connectBackend`, `forwardToBackend`).

10) **Metrics and orphan tracking**  
   The proxy records request metrics and upload bytes. If the broker connection fails after upload, it logs and counts orphaned objects.  
   Implementation: `cmd/lfs-proxy/metrics.go`, `cmd/lfs-proxy/handler.go` (`trackOrphans`).

Result: Kafka stores a **small pointer record** instead of the blob. The blob is stored in S3.

## Write Path B: HTTP `/lfs/produce`

This path is for clients that do not speak Kafka protocol.

1) **Client sends HTTP POST**  
   `POST /lfs/produce` with the blob as body and headers:
   - `X-Kafka-Topic` (required)
   - `X-Kafka-Key` (optional, base64)
   - `X-Kafka-Partition` (optional, int)
   - `X-LFS-Checksum` (optional, hex checksum)
   - `X-LFS-Checksum-Alg` (optional, checksum algorithm; planned)

2) **Auth (optional)**  
   If `KAFSCALE_LFS_PROXY_HTTP_API_KEY` is set, the request must include `X-API-Key` or `Authorization: Bearer <key>`.  
   Implementation: `cmd/lfs-proxy/http.go`.

3) **Upload to S3**  
   The body is streamed to S3 with size limits and SHA-256 computed.  
   Implementation: `cmd/lfs-proxy/http.go`, `cmd/lfs-proxy/s3.go`.

4) **Create envelope and produce to Kafka**  
   A single-record Produce request is built and forwarded to the backend broker.  
   Implementation: `cmd/lfs-proxy/http.go`, `cmd/lfs-proxy/record.go`.

Result: Same envelope format as Kafka path; blob stored in S3.

## Read Path (Consumer)

Consumers can detect and hydrate LFS records using `pkg/lfs`.

1) **Consume Kafka records**  
   The consumer receives messages from Kafka as usual.

2) **Detect LFS envelope**  
   Call `lfs.IsLfsEnvelope(value)` to detect LFS records (quick JSON marker check).  
   Implementation: `pkg/lfs/envelope.go`.

3) **Decode envelope**  
   Use `lfs.DecodeEnvelope(value)` to parse fields and validate required fields.  
   Implementation: `pkg/lfs/envelope.go`.

4) **Fetch blob from S3**  
   Use `lfs.NewS3Client` with S3 config and call:
   - `Fetch(ctx, key)` to read all bytes, or
   - `Stream(ctx, key)` to get an `io.ReadCloser` + content length.
   Implementation: `pkg/lfs/s3client.go`.

5) **Verify checksum (recommended)**  
   Compare the retrieved bytes to `env.SHA256` or to `env.Checksum` based on `env.ChecksumAlg` (planned).  
   The SDK does not automatically verify; callers should enforce integrity.

Result: The consumer gets the original blob payload.

## Failure Modes and Signals

- **Upload errors**: The proxy increments `kafscale_lfs_proxy_s3_errors_total` and returns errors to the client.
- **Checksum mismatch**: The proxy returns an error. The object has already been uploaded, so it may be orphaned.
- **Backend failures**: Uploaded objects are tracked as orphans when produce forwarding fails.
- **Metrics**: `kafscale_lfs_proxy_requests_total{topic,status,type}` and upload duration/bytes report LFS activity.

## Quick Reference: Environment Variables

- **Proxy**
  - `KAFSCALE_LFS_PROXY_ADDR` (Kafka listener)
  - `KAFSCALE_LFS_PROXY_HTTP_ADDR` (HTTP listener)
  - `KAFSCALE_LFS_PROXY_HTTP_API_KEY` (optional)
  - `KAFSCALE_LFS_PROXY_BACKENDS` (broker list)
  - `KAFSCALE_LFS_PROXY_ETCD_ENDPOINTS` (metadata store)
  - `KAFSCALE_LFS_PROXY_S3_*` (bucket/region/endpoint/credentials)
  - `KAFSCALE_LFS_PROXY_MAX_BLOB_SIZE`
  - `KAFSCALE_LFS_PROXY_CHUNK_SIZE`
  - `KAFSCALE_S3_NAMESPACE`
  - `KAFSCALE_LFS_PROXY_CHECKSUM_ALGO` (planned; default sha256)

## Checksum Algorithm Options (Planned)

To support workloads that prefer faster corruption detection, the proxy will optionally accept
non-cryptographic checksums (e.g., CRC32) or MD5. The default remains SHA-256 for integrity.

- **Kafka header**: `LFS_BLOB_ALG` (optional)
- **HTTP header**: `X-LFS-Checksum-Alg` (optional)
- **Envelope fields** (planned): `checksum_alg`, `checksum` (with `sha256` preserved for compatibility)

## End-to-End Summary

- Producers send large data with `LFS_BLOB` header or via HTTP.
- LFS proxy stores the blob in S3 and replaces the record value with a compact JSON pointer.
- Consumers detect pointer envelopes, fetch from S3, and verify integrity.

## Flow Chart (Write + Read)

```mermaid
flowchart TD
  A[Producer] -->|Kafka Produce + LFS_BLOB| B[LFS Proxy]
  A2[Client] -->|HTTP /lfs/produce| B
  B -->|Upload blob| S3[(S3/MinIO)]
  B -->|Write pointer record| K[Kafka Broker]
  K --> C[Consumer]
  C -->|Detect envelope| D[SDK: pkg/lfs]
  D -->|Fetch blob| S3
  D -->|Return payload| C
```

## Sequence Diagram (Kafka Write Path)

```mermaid
sequenceDiagram
  participant P as Producer
  participant L as LFS Proxy
  participant S as S3/MinIO
  participant K as Kafka Broker
  P->>L: Produce(records with LFS_BLOB header)
  L->>L: Parse records + detect LFS_BLOB
  L->>S: Upload payload (compute SHA256)
  S-->>L: OK (object key)
  L->>L: Replace value with envelope JSON
  L->>K: Forward rewritten Produce
  K-->>L: Produce response
  L-->>P: Produce response
```

## Sequence Diagram (HTTP Write Path)

```mermaid
sequenceDiagram
  participant C as HTTP Client
  participant L as LFS Proxy
  participant S as S3/MinIO
  participant K as Kafka Broker
  C->>L: POST /lfs/produce (body + headers)
  L->>L: Validate headers + optional API key
  L->>S: Upload stream (compute SHA256)
  S-->>L: OK (object key)
  L->>K: Produce(pointer record)
  K-->>L: Produce response
  L-->>C: 200 + envelope JSON
```

## Sequence Diagram (Read Path)

```mermaid
sequenceDiagram
  participant K as Kafka Broker
  participant C as Consumer
  participant D as SDK: pkg/lfs
  participant S as S3/MinIO
  K-->>C: Record value (envelope JSON)
  C->>D: IsLfsEnvelope + DecodeEnvelope
  D->>S: GetObject (Fetch/Stream)
  S-->>D: Blob bytes
  D-->>C: Blob payload
```
