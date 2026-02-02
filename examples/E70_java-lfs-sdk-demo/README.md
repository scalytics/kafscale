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

# Java LFS SDK Demo (E70)

This demo shows how to use the Java LFS SDK to:
- Produce a blob via the LFS proxy HTTP API.
- Consume the pointer record via Kafka.
- Resolve the blob from S3/MinIO.

## Prerequisites

1. **Bring up the LFS demo stack** (keeps the cluster running for E70/E71):

```bash
make lfs-demo-video
```

2. **Port-forward the services** (separate terminal):

```bash
kubectl -n kafscale-video port-forward svc/lfs-proxy 8080:8080
kubectl -n kafscale-video port-forward svc/kafscale-broker 9092:9092
kubectl -n kafscale-video port-forward svc/minio 9000:9000
```

3. **Build + install the Java SDK locally**:

```bash
cd lfs-client-sdk/java
mvn -DskipTests install
```

## Run the Demo

```bash
cd examples/E70_java-lfs-sdk-demo
mvn -DskipTests package
mvn -Dexec.mainClass=io.kafscale.examples.lfs.E70JavaLfsDemo exec:java
```

## Environment Variables

| Variable | Default | Description |
| --- | --- | --- |
| `LFS_HTTP_ENDPOINT` | `http://localhost:8080/lfs/produce` | LFS proxy HTTP endpoint |
| `LFS_TOPIC` | `video-raw` | Kafka topic for pointer records |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap address |
| `S3_BUCKET` | `kafscale-lfs` | Bucket used by the LFS proxy |
| `S3_ENDPOINT` | `http://localhost:9000` | MinIO endpoint |
| `S3_REGION` | `us-east-1` | S3 region |
| `S3_PATH_STYLE` | `true` | Use path-style S3 addressing |
| `AWS_ACCESS_KEY_ID` | `minioadmin` | MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | `minioadmin` | MinIO secret key |

## Expected Output

```
Produced envelope: key=... sha256=...
Resolved record: isEnvelope=true payloadBytes=...
```

## Cleanup

Stop port-forwards when done. The cluster remains running until you delete it or run cleanup scripts.
