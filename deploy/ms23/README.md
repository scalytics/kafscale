# MS23 Kind Deployment

Decoupled Kind-based deployment for milestone MS23. This setup separates system infrastructure from development, providing a clean Kubernetes environment for testing KafScale with LFS support.

## Prerequisites

1. **Kind** - Kubernetes in Docker
   ```bash
   brew install kind
   ```

2. **kubectl** - Kubernetes CLI
   ```bash
   brew install kubectl
   ```

3. **Helm** - Kubernetes package manager
   ```bash
   brew install helm
   ```

4. **Local Registry** - Images available at `192.168.0.131:5100`
   ```bash
   # Verify registry
   curl http://192.168.0.131:5100/v2/_catalog
   ```

5. **Docker** - With insecure registry configured
   ```json
   {
     "insecure-registries": ["192.168.0.131:5100"]
   }
   ```

## Quick Start

```bash
cd deploy/ms23

# Deploy everything
make up

# Check status
make status

# View logs
make logs
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Kind Cluster: ks-ms23                         │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                  Namespace: ks-ms23-01                       ││
│  │                                                              ││
│  │  ┌──────────┐  ┌──────────┐  ┌───────────┐  ┌────────────┐ ││
│  │  │ Operator │  │  Broker  │  │ LFS Proxy │  │  Console   │ ││
│  │  └──────────┘  └──────────┘  └───────────┘  └────────────┘ ││
│  │                     │              │                        ││
│  │                     └──────┬───────┘                        ││
│  │                            │                                ││
│  │  ┌──────────┐        ┌─────┴─────┐        ┌──────────────┐ ││
│  │  │   etcd   │        │   MinIO   │        │ E72 Browser  │ ││
│  │  │ (in-mem) │        │ (S3 API)  │        │    Demo      │ ││
│  │  └──────────┘        └───────────┘        └──────────────┘ ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                  Namespace: metallb-system                   ││
│  │                                                              ││
│  │  ┌────────────┐         ┌─────────┐                         ││
│  │  │ Controller │         │ Speaker │                         ││
│  │  └────────────┘         └─────────┘                         ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              │               │               │
         localhost:8080  localhost:3080  localhost:3072
         (LFS HTTP)      (Console)       (E72 Demo)
```

## Access Points

| Service | URL | Description |
|---------|-----|-------------|
| LFS HTTP API | http://localhost:8080 | Upload/download blobs |
| KafScale Console | http://localhost:3080 | Web management UI |
| E72 Browser Demo | http://localhost:3072 | Browser upload demo |
| MinIO Console | http://localhost:9001 | S3 storage UI |
| Kafka Broker | localhost:9092 | Native Kafka protocol |

## Makefile Targets

| Target | Description |
|--------|-------------|
| `make up` | Full deployment (cluster + all services) |
| `make down` | Delete Kind cluster |
| `make clean` | Delete cluster and clean up volumes |
| `make status` | Show cluster and service status |
| `make logs` | Follow operator logs |
| `make logs-lfs` | Follow LFS proxy logs |
| `make logs-broker` | Follow broker logs |
| `make test-upload` | Test LFS upload with sample data |

## Configuration

### Namespace

All KafScale resources are deployed in namespace `ks-ms23-01`.

### Images

Images are pulled from the local registry:

| Component | Image |
|-----------|-------|
| Operator | `192.168.0.131:5100/kafscale/kafscale-operator:dev` |
| Broker | `192.168.0.131:5100/kafscale/kafscale-broker:dev` |
| LFS Proxy | `192.168.0.131:5100/kafscale/kafscale-lfs-proxy:dev` |
| Console | `192.168.0.131:5100/kafscale/kafscale-console:dev` |

### MinIO Credentials

- **User**: minioadmin
- **Password**: minioadmin
- **Bucket**: kafscale

## Testing

### LFS Upload

```bash
# Via Makefile
make test-upload

# Manual upload
curl -X POST http://localhost:8080/lfs/produce \
  -H "X-Kafka-Topic: test-uploads" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @myfile.bin
```

### E72 Browser Demo

1. Open http://localhost:3072
2. Configure endpoint: `http://localhost:8080/lfs/produce`
3. Drag & drop a file to upload
4. View uploaded files in MinIO Console

### Kafka (via kcat)

```bash
# List topics
kcat -b localhost:9092 -L

# Consume messages
kcat -b localhost:9092 -C -t test-uploads -o beginning
```

## Troubleshooting

### Cluster not starting

```bash
# Check Docker is running
docker info

# Check for port conflicts
lsof -i :9092
lsof -i :8080
```

### Pods not starting

```bash
# Check pod status
kubectl get pods -n ks-ms23-01

# Describe failing pod
kubectl describe pod -n ks-ms23-01 <pod-name>

# Check events
kubectl get events -n ks-ms23-01 --sort-by='.lastTimestamp'
```

### Image pull errors

```bash
# Verify registry is accessible
curl http://192.168.0.131:5100/v2/_catalog

# Check Docker insecure registries
docker info | grep -A5 "Insecure Registries"
```

### Reset everything

```bash
make clean
make up
```

## Files

| File | Purpose |
|------|---------|
| `Makefile` | Build and deployment automation |
| `kind-config.yaml` | Kind cluster configuration |
| `values.yaml` | Helm values for KafScale |
| `minio.yaml` | MinIO deployment manifests |
| `metallb-config.yaml` | MetalLB IP address pool |
