from __future__ import annotations

import os
import time
from datetime import datetime

import boto3
from botocore.config import Config
from confluent_kafka import Consumer

from lfs_sdk.producer import produce_lfs
from lfs_sdk.resolver import LfsResolver


def env(key: str, default: str) -> str:
    value = os.getenv(key)
    return value if value else default


def main() -> None:
    http_endpoint = env("LFS_HTTP_ENDPOINT", "http://localhost:8080/lfs/produce")
    topic = env("LFS_TOPIC", "video-raw")
    bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092")
    bucket = env("S3_BUCKET", "kafscale-lfs")
    s3_endpoint = env("S3_ENDPOINT", "http://localhost:9000")
    s3_region = env("S3_REGION", "us-east-1")

    payload = f"hello-lfs-{datetime.utcnow().isoformat()}".encode("utf-8")
    envelope = produce_lfs(http_endpoint, topic, payload, headers={"content-type": "text/plain"})
    print(f"Produced envelope: key={envelope.get('key')} sha256={envelope.get('sha256')}")

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "e71-python-lfs-demo",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])

    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        region_name=s3_region,
        config=Config(s3={"addressing_style": "path"}),
    )
    resolver = LfsResolver(bucket=bucket, s3_client=s3_client)

    deadline = time.time() + 30
    resolved = False
    try:
        while time.time() < deadline:
            msg = consumer.poll(2.0)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(msg.error())
            record = resolver.resolve(msg.value())
            print(f"Resolved record: is_envelope={record.is_envelope} payload_bytes={len(record.payload)}")
            resolved = True
            break
    finally:
        consumer.close()

    if not resolved:
        print("No records resolved within timeout.")


if __name__ == "__main__":
    main()
