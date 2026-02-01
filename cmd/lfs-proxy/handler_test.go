package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type fakeS3API struct{}

func (fakeS3API) CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String("upload")}, nil
}
func (fakeS3API) UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return &s3.UploadPartOutput{ETag: aws.String("etag")}, nil
}
func (fakeS3API) CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return &s3.CompleteMultipartUploadOutput{}, nil
}
func (fakeS3API) AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return &s3.AbortMultipartUploadOutput{}, nil
}
func (fakeS3API) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return &s3.PutObjectOutput{}, nil
}
func (fakeS3API) HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	return &s3.HeadBucketOutput{}, nil
}
func (fakeS3API) CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	return &s3.CreateBucketOutput{}, nil
}

type failingS3API struct {
	err error
}

func (f failingS3API) CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return nil, f.err
}
func (f failingS3API) UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return nil, f.err
}
func (f failingS3API) CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return nil, f.err
}
func (f failingS3API) AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return nil, f.err
}
func (f failingS3API) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, f.err
}
func (f failingS3API) HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	return nil, f.err
}
func (f failingS3API) CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	return nil, f.err
}

func TestRewriteProduceRecords(t *testing.T) {
	proxy := &lfsProxy{
		s3Uploader:  &s3Uploader{bucket: "bucket", chunkSize: 1024, api: fakeS3API{}},
		s3Bucket:    "bucket",
		s3Namespace: "ns",
		maxBlob:     1024 * 1024,
		proxyID:     "proxy-1",
		metrics:     newLfsMetrics(),
	}

	rec := kmsg.Record{
		TimestampDelta64: 0,
		OffsetDelta:      0,
		Value:            []byte("payload"),
		Headers: []kmsg.Header{
			{Key: "LFS_BLOB", Value: nil},
			{Key: "content-type", Value: []byte("application/octet-stream")},
		},
	}
	batchBytes := buildRecordBatch([]kmsg.Record{rec})

	req := &protocol.ProduceRequest{
		Acks:      1,
		TimeoutMs: 1000,
		Topics: []protocol.ProduceTopic{
			{
				Name: "topic",
				Partitions: []protocol.ProducePartition{{
					Partition: 0,
					Records:   batchBytes,
				}},
			},
		},
	}
	header := &protocol.RequestHeader{
		APIKey:        protocol.APIKeyProduce,
		APIVersion:    9,
		CorrelationID: 1,
		ClientID:      strPtr("client"),
	}

	result, err := proxy.rewriteProduceRecords(context.Background(), header, req)
	if err != nil {
		t.Fatalf("rewriteProduceRecords error: %v", err)
	}
	if !result.modified {
		t.Fatalf("expected modified payload")
	}
	parsedHeader, parsedReq, err := protocol.ParseRequest(result.payload)
	if err != nil {
		t.Fatalf("parse rewritten request: %v", err)
	}
	if parsedHeader.APIKey != protocol.APIKeyProduce {
		t.Fatalf("unexpected api key %d", parsedHeader.APIKey)
	}
	prodReq := parsedReq.(*protocol.ProduceRequest)
	batches, err := decodeRecordBatches(prodReq.Topics[0].Partitions[0].Records)
	if err != nil {
		t.Fatalf("decode record batches: %v", err)
	}
	records, _, err := decodeBatchRecords(&batches[0], kgo.DefaultDecompressor())
	if err != nil {
		t.Fatalf("decode records: %v", err)
	}
	var env lfs.Envelope
	if err := json.Unmarshal(records[0].Value, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.Bucket != "bucket" || env.Key == "" || env.Version != 1 {
		t.Fatalf("unexpected envelope: %+v", env)
	}
}

func TestRewriteProduceRecordsPassthrough(t *testing.T) {
	proxy := &lfsProxy{
		s3Uploader:  &s3Uploader{bucket: "bucket", chunkSize: 1024, api: fakeS3API{}},
		s3Bucket:    "bucket",
		s3Namespace: "ns",
		maxBlob:     1024 * 1024,
		metrics:     newLfsMetrics(),
	}

	rec := kmsg.Record{
		TimestampDelta64: 0,
		OffsetDelta:      0,
		Value:            []byte("payload"),
		Headers:          nil,
	}
	batchBytes := buildRecordBatch([]kmsg.Record{rec})

	req := &protocol.ProduceRequest{
		Acks:      1,
		TimeoutMs: 1000,
		Topics: []protocol.ProduceTopic{
			{
				Name: "topic",
				Partitions: []protocol.ProducePartition{{
					Partition: 0,
					Records:   batchBytes,
				}},
			},
		},
	}
	header := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9, CorrelationID: 1}

	result, err := proxy.rewriteProduceRecords(context.Background(), header, req)
	if err != nil {
		t.Fatalf("rewriteProduceRecords error: %v", err)
	}
	if result.modified {
		t.Fatalf("expected passthrough")
	}
}

func TestRewriteProduceRecordsS3Failure(t *testing.T) {
	proxy := &lfsProxy{
		s3Uploader:  &s3Uploader{bucket: "bucket", chunkSize: 1024, api: failingS3API{err: errors.New("boom")}},
		s3Bucket:    "bucket",
		s3Namespace: "ns",
		maxBlob:     1024 * 1024,
		metrics:     newLfsMetrics(),
	}

	rec := kmsg.Record{
		TimestampDelta64: 0,
		OffsetDelta:      0,
		Value:            []byte("payload"),
		Headers:          []kmsg.Header{{Key: "LFS_BLOB", Value: nil}},
	}
	batchBytes := buildRecordBatch([]kmsg.Record{rec})

	req := &protocol.ProduceRequest{
		Acks:      1,
		TimeoutMs: 1000,
		Topics: []protocol.ProduceTopic{{
			Name: "topic",
			Partitions: []protocol.ProducePartition{{
				Partition: 0,
				Records:   batchBytes,
			}},
		}},
	}
	header := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9, CorrelationID: 1}

	_, err := proxy.rewriteProduceRecords(context.Background(), header, req)
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestRewriteProduceRecordsChecksumMismatch(t *testing.T) {
	proxy := &lfsProxy{
		s3Uploader:  &s3Uploader{bucket: "bucket", chunkSize: 1024, api: fakeS3API{}},
		s3Bucket:    "bucket",
		s3Namespace: "ns",
		maxBlob:     1024 * 1024,
		metrics:     newLfsMetrics(),
	}

	rec := kmsg.Record{
		TimestampDelta64: 0,
		OffsetDelta:      0,
		Value:            []byte("payload"),
		Headers:          []kmsg.Header{{Key: "LFS_BLOB", Value: []byte("deadbeef")}},
	}
	batchBytes := buildRecordBatch([]kmsg.Record{rec})

	req := &protocol.ProduceRequest{
		Acks:      1,
		TimeoutMs: 1000,
		Topics: []protocol.ProduceTopic{{
			Name: "topic",
			Partitions: []protocol.ProducePartition{{
				Partition: 0,
				Records:   batchBytes,
			}},
		}},
	}
	header := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9, CorrelationID: 1}

	_, err := proxy.rewriteProduceRecords(context.Background(), header, req)
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestRewriteProduceRecordsMaxBlobSize(t *testing.T) {
	proxy := &lfsProxy{
		s3Uploader:  &s3Uploader{bucket: "bucket", chunkSize: 1024, api: fakeS3API{}},
		s3Bucket:    "bucket",
		s3Namespace: "ns",
		maxBlob:     3,
		metrics:     newLfsMetrics(),
	}

	rec := kmsg.Record{
		TimestampDelta64: 0,
		OffsetDelta:      0,
		Value:            []byte("payload"),
		Headers:          []kmsg.Header{{Key: "LFS_BLOB", Value: nil}},
	}
	batchBytes := buildRecordBatch([]kmsg.Record{rec})

	req := &protocol.ProduceRequest{
		Acks:      1,
		TimeoutMs: 1000,
		Topics: []protocol.ProduceTopic{{
			Name: "topic",
			Partitions: []protocol.ProducePartition{{
				Partition: 0,
				Records:   batchBytes,
			}},
		}},
	}
	header := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9, CorrelationID: 1}

	_, err := proxy.rewriteProduceRecords(context.Background(), header, req)
	if err == nil {
		t.Fatalf("expected error")
	}
}

func strPtr(v string) *string { return &v }
