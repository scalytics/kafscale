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

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type s3Config struct {
	Bucket          string
	Region          string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	ForcePathStyle  bool
	ChunkSize       int64
}

type s3API interface {
	CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
}

type s3Uploader struct {
	bucket    string
	region    string
	chunkSize int64
	api       s3API
}

func newS3Uploader(ctx context.Context, cfg s3Config) (*s3Uploader, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3 bucket required")
	}
	if cfg.Region == "" {
		return nil, errors.New("s3 region required")
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = defaultChunkSize
	}

	loadOpts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
	}
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)))
	}
	if cfg.Endpoint != "" {
		customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{
					URL:           cfg.Endpoint,
					PartitionID:   "aws",
					SigningRegion: cfg.Region,
				}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})
		loadOpts = append(loadOpts, config.WithEndpointResolverWithOptions(customResolver))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.ForcePathStyle
	})

	return &s3Uploader{
		bucket:    cfg.Bucket,
		region:    cfg.Region,
		chunkSize: cfg.ChunkSize,
		api:       client,
	}, nil
}

func (u *s3Uploader) HeadBucket(ctx context.Context) error {
	_, err := u.api.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(u.bucket)})
	if err == nil {
		return nil
	}
	return err
}

func (u *s3Uploader) EnsureBucket(ctx context.Context) error {
	if err := u.HeadBucket(ctx); err == nil {
		return nil
	}
	input := &s3.CreateBucketInput{Bucket: aws.String(u.bucket)}
	if u.region != "" && u.region != "us-east-1" {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{LocationConstraint: types.BucketLocationConstraint(u.region)}
	}
	_, err := u.api.CreateBucket(ctx, input)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
				return nil
			}
		}
		return fmt.Errorf("create bucket %s: %w", u.bucket, err)
	}
	return nil
}

func (u *s3Uploader) Upload(ctx context.Context, key string, payload []byte, alg lfs.ChecksumAlg) (string, string, string, error) {
	if key == "" {
		return "", "", "", errors.New("s3 key required")
	}
	shaHasher := sha256.New()
	if _, err := shaHasher.Write(payload); err != nil {
		return "", "", "", err
	}
	shaHex := hex.EncodeToString(shaHasher.Sum(nil))

	checksumAlg := alg
	if checksumAlg == "" {
		checksumAlg = lfs.ChecksumSHA256
	}
	var checksum string
	if checksumAlg != lfs.ChecksumNone {
		if checksumAlg == lfs.ChecksumSHA256 {
			checksum = shaHex
		} else {
			computed, err := lfs.ComputeChecksum(checksumAlg, payload)
			if err != nil {
				return "", "", "", err
			}
			checksum = computed
		}
	}

	size := int64(len(payload))
	if size <= u.chunkSize {
		_, err := u.api.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(u.bucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(payload),
			ContentLength: aws.Int64(size),
		})
		return shaHex, checksum, string(checksumAlg), err
	}
	return shaHex, checksum, string(checksumAlg), u.multipartUpload(ctx, key, payload)
}

func (u *s3Uploader) UploadStream(ctx context.Context, key string, reader io.Reader, maxSize int64, alg lfs.ChecksumAlg) (string, string, string, int64, error) {
	if key == "" {
		return "", "", "", 0, errors.New("s3 key required")
	}
	if reader == nil {
		return "", "", "", 0, errors.New("reader required")
	}
	if u.chunkSize <= 0 {
		u.chunkSize = defaultChunkSize
	}

	createResp, err := u.api.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", "", "", 0, fmt.Errorf("create multipart upload: %w", err)
	}
	uploadID := createResp.UploadId
	if uploadID == nil {
		return "", "", "", 0, errors.New("missing upload id")
	}

	shaHasher := sha256.New()
	checksumAlg := alg
	if checksumAlg == "" {
		checksumAlg = lfs.ChecksumSHA256
	}
	var checksumHasher interface {
		Write([]byte) (int, error)
		Sum([]byte) []byte
	}
	if checksumAlg != lfs.ChecksumNone {
		if checksumAlg == lfs.ChecksumSHA256 {
			checksumHasher = shaHasher
		} else {
			h, err := lfs.NewChecksumHasher(checksumAlg)
			if err != nil {
				_ = u.abortUpload(ctx, key, *uploadID)
				return "", "", "", 0, err
			}
			checksumHasher = h
		}
	}
	parts := make([]types.CompletedPart, 0, 4)
	buf := make([]byte, u.chunkSize)
	partNum := int32(1)
	var total int64

	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			total += int64(n)
			if maxSize > 0 && total > maxSize {
				_ = u.abortUpload(ctx, key, *uploadID)
				return "", "", "", total, fmt.Errorf("blob size %d exceeds max %d", total, maxSize)
			}
			if _, err := shaHasher.Write(buf[:n]); err != nil {
				_ = u.abortUpload(ctx, key, *uploadID)
				return "", "", "", total, err
			}
			if checksumHasher != nil && checksumHasher != shaHasher {
				if _, err := checksumHasher.Write(buf[:n]); err != nil {
					_ = u.abortUpload(ctx, key, *uploadID)
					return "", "", "", total, err
				}
			}
			partResp, err := u.api.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:     aws.String(u.bucket),
				Key:        aws.String(key),
				UploadId:   uploadID,
				PartNumber: aws.Int32(partNum),
				Body:       bytes.NewReader(buf[:n]),
			})
			if err != nil {
				_ = u.abortUpload(ctx, key, *uploadID)
				return "", "", "", total, fmt.Errorf("upload part %d: %w", partNum, err)
			}
			parts = append(parts, types.CompletedPart{ETag: partResp.ETag, PartNumber: aws.Int32(partNum)})
			partNum++
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			_ = u.abortUpload(ctx, key, *uploadID)
			return "", "", "", total, readErr
		}
	}

	if len(parts) == 0 {
		_ = u.abortUpload(ctx, key, *uploadID)
		return "", "", "", total, errors.New("empty upload")
	}

	_, err = u.api.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(key),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		_ = u.abortUpload(ctx, key, *uploadID)
		return "", "", "", total, fmt.Errorf("complete multipart upload: %w", err)
	}
	shaHex := hex.EncodeToString(shaHasher.Sum(nil))
	checksum := ""
	if checksumAlg != lfs.ChecksumNone {
		if checksumAlg == lfs.ChecksumSHA256 {
			checksum = shaHex
		} else if checksumHasher != nil {
			checksum = hex.EncodeToString(checksumHasher.Sum(nil))
		}
	}
	return shaHex, checksum, string(checksumAlg), total, nil
}

func (u *s3Uploader) multipartUpload(ctx context.Context, key string, payload []byte) error {
	createResp, err := u.api.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("create multipart upload: %w", err)
	}
	uploadID := createResp.UploadId
	if uploadID == nil {
		return errors.New("missing upload id")
	}

	parts := make([]types.CompletedPart, 0, (len(payload)/int(u.chunkSize))+1)
	reader := bytes.NewReader(payload)
	partNum := int32(1)
	buf := make([]byte, u.chunkSize)
	for {
		n, readErr := io.ReadFull(reader, buf)
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			if n == 0 {
				break
			}
		}
		if n > 0 {
			partResp, err := u.api.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:     aws.String(u.bucket),
				Key:        aws.String(key),
				UploadId:   uploadID,
				PartNumber: aws.Int32(partNum),
				Body:       bytes.NewReader(buf[:n]),
			})
			if err != nil {
				_ = u.abortUpload(ctx, key, *uploadID)
				return fmt.Errorf("upload part %d: %w", partNum, err)
			}
			parts = append(parts, types.CompletedPart{ETag: partResp.ETag, PartNumber: aws.Int32(partNum)})
			partNum++
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			_ = u.abortUpload(ctx, key, *uploadID)
			return fmt.Errorf("read payload: %w", readErr)
		}
		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}

	_, err = u.api.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(key),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		_ = u.abortUpload(ctx, key, *uploadID)
		return fmt.Errorf("complete multipart upload: %w", err)
	}
	return nil
}

func (u *s3Uploader) abortUpload(ctx context.Context, key, uploadID string) error {
	_, err := u.api.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	return err
}

func (u *s3Uploader) DeleteObject(ctx context.Context, key string) error {
	if key == "" {
		return errors.New("s3 key required")
	}
	_, err := u.api.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
	})
	return err
}
