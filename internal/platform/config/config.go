// Package config loads and validates the process configuration.
package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Endpoint, Region, InputBucket, IntakeQueueURL, ChunkQueueURL, OutputQueueURL string
	// CompletionQueueURL is the SQS queue where the completion-publisher Lambda
	// sends technical conclusion events. It is separate from the record-envelope
	// output queue so consumers can subscribe independently.
	CompletionQueueURL                             string
	LedgerTable                                    string
	Environment                                    string
	FileConfigPath                                 string
	GlobalLimitsParameter                          string
	RecordsPerChunk, BatchSize, MaxReceiveCount    int
	LedgerRetentionDays                            int
	MaxEventBytes                                  int
	JSONArraySearchBytes                           int
	MaxFileBytes, MaxChunkBytes, TargetChunkBytes  int64
	EventSchemaID, EventSchemaVersion, EventFormat string
	// PublishConcurrency is the maximum number of SQS SendMessageBatch calls
	// that may be in-flight simultaneously within a single chunk processing.
	// Default is 1 (sequential). Values 2–16 are valid; higher is capped to 16.
	PublishConcurrency int
}

func Load() (Config, error) {
	c := Config{Endpoint: os.Getenv("AWS_ENDPOINT_URL"), Region: value("AWS_REGION", "us-east-1"), InputBucket: value("F2E_INPUT_BUCKET", "f2e-input"), IntakeQueueURL: os.Getenv("F2E_INTAKE_QUEUE_URL"), ChunkQueueURL: os.Getenv("F2E_CHUNK_QUEUE_URL"), OutputQueueURL: os.Getenv("F2E_OUTPUT_QUEUE_URL")}
	c.LedgerTable = os.Getenv("F2E_LEDGER_TABLE")
	c.Environment = value("F2E_ENVIRONMENT", "local")
	c.FileConfigPath = value("F2E_FILE_CONFIG_PATH", "/f2e/"+c.Environment+"/file-config")
	c.GlobalLimitsParameter = value("F2E_GLOBAL_LIMITS_PARAMETER", "/f2e/"+c.Environment+"/global-limits")
	var err error
	if c.RecordsPerChunk, err = integer("F2E_RECORDS_PER_CHUNK", 1000); err != nil {
		return c, err
	}
	if c.BatchSize, err = integer("F2E_BATCH_SIZE", 10); err != nil {
		return c, err
	}
	if c.MaxEventBytes, err = integer("F2E_MAX_EVENT_BYTES", 256*1024); err != nil {
		return c, err
	}
	if c.MaxReceiveCount, err = integer("F2E_MAX_RECEIVE_COUNT", 3); err != nil {
		return c, err
	}
	if c.JSONArraySearchBytes, err = integer("F2E_JSON_ARRAY_SEARCH_BYTES", 1024*1024); err != nil {
		return c, err
	}
	if c.LedgerRetentionDays, err = integer("F2E_LEDGER_RETENTION_DAYS", 90); err != nil {
		return c, err
	}
	if c.MaxFileBytes, err = integer64("F2E_MAX_FILE_BYTES", 10*1024*1024*1024); err != nil {
		return c, err
	}
	if c.MaxChunkBytes, err = integer64("F2E_MAX_CHUNK_BYTES", 64*1024*1024); err != nil {
		return c, err
	}
	if c.TargetChunkBytes, err = integer64("F2E_TARGET_CHUNK_BYTES", 0); err != nil {
		return c, err
	}
	if c.PublishConcurrency, err = integer("F2E_PUBLISH_CONCURRENCY", 1); err != nil {
		return c, err
	}
	if c.RecordsPerChunk < 1 || c.BatchSize < 1 || c.BatchSize > 10 || c.MaxReceiveCount < 1 || c.LedgerRetentionDays < 1 || c.MaxEventBytes < 1024 || c.MaxEventBytes > 256*1024 || c.JSONArraySearchBytes < 1024 || c.JSONArraySearchBytes > 16*1024*1024 || c.MaxChunkBytes < 1024 || c.MaxFileBytes < c.MaxChunkBytes || (c.TargetChunkBytes != 0 && (c.TargetChunkBytes < 5*1024*1024 || c.TargetChunkBytes > 100*1024*1024 || c.TargetChunkBytes > c.MaxChunkBytes)) {
		return c, fmt.Errorf("invalid F2E numeric configuration")
	}
	if c.PublishConcurrency < 1 {
		c.PublishConcurrency = 1
	} else if c.PublishConcurrency > 16 {
		c.PublishConcurrency = 16
	}
	c.EventSchemaID = value("F2E_EVENT_SCHEMA_ID", "f2e-record")
	c.EventSchemaVersion = value("F2E_EVENT_SCHEMA_VERSION", "1")
	c.EventFormat = value("F2E_EVENT_FORMAT", "json")
	return c, nil
}
func value(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func integer(k string, d int) (int, error) {
	v := value(k, strconv.Itoa(d))
	n, e := strconv.Atoi(v)
	if e != nil {
		return 0, fmt.Errorf("%s: %w", k, e)
	}
	return n, nil
}

func integer64(k string, d int64) (int64, error) {
	v := value(k, strconv.FormatInt(d, 10))
	n, e := strconv.ParseInt(v, 10, 64)
	if e != nil {
		return 0, fmt.Errorf("%s: %w", k, e)
	}
	return n, nil
}
