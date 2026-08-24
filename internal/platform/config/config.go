// Package config loads and validates the process configuration.
package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Endpoint, Region, InputBucket, IntakeQueueURL, ChunkQueueURL, OutputQueueURL string
	RecordLength, RecordsPerChunk, BatchSize, WorkerConcurrency                  int
}

func Load() (Config, error) {
	c := Config{Endpoint: os.Getenv("AWS_ENDPOINT_URL"), Region: value("AWS_REGION", "us-east-1"), InputBucket: value("F2E_INPUT_BUCKET", "f2e-input"), IntakeQueueURL: os.Getenv("F2E_INTAKE_QUEUE_URL"), ChunkQueueURL: os.Getenv("F2E_CHUNK_QUEUE_URL"), OutputQueueURL: os.Getenv("F2E_OUTPUT_QUEUE_URL")}
	var err error
	if c.RecordLength, err = integer("F2E_RECORD_LENGTH", 100); err != nil {
		return c, err
	}
	if c.RecordsPerChunk, err = integer("F2E_RECORDS_PER_CHUNK", 1000); err != nil {
		return c, err
	}
	if c.BatchSize, err = integer("F2E_BATCH_SIZE", 10); err != nil {
		return c, err
	}
	if c.WorkerConcurrency, err = integer("F2E_WORKER_CONCURRENCY", 4); err != nil {
		return c, err
	}
	if c.RecordLength < 2 || c.RecordsPerChunk < 1 || c.BatchSize < 1 || c.BatchSize > 10 || c.WorkerConcurrency < 1 {
		return c, fmt.Errorf("invalid F2E numeric configuration")
	}
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
