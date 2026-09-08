package organizer

import (
	"strings"
	"testing"

	"github.com/f2e/f2e/internal/domain/f2e"
)

// baseGlobalLimits returns a valid GlobalLimits for use in validation tests.
func baseGlobalLimits() f2e.GlobalLimits {
	return f2e.GlobalLimits{
		MaxFileBytes:            512 * 1024 * 1024,
		MaxChunkBytes:           64 * 1024 * 1024,
		MaxEventBytes:           256 * 1024,
		MaxBatchSize:            10,
		MaxJSONArraySearchBytes: 16 * 1024 * 1024,
		InputTypes: map[f2e.DataType]f2e.InputTypeLimits{
			f2e.DataTypeText:      {MaxFileBytes: 512 * 1024 * 1024, MaxRecordBytes: 256 * 1024},
			f2e.DataTypeJSON:      {MaxFileBytes: 512 * 1024 * 1024, MaxRecordBytes: 256 * 1024},
			f2e.DataTypeMultiLine: {MaxFileBytes: 512 * 1024 * 1024, MaxRecordBytes: 256 * 1024},
		},
	}
}

// basePrefixConfig returns a valid PrefixConfiguration for use in tests.
func basePrefixConfig() f2e.PrefixConfiguration {
	return f2e.PrefixConfiguration{
		Bucket:               "my-bucket",
		Prefix:               "logs/",
		DataType:             f2e.DataTypeText,
		RecordsPerChunk:      1000,
		BatchSize:            10,
		MaxEventBytes:        64 * 1024,
		MaxFileBytes:         128 * 1024 * 1024,
		MaxChunkBytes:        32 * 1024 * 1024,
		JSONArraySearchBytes: 1 * 1024 * 1024,
		EventSchemaID:        "my-schema",
		EventSchemaVersion:   "1.0",
		EventFormat:          "json",
	}
}

// ── T20: PrefixID and OutputQueueURL validation ───────────────────────────────

func TestValidatePrefixConfiguration_ValidPrefixIDAndQueueURL(t *testing.T) {
	cfg := basePrefixConfig()
	cfg.PrefixID = "my-prefix-01"
	cfg.ChunkQueueURL = "https://sqs.us-east-1.amazonaws.com/123456789012/chunks"
	cfg.OutputQueueURL = "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"
	if err := ValidatePrefixConfiguration(cfg, baseGlobalLimits()); err != nil {
		t.Fatalf("unexpected error for valid prefixId+outputQueueURL: %v", err)
	}
}

func TestValidatePrefixConfiguration_EmptyPrefixIDAllowed(t *testing.T) {
	cfg := basePrefixConfig()
	cfg.PrefixID = ""
	if err := ValidatePrefixConfiguration(cfg, baseGlobalLimits()); err != nil {
		t.Fatalf("unexpected error when prefixId is empty: %v", err)
	}
}

func TestValidatePrefixConfiguration_InvalidPrefixIDSpace(t *testing.T) {
	cfg := basePrefixConfig()
	cfg.PrefixID = "my prefix"
	err := ValidatePrefixConfiguration(cfg, baseGlobalLimits())
	if err == nil || !strings.Contains(err.Error(), "prefixId contains invalid character") {
		t.Fatalf("expected invalid character error, got: %v", err)
	}
}

func TestValidatePrefixConfiguration_InvalidPrefixIDSlash(t *testing.T) {
	cfg := basePrefixConfig()
	cfg.PrefixID = "my/prefix"
	err := ValidatePrefixConfiguration(cfg, baseGlobalLimits())
	if err == nil || !strings.Contains(err.Error(), "prefixId contains invalid character") {
		t.Fatalf("expected invalid character error, got: %v", err)
	}
}

func TestValidatePrefixConfiguration_ValidPrefixIDWithDashAndUnderscore(t *testing.T) {
	cfg := basePrefixConfig()
	cfg.PrefixID = "prefix_01-v2"
	cfg.ChunkQueueURL = "https://sqs.us-east-1.amazonaws.com/123456789012/chunks"
	if err := ValidatePrefixConfiguration(cfg, baseGlobalLimits()); err != nil {
		t.Fatalf("unexpected error for dash/underscore prefixId: %v", err)
	}
}

func TestValidatePrefixConfiguration_EmptyOutputQueueURLAllowed(t *testing.T) {
	cfg := basePrefixConfig()
	cfg.OutputQueueURL = ""
	if err := ValidatePrefixConfiguration(cfg, baseGlobalLimits()); err != nil {
		t.Fatalf("unexpected error when outputQueueURL is empty: %v", err)
	}
}

func TestValidatePrefixConfiguration_InvalidOutputQueueURLARN(t *testing.T) {
	cfg := basePrefixConfig()
	cfg.OutputQueueURL = "arn:aws:sqs:us-east-1:123456789012:my-queue"
	err := ValidatePrefixConfiguration(cfg, baseGlobalLimits())
	if err == nil || !strings.Contains(err.Error(), "outputQueueURL must start with") {
		t.Fatalf("expected outputQueueURL error, got: %v", err)
	}
}

func TestValidatePrefixConfiguration_LocalstackOutputQueueURLAllowed(t *testing.T) {
	cfg := basePrefixConfig()
	cfg.OutputQueueURL = "http://localhost:4566/000000000000/my-queue"
	if err := ValidatePrefixConfiguration(cfg, baseGlobalLimits()); err != nil {
		t.Fatalf("unexpected error for localstack outputQueueURL: %v", err)
	}
}

func TestValidatePrefixConfiguration_AllowedSourceARNsAreInformational(t *testing.T) {
	// AllowedSourceARNs are informational only (Terraform generates policies).
	cfg := basePrefixConfig()
	cfg.AllowedSourceARNs = []string{
		"arn:aws:iam::123456789012:role/my-role",
		"arn:aws:sts::123456789012:assumed-role/my-role/session",
	}
	if err := ValidatePrefixConfiguration(cfg, baseGlobalLimits()); err != nil {
		t.Fatalf("unexpected error for allowedSourceARNs: %v", err)
	}
}

// ── T20: OutputQueueURL propagation via JobConfiguration ─────────────────────

func TestJobConfigurationCarriesOutputQueueURLAndPrefixID(t *testing.T) {
	j := f2e.JobConfiguration{
		OutputQueueURL: "https://sqs.us-east-1.amazonaws.com/123/q",
		PrefixID:       "my-prefix",
	}
	if j.OutputQueueURL != "https://sqs.us-east-1.amazonaws.com/123/q" {
		t.Fatalf("OutputQueueURL not set: %v", j.OutputQueueURL)
	}
	if j.PrefixID != "my-prefix" {
		t.Fatalf("PrefixID not set: %v", j.PrefixID)
	}
}

// ── ValidateGlobalLimits ─────────────────────────────────────────────────────

func TestValidateGlobalLimits_ValidLimits(t *testing.T) {
	if err := ValidateGlobalLimits(baseGlobalLimits()); err != nil {
		t.Fatalf("unexpected error for valid global limits: %v", err)
	}
}

func TestValidateGlobalLimits_ZeroMaxFileBytes(t *testing.T) {
	limits := baseGlobalLimits()
	limits.MaxFileBytes = 0
	if err := ValidateGlobalLimits(limits); err == nil {
		t.Fatal("expected error for zero MaxFileBytes")
	}
}

func TestValidateGlobalLimits_MissingInputType(t *testing.T) {
	limits := baseGlobalLimits()
	delete(limits.InputTypes, f2e.DataTypeText)
	if err := ValidateGlobalLimits(limits); err == nil {
		t.Fatal("expected error for missing input type")
	}
}
