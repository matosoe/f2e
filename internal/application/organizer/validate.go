package organizer

import (
	"fmt"
	"strings"

	"github.com/matosoe/f2e/internal/application/port"
	"github.com/matosoe/f2e/internal/domain/f2e"
)

// ValidateGlobalLimits checks that the global limits are within acceptable bounds.
func ValidateGlobalLimits(limits f2e.GlobalLimits) error {
	if limits.MaxFileBytes < 1024 || limits.MaxChunkBytes < 1024 || limits.MaxFileBytes < limits.MaxChunkBytes ||
		limits.MaxEventBytes < 1024 || limits.MaxEventBytes > port.MaxSQSMessageBytes || limits.MaxBatchSize < 1 || limits.MaxBatchSize > 10 {
		return fmt.Errorf("invalid numeric global limits")
	}
	for _, dataType := range []f2e.DataType{f2e.DataTypeText, f2e.DataTypeJSON, f2e.DataTypeMultiLine} {
		limit, ok := limits.InputTypes[dataType]
		if !ok || limit.MaxFileBytes < 1 || limit.MaxFileBytes > limits.MaxFileBytes || limit.MaxRecordBytes < 1 || limit.MaxRecordBytes > int64(limits.MaxEventBytes) {
			return fmt.Errorf("invalid global limits for dataType %q", dataType)
		}
	}
	return nil
}

// ValidatePrefixConfiguration checks that the prefix configuration is within
// acceptable bounds and consistent with the global limits. It also validates
// routing fields (PrefixID, OutputQueueURL).
func ValidatePrefixConfiguration(c f2e.PrefixConfiguration, limits f2e.GlobalLimits) error {
	if c.DataType != f2e.DataTypeText && c.DataType != f2e.DataTypeJSON && c.DataType != f2e.DataTypeMultiLine {
		return fmt.Errorf("unsupported data type %q", c.DataType)
	}
	typeLimits, knownType := limits.InputTypes[c.DataType]
	if c.RecordsPerChunk < 1 || c.BatchSize < 1 || c.BatchSize > 10 ||
		c.MaxEventBytes < 1024 || c.MaxEventBytes > port.MaxSQSMessageBytes || c.MaxChunkBytes < 1024 ||
		c.MaxFileBytes < c.MaxChunkBytes ||
		c.EventSchemaID == "" || c.EventSchemaVersion == "" || c.EventFormat == "" {
		return fmt.Errorf("invalid SSM configuration for s3://%s/%s", c.Bucket, c.Prefix)
	}
	if !knownType || c.MaxFileBytes > limits.MaxFileBytes || c.MaxFileBytes > typeLimits.MaxFileBytes ||
		c.MaxChunkBytes > limits.MaxChunkBytes || c.MaxEventBytes > limits.MaxEventBytes || c.BatchSize > limits.MaxBatchSize ||
		c.MaxRecordLengthBytes > typeLimits.MaxRecordBytes || c.MultiLineLayout.MaxBytesPerRecord > typeLimits.MaxRecordBytes ||
		c.JSONArrayLayout.MaxBytesPerElement > typeLimits.MaxRecordBytes {
		return fmt.Errorf("SSM configuration for s3://%s/%s exceeds global limits", c.Bucket, c.Prefix)
	}
	if c.DataType == f2e.DataTypeJSON && (c.JSONArrayLayout.FirstFieldName == "" || c.JSONArrayLayout.MaxBytesPerElement < 1) {
		return fmt.Errorf("invalid JSON layout for s3://%s/%s", c.Bucket, c.Prefix)
	}
	if c.DataType == f2e.DataTypeMultiLine {
		if err := validateMultiLineLayout(c.MultiLineLayout); err != nil {
			return fmt.Errorf("invalid multi-line layout for s3://%s/%s: %w", c.Bucket, c.Prefix, err)
		}
	}
	// ── Bundle output mode validation (T15) ───────────────────────────────────
	switch c.OutputMode {
	case "", f2e.OutputModeSingle:
		// default; no extra fields required
	case f2e.OutputModeBundle:
		if c.MaxEnvelopesPerMessage < 0 {
			return fmt.Errorf("invalid SSM configuration for s3://%s/%s: maxEnvelopesPerMessage must be ≥ 0 (0 = optimize by byte limit)", c.Bucket, c.Prefix)
		}
		if c.MaxMessageBytes != 0 && (c.MaxMessageBytes < 1024 || c.MaxMessageBytes > c.MaxEventBytes) {
			return fmt.Errorf("invalid SSM configuration for s3://%s/%s: maxMessageBytes must be in [1024, maxEventBytes] when set", c.Bucket, c.Prefix)
		}
	default:
		return fmt.Errorf("invalid SSM configuration for s3://%s/%s: unknown outputMode %q (valid: single, bundle)", c.Bucket, c.Prefix, c.OutputMode)
	}
	// Authorisation and routing validation.
	if c.PrefixID != "" {
		for _, ch := range c.PrefixID {
			if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_') {
				return fmt.Errorf("invalid SSM configuration for s3://%s/%s: prefixId contains invalid character %q (allowed: a-z A-Z 0-9 - _)", c.Bucket, c.Prefix, ch)
			}
		}
	}
	if c.OutputQueueURL != "" {
		if !strings.HasPrefix(c.OutputQueueURL, "https://sqs.") && !strings.HasPrefix(c.OutputQueueURL, "http://") {
			return fmt.Errorf("invalid SSM configuration for s3://%s/%s: outputQueueURL must start with https://sqs. or http:// (localstack)", c.Bucket, c.Prefix)
		}
	}
	if c.ChunkQueueURL != "" && !strings.HasPrefix(c.ChunkQueueURL, "https://sqs.") && !strings.HasPrefix(c.ChunkQueueURL, "http://") {
		return fmt.Errorf("invalid SSM configuration for s3://%s/%s: chunkQueueURL must start with https://sqs. or http:// (localstack)", c.Bucket, c.Prefix)
	}
	if c.PrefixID != "" && c.ChunkQueueURL == "" {
		return fmt.Errorf("invalid SSM configuration for s3://%s/%s: registered prefix requires chunkQueueURL", c.Bucket, c.Prefix)
	}
	if c.TargetChunkBytes != 0 && (c.TargetChunkBytes < 5*1024*1024 || c.TargetChunkBytes > 100*1024*1024 || c.TargetChunkBytes > c.MaxChunkBytes) {
		return fmt.Errorf("invalid SSM configuration for s3://%s/%s: targetChunkBytes must be between 5 MiB and min(100 MiB, maxChunkBytes)", c.Bucket, c.Prefix)
	}
	return nil
}

func validateMultiLineLayout(layout f2e.MultiLineLayout) error {
	for name, fields := range map[string][]f2e.LineMatchField{"breakFields": layout.BreakFields, "includeFields": layout.IncludeFields, "ignoreFields": layout.IgnoreFields} {
		if name == "breakFields" && len(fields) == 0 {
			return fmt.Errorf("breakFields is required")
		}
		if len(fields) > 99 {
			return fmt.Errorf("%s has more than 99 fields", name)
		}
		for _, field := range fields {
			if field.StartByte < 0 || field.LengthBytes < 1 || len(field.Value) != field.LengthBytes {
				return fmt.Errorf("invalid %s field", name)
			}
		}
	}
	if layout.MaxBytesPerRecord < 1 {
		return fmt.Errorf("maxBytesPerRecord is required")
	}
	return nil
}
