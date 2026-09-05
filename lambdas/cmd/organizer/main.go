// Organizer Lambda bootstrap for the F2E framework.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/f2e/f2e/internal/adapter/inbound/s3event"
	"github.com/f2e/f2e/internal/application/organizer"
	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/domain/f2e"
	awsclient "github.com/f2e/f2e/internal/platform/aws"
	"github.com/f2e/f2e/internal/platform/config"
)

var service organizer.Service
var configurationResolver port.PrefixConfigurationResolver
var globalLimits f2e.GlobalLimits
var globalLimitsVersion int64
var globalLimitsParameter string

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx := context.Background()
	c, e := config.Load()
	if e != nil {
		slog.Error("invalid configuration", "service", "organizer", "error", e)
		os.Exit(1)
	}
	a, e := awsclient.New(ctx, c)
	if e != nil {
		slog.Error("aws initialization failed", "service", "organizer", "error", e)
		os.Exit(1)
	}
	globalLimits, globalLimitsVersion, globalLimitsParameter, e = a.LoadGlobalLimits(ctx)
	if e != nil {
		slog.Error("global limits initialization failed", "service", "organizer", "error", e)
		os.Exit(1)
	}
	if e = validateGlobalLimits(globalLimits); e != nil {
		slog.Error("invalid global limits", "service", "organizer", "error", e)
		os.Exit(1)
	}
	c.MaxFileBytes = globalLimits.MaxFileBytes
	c.MaxChunkBytes = globalLimits.MaxChunkBytes
	c.MaxEventBytes = globalLimits.MaxEventBytes
	c.JSONArraySearchBytes = globalLimits.MaxJSONArraySearchBytes
	service = organizer.Service{Store: a, Queue: a, Config: c}
	configurationResolver = a
	if c.LedgerTable != "" {
		service.Ledger = a
	}
}
func handler(ctx context.Context, e events.SQSEvent) (events.SQSEventResponse, error) {
	out := events.SQSEventResponse{}
	for _, r := range e.Records {
		jobs, err := jobsFor(ctx, []byte(r.Body), r.MessageId)
		if err == nil {
			err = service.Publish(ctx, jobs)
		}
		if err != nil {
			slog.Error("organizer message failed", "service", "organizer", "sqsMessageId", r.MessageId, "receiveCount", r.Attributes["ApproximateReceiveCount"], "error", err)
			out.BatchItemFailures = append(out.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: r.MessageId})
		}
	}
	return out, nil
}

// jobsFor accepts either an explicit OrganizerRequest or an S3 event whose
// bucket/key prefix selects a configuration from SSM Parameter Store.
func jobsFor(ctx context.Context, body []byte, executionID string) ([]f2e.ChunkJob, error) {
	var request f2e.OrganizerRequest
	if err := json.Unmarshal(body, &request); err == nil && request.SchemaVersion != "" {
		request.ExecutionID = executionID
		execution, err := service.PlanWithSummary(ctx, request)
		if err != nil {
			return nil, err
		}
		// Log summary
		if execution.Summary != nil {
			logSummary(execution.Summary)
		}
		return execution.Jobs, nil
	}
	references, err := s3event.Parse(body)
	if err != nil {
		return nil, err
	}
	// S3 emits a TestEvent while a bucket notification is configured. It has no
	// object Records and is acknowledged without creating a job.
	if len(references) == 0 {
		return nil, nil
	}
	if configurationResolver == nil {
		return nil, fmt.Errorf("S3 event received but no prefix configuration resolver available")
	}
	var jobs []f2e.ChunkJob
	for _, reference := range references {
		prefixConfig, configSnapshot, err := configurationResolver.ResolvePrefixConfiguration(ctx, reference.Bucket, reference.Key)
		if err != nil {
			return nil, err
		}
		// Enrich snapshot with global limits provenance captured at cold start.
		configSnapshot.GlobalLimitsParameter = globalLimitsParameter
		configSnapshot.GlobalLimitsVersion = globalLimitsVersion
		if err := validatePrefixConfiguration(prefixConfig, globalLimits); err != nil {
			return nil, err
		}
		configured := service
		configured.Config.RecordsPerChunk = prefixConfig.RecordsPerChunk
		configured.Config.BatchSize = prefixConfig.BatchSize
		configured.Config.MaxEventBytes = prefixConfig.MaxEventBytes
		configured.Config.MaxFileBytes = prefixConfig.MaxFileBytes
		configured.Config.MaxChunkBytes = prefixConfig.MaxChunkBytes
		configured.Config.JSONArraySearchBytes = prefixConfig.JSONArraySearchBytes
		configured.Config.EventSchemaID = prefixConfig.EventSchemaID
		configured.Config.EventSchemaVersion = prefixConfig.EventSchemaVersion
		configured.Config.EventFormat = prefixConfig.EventFormat
		request := f2e.OrganizerRequest{SchemaVersion: f2e.SchemaVersion, ExecutionID: executionID, Files: []f2e.FileRequest{{
			Bucket: reference.Bucket, Key: reference.Key, DataType: prefixConfig.DataType,
			MaxRecordLengthBytes: prefixConfig.MaxRecordLengthBytes, MultiLineLayout: prefixConfig.MultiLineLayout,
			JSONArrayLayout: prefixConfig.JSONArrayLayout,
		}}}
		execution, err := configured.PlanWithSummary(ctx, request)
		if err != nil {
			return nil, err
		}
		for i := range execution.Jobs {
			execution.Jobs[i].Configuration = f2e.JobConfiguration{
				BatchSize: prefixConfig.BatchSize, MaxEventBytes: prefixConfig.MaxEventBytes,
				MaxChunkBytes: prefixConfig.MaxChunkBytes, EventSchemaID: prefixConfig.EventSchemaID,
				EventSchemaVersion: prefixConfig.EventSchemaVersion, EventFormat: prefixConfig.EventFormat,
			}
			execution.Jobs[i].ConfigSnapshot = configSnapshot
		}
		logSummary(execution.Summary)
		jobs = append(jobs, execution.Jobs...)
	}
	return jobs, nil
}

func validateGlobalLimits(limits f2e.GlobalLimits) error {
	if limits.MaxFileBytes < 1024 || limits.MaxChunkBytes < 1024 || limits.MaxFileBytes < limits.MaxChunkBytes ||
		limits.MaxEventBytes < 1024 || limits.MaxEventBytes > 256*1024 || limits.MaxBatchSize < 1 || limits.MaxBatchSize > 10 ||
		limits.MaxJSONArraySearchBytes < 1024 || limits.MaxJSONArraySearchBytes > 16*1024*1024 {
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

func validatePrefixConfiguration(c f2e.PrefixConfiguration, limits f2e.GlobalLimits) error {
	if c.DataType != f2e.DataTypeText && c.DataType != f2e.DataTypeJSON && c.DataType != f2e.DataTypeMultiLine {
		return fmt.Errorf("unsupported data type %q", c.DataType)
	}
	typeLimits, knownType := limits.InputTypes[c.DataType]
	if c.RecordsPerChunk < 1 || c.BatchSize < 1 || c.BatchSize > 10 ||
		c.MaxEventBytes < 1024 || c.MaxEventBytes > 256*1024 || c.MaxChunkBytes < 1024 ||
		c.MaxFileBytes < c.MaxChunkBytes || c.JSONArraySearchBytes < 1024 || c.JSONArraySearchBytes > 16*1024*1024 ||
		c.EventSchemaID == "" || c.EventSchemaVersion == "" || c.EventFormat == "" {
		return fmt.Errorf("invalid SSM configuration for s3://%s/%s", c.Bucket, c.Prefix)
	}
	if !knownType || c.MaxFileBytes > limits.MaxFileBytes || c.MaxFileBytes > typeLimits.MaxFileBytes ||
		c.MaxChunkBytes > limits.MaxChunkBytes || c.MaxEventBytes > limits.MaxEventBytes || c.BatchSize > limits.MaxBatchSize ||
		c.JSONArraySearchBytes > limits.MaxJSONArraySearchBytes ||
		c.MaxRecordLengthBytes > typeLimits.MaxRecordBytes || c.MultiLineLayout.MaxBytesPerRecord > typeLimits.MaxRecordBytes ||
		c.JSONArrayLayout.MaxBytesPerElement > typeLimits.MaxRecordBytes {
		return fmt.Errorf("SSM configuration for s3://%s/%s exceeds global limits", c.Bucket, c.Prefix)
	}
	return nil
}

// logSummary logs the organizer summary to stdout
func logSummary(summary *f2e.OrganizerSummary) {
	slog.Info("organizer execution completed", "service", "organizer", "filesProcessed", summary.FilesProcessed, "chunksGenerated", summary.TotalChunksGenerated, "durationMs", summary.ProcessingTimeMillis)
}
func main() { lambda.Start(handler) }
