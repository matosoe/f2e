// Organizer Lambda bootstrap for the F2E framework.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/matosoe/f2e/internal/adapter/inbound/s3event"
	"github.com/matosoe/f2e/internal/application/organizer"
	"github.com/matosoe/f2e/internal/application/port"
	"github.com/matosoe/f2e/internal/domain/f2e"
	awsclient "github.com/matosoe/f2e/internal/platform/aws"
	"github.com/matosoe/f2e/internal/platform/config"
	"github.com/matosoe/f2e/internal/platform/runtimeclock"
)

var service organizer.Service
var configurationResolver port.PrefixConfigurationResolver
var globalLimits f2e.GlobalLimits
var globalLimitsVersion int64
var globalLimitsParameter string

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := runtimeclock.ConfigureLocalTimezone(); err != nil {
		slog.Error("invalid timezone configuration", "service", "organizer", "error", err)
		os.Exit(1)
	}
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
	if e = organizer.ValidateGlobalLimits(globalLimits); e != nil {
		slog.Error("invalid global limits", "service", "organizer", "error", e)
		os.Exit(1)
	}
	c.MaxFileBytes = globalLimits.MaxFileBytes
	c.MaxChunkBytes = globalLimits.MaxChunkBytes
	c.MaxEventBytes = globalLimits.MaxEventBytes
	service = organizer.Service{Store: a, Queue: a, Config: c}
	configurationResolver = a
	if c.LedgerTable != "" {
		service.Ledger = a
	}
}

func handler(ctx context.Context, raw json.RawMessage) (any, error) {
	var e events.SQSEvent
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, fmt.Errorf("decode organizer event: %w", err)
	}
	return handleSQSEvent(ctx, e)
}

func handleSQSEvent(ctx context.Context, e events.SQSEvent) (events.SQSEventResponse, error) {
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
		configSnapshot.GlobalLimits = globalLimits
		if err := organizer.ValidatePrefixConfiguration(prefixConfig, globalLimits); err != nil {
			return nil, err
		}
		configured := service
		configured.Config.RecordsPerChunk = prefixConfig.RecordsPerChunk
		configured.Config.BatchSize = prefixConfig.BatchSize
		configured.Config.MaxEventBytes = prefixConfig.MaxEventBytes
		configured.Config.MaxFileBytes = prefixConfig.MaxFileBytes
		configured.Config.MaxChunkBytes = prefixConfig.MaxChunkBytes
		configured.Config.TargetChunkBytes = prefixConfig.TargetChunkBytes
		configured.Config.EventSchemaID = prefixConfig.EventSchemaID
		configured.Config.EventSchemaVersion = prefixConfig.EventSchemaVersion
		configured.Config.EventFormat = prefixConfig.EventFormat
		object, err := headAdmissionObject(ctx, configured.Store, reference)
		if err != nil {
			return nil, err
		}
		fileID := f2e.FileID(object)
		if configured.Ledger != nil {
			outcome, admitErr := configured.Ledger.Admit(ctx, f2e.Receipt{ReceiptID: executionID, FileID: fileID, Source: object, Environment: configured.Config.Environment, ReceivedAt: time.Now().Local(), ConfigSnapshot: configSnapshot, PrefixID: prefixConfig.PrefixID})
			if admitErr != nil {
				return nil, fmt.Errorf("admit immutable S3 object: %w", admitErr)
			}
			if outcome == f2e.AlreadyCompleted {
				continue
			}
			if outcome == f2e.Busy {
				return nil, fmt.Errorf("admission for fileId %s is busy", fileID)
			}
			if err := configured.Ledger.BeginValidation(ctx, fileID); err != nil {
				return nil, fmt.Errorf("begin admission validation: %w", err)
			}
			if err := configured.Ledger.BeginPlanning(ctx, fileID); err != nil {
				return nil, fmt.Errorf("begin admission planning: %w", err)
			}
		}
		request := f2e.OrganizerRequest{SchemaVersion: f2e.SchemaVersion, ExecutionID: fileID, Files: []f2e.FileRequest{{
			Bucket: reference.Bucket, Key: reference.Key, DataType: prefixConfig.DataType,
			VersionID: object.VersionID, ETag: object.ETag,
			MaxRecordLengthBytes: prefixConfig.MaxRecordLengthBytes, MultiLineLayout: prefixConfig.MultiLineLayout,
			JSONArrayLayout: prefixConfig.JSONArrayLayout,
		}}}
		execution, err := configured.PlanWithSummary(ctx, request)
		if err != nil {
			return nil, err
		}
		// A selected empty JSON array has no Worker message. Seal and complete
		// its zero-chunk manifest directly so admission never remains PLANNING.
		if len(execution.Jobs) == 0 && configured.Ledger != nil {
			if err := configured.Ledger.Plan(ctx, f2e.JobPlan{JobID: fileID, FileID: fileID, Bucket: object.Bucket, Key: object.Key, VersionID: object.VersionID, ETag: object.ETag, FileSize: object.Size, ExpectedChunks: 0, CreatedAt: time.Now().Local(), ConfigSnapshot: configSnapshot}, nil); err != nil {
				return nil, fmt.Errorf("seal empty manifest: %w", err)
			}
			// The Worker owns completion publication even for an empty array. This
			// control message is durable in the normal chunk queue and can be
			// redriven without re-planning the file.
			execution.Jobs = append(execution.Jobs, f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, JobID: fileID, FileID: fileID, ChunkID: "completion", Bucket: object.Bucket, Key: object.Key, Configuration: f2e.JobConfiguration{ChunkQueueURL: prefixConfig.ChunkQueueURL}, Control: "completion"})
		}
		for i := range execution.Jobs {
			execution.Jobs[i].Configuration = f2e.JobConfiguration{
				BatchSize:              prefixConfig.BatchSize,
				MaxEventBytes:          prefixConfig.MaxEventBytes,
				MaxChunkBytes:          prefixConfig.MaxChunkBytes,
				TargetChunkBytes:       prefixConfig.TargetChunkBytes,
				EventSchemaID:          prefixConfig.EventSchemaID,
				EventSchemaVersion:     prefixConfig.EventSchemaVersion,
				EventFormat:            prefixConfig.EventFormat,
				OutputMode:             prefixConfig.OutputMode,
				MaxEnvelopesPerMessage: prefixConfig.MaxEnvelopesPerMessage,
				MaxMessageBytes:        prefixConfig.MaxMessageBytes,
				OutputQueueURL:         prefixConfig.OutputQueueURL,
				PrefixID:               prefixConfig.PrefixID,
				ChunkQueueURL:          prefixConfig.ChunkQueueURL,
			}
			execution.Jobs[i].ConfigSnapshot = configSnapshot
		}
		logSummary(execution.Summary)
		jobs = append(jobs, execution.Jobs...)
	}
	return jobs, nil
}

// headAdmissionObject resolves the object identity used for admission. A
// versioned notification pins an immutable S3 version; otherwise the current
// object is admitted with its ETag, which subsequent reads use conditionally.
func headAdmissionObject(ctx context.Context, store port.ObjectStore, reference f2e.FileReference) (f2e.ObjectIdentity, error) {
	object := f2e.ObjectIdentity{Bucket: reference.Bucket, Key: reference.Key, VersionID: reference.VersionID}
	// S3 uses the literal "null" version ID for the mutable current object of
	// a bucket with versioning suspended; it cannot be treated as immutable.
	if reference.VersionID == "" || reference.VersionID == "null" {
		resolved, err := store.Head(ctx, reference.Bucket, reference.Key)
		if err != nil {
			return f2e.ObjectIdentity{}, fmt.Errorf("head S3 object %s/%s: %w", reference.Bucket, reference.Key, err)
		}
		return resolved, nil
	}
	versionedStore, ok := store.(port.VersionedObjectStore)
	if !ok {
		return f2e.ObjectIdentity{}, fmt.Errorf("configured object store does not support reads of requested version %q", reference.VersionID)
	}
	resolved, err := versionedStore.HeadObject(ctx, object)
	if err != nil {
		return f2e.ObjectIdentity{}, fmt.Errorf("head immutable S3 object %s/%s@%s: %w", reference.Bucket, reference.Key, reference.VersionID, err)
	}
	return resolved, nil
}

func singleS3Notification(reference f2e.FileReference) ([]byte, error) {
	return json.Marshal(s3event.Notification{Records: []s3event.Record{func() s3event.Record {
		var record s3event.Record
		record.EventName = "ObjectCreated:Put"
		record.S3.Bucket.Name = reference.Bucket
		record.S3.Object.Key = url.QueryEscape(reference.Key)
		record.S3.Object.VersionID = reference.VersionID
		return record
	}()}})
}

// logSummary logs the organizer summary to stdout
func logSummary(summary *f2e.OrganizerSummary) {
	slog.Info("organizer execution completed", "service", "organizer", "filesProcessed", summary.FilesProcessed, "chunksGenerated", summary.TotalChunksGenerated, "durationMs", summary.ProcessingTimeMillis)
}
func main() { lambda.Start(handler) }
