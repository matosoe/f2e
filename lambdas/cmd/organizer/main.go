// Organizer Lambda bootstrap for the F2E framework.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"time"

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
	if e = organizer.ValidateGlobalLimits(globalLimits); e != nil {
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

// handler accepts both the SQS intake event and the small EventBridge recovery
// event. Keeping both paths in the Organizer makes the PoC operationally
// small: there is one admission implementation and no extra queue consumer.
func handler(ctx context.Context, raw json.RawMessage) (any, error) {
	var scheduled struct {
		ReleaseWaiting bool `json:"releaseWaiting"`
	}
	if err := json.Unmarshal(raw, &scheduled); err == nil && scheduled.ReleaseWaiting {
		return nil, releaseWaiting(ctx)
	}
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
			// A quota miss is normally converted into a durable WAITING admission
			// by jobsFor. Keep this guard for non-S3 callers that still surface it.
			if !errors.Is(err, port.ErrQuotaExceeded) {
				out.BatchItemFailures = append(out.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: r.MessageId})
			}
		}
	}
	return out, nil
}

func releaseWaiting(ctx context.Context) error {
	if service.Ledger == nil {
		return nil
	}
	waiting, err := service.Ledger.ClaimWaitingAdmissions(ctx, 25)
	if err != nil {
		return err
	}
	for _, item := range waiting {
		jobs, admissionErr := jobsFor(ctx, []byte(item.Body), "waiting-"+item.FileID)
		if admissionErr == nil {
			admissionErr = service.Publish(ctx, jobs)
		}
		if admissionErr == nil {
			if err := service.Ledger.CompleteWaitingAdmission(ctx, item.FileID); err != nil {
				return fmt.Errorf("complete waiting admission %s: %w", item.FileID, err)
			}
			continue
		}
		if err := service.Ledger.ReturnWaitingAdmission(ctx, item.FileID); err != nil {
			return fmt.Errorf("return waiting admission %s: %w", item.FileID, err)
		}
		if !errors.Is(admissionErr, port.ErrQuotaExceeded) {
			slog.Error("waiting admission deferred", "service", "organizer", "fileId", item.FileID, "prefixId", item.PrefixID, "error", admissionErr)
		}
	}
	return nil
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
		if reference.VersionID == "" {
			return nil, fmt.Errorf("S3 notification for s3://%s/%s has no VersionId; immutable admission is required", reference.Bucket, reference.Key)
		}
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
		configured.Config.JSONArraySearchBytes = prefixConfig.JSONArraySearchBytes
		configured.Config.EventSchemaID = prefixConfig.EventSchemaID
		configured.Config.EventSchemaVersion = prefixConfig.EventSchemaVersion
		configured.Config.EventFormat = prefixConfig.EventFormat
		versionedStore, ok := configured.Store.(port.VersionedObjectStore)
		if !ok {
			return nil, fmt.Errorf("configured object store does not support immutable version reads")
		}
		object, err := versionedStore.HeadObject(ctx, f2e.ObjectIdentity{Bucket: reference.Bucket, Key: reference.Key, VersionID: reference.VersionID})
		if err != nil {
			return nil, fmt.Errorf("head immutable S3 object %s/%s@%s: %w", reference.Bucket, reference.Key, reference.VersionID, err)
		}
		fileID := f2e.FileID(object)
		if configured.Ledger != nil {
			// A quota miss becomes a durable FIFO admission record. This acks the
			// intake message, avoiding retry/DLQ churn while preserving the oldest
			// queued file for each prefix.
			if prefixConfig.PrefixID != "" && prefixConfig.MaxActiveJobs > 0 {
				if slotErr := configured.Ledger.ReserveSlot(ctx, prefixConfig.PrefixID, prefixConfig.MaxActiveJobs); slotErr != nil {
					if errors.Is(slotErr, port.ErrQuotaExceeded) {
						waitingBody, bodyErr := singleS3Notification(reference)
						if bodyErr != nil {
							return nil, bodyErr
						}
						if waitErr := configured.Ledger.EnqueueWaitingAdmission(ctx, port.WaitingAdmission{FileID: fileID, PrefixID: prefixConfig.PrefixID, Body: string(waitingBody)}); waitErr != nil {
							return nil, fmt.Errorf("queue saturated admission: %w", waitErr)
						}
						slog.Info("admission queued for quota", "service", "organizer", "fileId", fileID, "prefixId", prefixConfig.PrefixID)
						return nil, port.ErrQuotaExceeded
					}
					return nil, slotErr
				}
			}
			outcome, admitErr := configured.Ledger.Admit(ctx, f2e.Receipt{ReceiptID: executionID, FileID: fileID, Source: object, Environment: configured.Config.Environment, ReceivedAt: time.Now().UTC(), ConfigSnapshot: configSnapshot, PrefixID: prefixConfig.PrefixID})
			if admitErr != nil {
				// Roll back the quota slot we just reserved.
				if prefixConfig.PrefixID != "" && prefixConfig.MaxActiveJobs > 0 {
					_ = configured.Ledger.ReleaseSlot(ctx, prefixConfig.PrefixID)
				}
				return nil, fmt.Errorf("admit immutable S3 object: %w", admitErr)
			}
			if outcome == f2e.AlreadyCompleted {
				// Deduplication: this slot was reserved optimistically but no new job
				// is created — release it so the counter stays accurate.
				if prefixConfig.PrefixID != "" && prefixConfig.MaxActiveJobs > 0 {
					_ = configured.Ledger.ReleaseSlot(ctx, prefixConfig.PrefixID)
				}
				continue
			}
			if outcome == f2e.Busy {
				if prefixConfig.PrefixID != "" && prefixConfig.MaxActiveJobs > 0 {
					_ = configured.Ledger.ReleaseSlot(ctx, prefixConfig.PrefixID)
				}
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
			VersionID:            reference.VersionID,
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
			if err := configured.Ledger.Plan(ctx, f2e.JobPlan{JobID: fileID, FileID: fileID, Bucket: object.Bucket, Key: object.Key, VersionID: object.VersionID, ETag: object.ETag, ExpectedChunks: 0, CreatedAt: time.Now().UTC(), ConfigSnapshot: configSnapshot}, nil); err != nil {
				return nil, fmt.Errorf("seal empty manifest: %w", err)
			}
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
