// Organizer Lambda bootstrap for the F2E framework.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/f2e/f2e/internal/adapter/inbound/s3event"
	"github.com/f2e/f2e/internal/application/organizer"
	"github.com/f2e/f2e/internal/domain/f2e"
	awsclient "github.com/f2e/f2e/internal/platform/aws"
	"github.com/f2e/f2e/internal/platform/config"
)

var service organizer.Service

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
	service = organizer.Service{Store: a, Queue: a, Config: c}
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

// jobsFor accepts the explicit OrganizerRequest. S3 event notifications remain
// supported as the legacy fixed-width input format.
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
	return service.JobsWithExecutionID(ctx, references, executionID)
}

// logSummary logs the organizer summary to stdout
func logSummary(summary *f2e.OrganizerSummary) {
	slog.Info("organizer execution completed", "service", "organizer", "filesProcessed", summary.FilesProcessed, "chunksGenerated", summary.TotalChunksGenerated, "durationMs", summary.ProcessingTimeMillis)
}
func main() { lambda.Start(handler) }
