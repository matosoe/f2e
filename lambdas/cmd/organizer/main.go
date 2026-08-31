// Organizer Lambda bootstrap for the F2E framework.
package main

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/f2e/f2e/internal/adapter/inbound/s3event"
	"github.com/f2e/f2e/internal/application/organizer"
	"github.com/f2e/f2e/internal/domain/f2e"
	awsclient "github.com/f2e/f2e/internal/platform/aws"
	"github.com/f2e/f2e/internal/platform/config"
	"log"
)

var service organizer.Service

func init() {
	ctx := context.Background()
	c, e := config.Load()
	if e != nil {
		log.Fatal(e)
	}
	a, e := awsclient.New(ctx, c)
	if e != nil {
		log.Fatal(e)
	}
	service = organizer.Service{Store: a, Queue: a, Config: c}
}
func handler(ctx context.Context, e events.SQSEvent) (events.SQSEventResponse, error) {
	out := events.SQSEventResponse{}
	for _, r := range e.Records {
		jobs, err := jobsFor(ctx, []byte(r.Body))
		if err == nil {
			err = service.Publish(ctx, jobs)
		}
		if err != nil {
			log.Printf("organizer message=%s error=%v", r.MessageId, err)
			out.BatchItemFailures = append(out.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: r.MessageId})
		}
	}
	return out, nil
}

// jobsFor accepts the explicit OrganizerRequest. S3 event notifications remain
// supported as the legacy fixed-width input format.
func jobsFor(ctx context.Context, body []byte) ([]f2e.ChunkJob, error) {
	var request f2e.OrganizerRequest
	if err := json.Unmarshal(body, &request); err == nil && request.SchemaVersion != "" {
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
	return service.Jobs(ctx, references)
}

// logSummary logs the organizer summary to stdout
func logSummary(summary *f2e.OrganizerSummary) {
	log.Println("=== ORGANIZER SUMMARY ===")
	log.Printf("Files Processed: %d", summary.FilesProcessed)
	log.Printf("Total Chunks Generated: %d", summary.TotalChunksGenerated)
	log.Printf("Processing Time: %s", f2e.FormatDuration(summary.ProcessingTimeMillis))
	
	for _, fileSummary := range summary.Files {
		log.Printf("  File: %s/%s", fileSummary.Bucket, fileSummary.Key)
		log.Printf("    Size: %d bytes", fileSummary.SizeBytes)
		log.Printf("    Chunks: %d", fileSummary.ChunksGenerated)
		log.Printf("    Time: %s", f2e.FormatDuration(fileSummary.ProcessingTimeMillis))
	}
	log.Println("========================")
}
func main() { lambda.Start(handler) }
