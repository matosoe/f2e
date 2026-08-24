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
		return service.Plan(ctx, request)
	}
	references, err := s3event.Parse(body)
	if err != nil {
		return nil, err
	}
	return service.Jobs(ctx, references)
}
func main() { lambda.Start(handler) }
