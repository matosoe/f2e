package main

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/f2e/f2e/internal/adapter/inbound/s3event"
	"github.com/f2e/f2e/internal/application/organizer"
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
		references, err := s3event.Parse([]byte(r.Body))
		if err == nil {
			jobs, err := service.Jobs(ctx, references)
			if err == nil {
				err = service.Publish(ctx, jobs)
			}
		}
		if err != nil {
			log.Printf("organizer message=%s error=%v", r.MessageId, err)
			out.BatchItemFailures = append(out.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: r.MessageId})
		}
	}
	return out, nil
}
func main() { lambda.Start(handler) }
