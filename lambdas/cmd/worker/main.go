// Worker Lambda bootstrap for the F2E framework.
package main

import (
	"context"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/f2e/f2e/internal/application/worker"
	awsclient "github.com/f2e/f2e/internal/platform/aws"
	"github.com/f2e/f2e/internal/platform/config"
)

var service worker.Service

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
	service = worker.Service{Resolver: a, Queue: a, Config: c}
}
func handler(ctx context.Context, e events.SQSEvent) (events.SQSEventResponse, error) {
	out := events.SQSEventResponse{}
	for _, r := range e.Records {
		if err := service.Process(ctx, []byte(r.Body)); err != nil {
			log.Printf("worker message=%s error=%v", r.MessageId, err)
			out.BatchItemFailures = append(out.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: r.MessageId})
		}
	}
	return out, nil
}
func main() { lambda.Start(handler) }
