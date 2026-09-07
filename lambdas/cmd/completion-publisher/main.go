// Completion-publisher Lambda for the F2E framework.
//
// This Lambda is triggered by DynamoDB Streams on the job-ledger table. It
// inspects each stream record, extracts COMPLETION_INTENT items that are still
// pending (intentPending="1"), and publishes the serialized CompletionEvent to
// the dedicated completion SQS queue. After SQS confirms the send it marks the
// intent as delivered so the item drops out of the pending-intents-index GSI.
//
// To avoid an infinite loop the handler skips REMOVE/MODIFY events on items
// whose sort-key prefix is COMPLETION_INTENT# — those are the deliveredAt
// updates written by this function itself.
//
// A scheduled companion recovery path (see RecoverPending below) queries the
// GSI for intents that were never delivered — covering Streams expiration and
// publisher crashes.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/platform/config"

	awsclient "github.com/f2e/f2e/internal/platform/aws"
)

// completionBackend is the interface used by the publisher logic so unit tests
// can inject a stub without starting a real AWS client.
type completionBackend interface {
	SendCompletion(ctx context.Context, queueURL string, body string) error
	MarkIntentDelivered(ctx context.Context, jobID string, version int64) error
	PendingCompletionIntents(ctx context.Context, limit int) ([]f2e.CompletionIntent, error)
}

var publisher struct {
	backend completionBackend
	cfg     config.Config
}

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx := context.Background()
	c, err := config.Load()
	if err != nil {
		slog.Error("invalid configuration", "service", "completion-publisher", "error", err)
		os.Exit(1)
	}
	// CompletionQueueURL may be empty during unit tests; the real deployment
	// will always set F2E_COMPLETION_QUEUE_URL. We validate lazily in handler
	// so tests can run without full AWS initialization.
	c.CompletionQueueURL = os.Getenv("F2E_COMPLETION_QUEUE_URL")
	if c.CompletionQueueURL == "" {
		// No AWS client needed without a queue; leave publisher.ledger nil so
		// tests can inject their own dependencies.
		publisher.cfg = c
		return
	}
	a, err := awsclient.New(ctx, c)
	if err != nil {
		slog.Error("aws initialization failed", "service", "completion-publisher", "error", err)
		os.Exit(1)
	}
	publisher.backend = a
	publisher.cfg = c
}

// handler is the DynamoDB Streams event source handler. It processes only
// INSERT events on COMPLETION_INTENT# items that carry intentPending="1".
// MODIFY and REMOVE events on those items are updates written by this function
// itself (deliveredAt); processing them would cause an infinite loop.
func handler(ctx context.Context, event events.DynamoDBEvent) error {
	for _, r := range event.Records {
		if r.EventName != "INSERT" {
			continue
		}
		sk, ok := r.Change.NewImage["sk"]
		if !ok || !strings.HasPrefix(sk.String(), "COMPLETION_INTENT#") {
			continue
		}
		pending, ok := r.Change.NewImage["intentPending"]
		if !ok || pending.String() != "1" {
			continue
		}
		payloadAttr, ok := r.Change.NewImage["payload"]
		if !ok {
			slog.Warn("completion intent missing payload", "sk", sk.String())
			continue
		}
		var intent f2e.CompletionIntent
		if err := json.Unmarshal([]byte(payloadAttr.String()), &intent); err != nil {
			slog.Error("decode completion intent", "sk", sk.String(), "error", err)
			// Return error so the batch fails and the stream retries.
			return fmt.Errorf("decode completion intent %s: %w", sk.String(), err)
		}
		if err := publishIntent(ctx, intent); err != nil {
			return err
		}
	}
	return nil
}

// eventHandler accepts both DynamoDB Streams records and the scheduled
// EventBridge recovery event. Keeping recovery in this Lambda gives it the
// same idempotency and least-privilege boundaries as stream delivery.
func eventHandler(ctx context.Context, raw json.RawMessage) error {
	var scheduled struct {
		Recover bool `json:"recover"`
	}
	if err := json.Unmarshal(raw, &scheduled); err != nil {
		return fmt.Errorf("decode publisher event: %w", err)
	}
	if scheduled.Recover {
		return RecoverPending(ctx, 100)
	}
	var streamEvent events.DynamoDBEvent
	if err := json.Unmarshal(raw, &streamEvent); err != nil {
		return fmt.Errorf("decode DynamoDB stream event: %w", err)
	}
	return handler(ctx, streamEvent)
}

// publishIntent sends the completion event to the SQS queue and marks the
// intent as delivered. A crash after send but before mark results in a
// duplicate SQS delivery; consumers deduplicate via the stable eventId.
func publishIntent(ctx context.Context, intent f2e.CompletionIntent) error {
	body, err := json.Marshal(intent.Event)
	if err != nil {
		return fmt.Errorf("marshal completion event for job %s: %w", intent.JobID, err)
	}
	if err := publisher.backend.SendCompletion(ctx, publisher.cfg.CompletionQueueURL, string(body)); err != nil {
		return fmt.Errorf("send completion event for job %s: %w", intent.JobID, err)
	}
	if err := publisher.backend.MarkIntentDelivered(ctx, intent.JobID, intent.Version); err != nil {
		// Log but do not fail: the intent will be re-discovered by the recovery
		// path and the duplicate delivery is acceptable (same eventId).
		slog.Warn("mark intent delivered failed — duplicate delivery expected",
			"jobId", intent.JobID, "version", intent.Version, "error", err)
	}
	slog.Info("completion event published",
		"jobId", intent.JobID, "eventId", intent.Event.EventID, "status", intent.Status)
	return nil
}

// RecoverPending is exported so it can be invoked by a scheduled Lambda or
// EventBridge rule to recover intents that were never delivered due to Streams
// expiration or a publisher crash. The limit parameter caps how many intents
// are processed per invocation to bound runtime.
func RecoverPending(ctx context.Context, limit int) error {
	intents, err := publisher.backend.PendingCompletionIntents(ctx, limit)
	if err != nil {
		return fmt.Errorf("query pending completion intents: %w", err)
	}
	for _, intent := range intents {
		if err := publishIntent(ctx, intent); err != nil {
			slog.Error("recover pending intent failed", "jobId", intent.JobID, "error", err)
			// Continue with remaining intents; individual failures are logged.
		}
	}
	return nil
}

func main() { lambda.Start(eventHandler) }
