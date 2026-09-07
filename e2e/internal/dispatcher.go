package internal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// MessageDispatcher polls the shared output SQS queue in a background goroutine
// and routes each logical Envelope (bundles are expanded) to a per-jobId channel
// registered by the active scenario. Unrecognised messages (no registered
// subscriber) are returned to the queue via a ChangeMessageVisibility call so
// they become visible to the next polling cycle without being lost.
//
// Usage:
//
//	d := NewMessageDispatcher(client)
//	ch := d.Subscribe(jobID)       // before sending the file
//	defer d.Unsubscribe(jobID)
//	// ... drive the pipeline ...
//	envelopes := collectFromChannel(ch, expected, timeout)
//
// The dispatcher is safe for concurrent use: multiple scenarios can subscribe
// and unsubscribe simultaneously.
type MessageDispatcher struct {
	client     *AWSClient
	mu         sync.RWMutex
	subs       map[string]chan Envelope // keyed by jobId
	orphanBuf  []orphanMsg             // messages whose jobId has not yet been subscribed
	stopCh     chan struct{}
	done       chan struct{}
}

type orphanMsg struct {
	body          string
	receiptHandle string
	arrivedAt     time.Time
}

// orphanTTL controls how long an unrouted message is buffered before it is
// returned to SQS via ChangeMessageVisibility(0).
const orphanTTL = 30 * time.Second

// NewMessageDispatcher creates a dispatcher and starts its background polling
// goroutine. Call Stop to shut it down.
func NewMessageDispatcher(client *AWSClient) *MessageDispatcher {
	d := &MessageDispatcher{
		client: client,
		subs:   make(map[string]chan Envelope),
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
	go d.poll()
	return d
}

// Subscribe registers a subscription for the given jobId and returns a buffered
// channel that will receive all Envelopes whose Processing.JobID equals jobId.
// The caller owns the channel until Unsubscribe is called.
func (d *MessageDispatcher) Subscribe(jobID string) <-chan Envelope {
	ch := make(chan Envelope, 1024)
	d.mu.Lock()
	d.subs[jobID] = ch
	d.mu.Unlock()
	// Flush any orphans that arrived before the subscription.
	d.flushOrphans(jobID, ch)
	return ch
}

// Unsubscribe removes the subscription for jobId. The returned channel is
// closed so consumers can range over it safely.
func (d *MessageDispatcher) Unsubscribe(jobID string) {
	d.mu.Lock()
	ch, ok := d.subs[jobID]
	if ok {
		delete(d.subs, jobID)
		close(ch)
	}
	d.mu.Unlock()
}

// Stop shuts down the background polling goroutine and waits for it to exit.
func (d *MessageDispatcher) Stop() {
	close(d.stopCh)
	<-d.done
}

// poll is the background goroutine.
func (d *MessageDispatcher) poll() {
	defer close(d.done)
	for {
		select {
		case <-d.stopCh:
			return
		default:
		}
		d.pollOnce()
		// Periodically expire old orphans that no scenario ever claimed.
		d.expireOrphans()
	}
}

func (d *MessageDispatcher) pollOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := d.client.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(d.client.outputQueueURL),
		MaxNumberOfMessages:   10,
		WaitTimeSeconds:       5,
		MessageAttributeNames: []string{"All"},
	})
	if err != nil {
		return
	}

	for _, msg := range out.Messages {
		d.routeMessage(*msg.Body, *msg.ReceiptHandle)
	}
}

func (d *MessageDispatcher) routeMessage(body, receiptHandle string) {
	envelopes, err := expandMessage(body)
	if err != nil || len(envelopes) == 0 {
		// Malformed message — delete it so it doesn't loop forever.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = d.client.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(d.client.outputQueueURL),
			ReceiptHandle: aws.String(receiptHandle),
		})
		return
	}

	// All envelopes in a batch share the same jobId (bundle or single).
	jobID := envelopes[0].Processing.JobID

	d.mu.RLock()
	ch, ok := d.subs[jobID]
	d.mu.RUnlock()

	if !ok {
		// No subscriber yet — buffer as orphan.
		d.mu.Lock()
		d.orphanBuf = append(d.orphanBuf, orphanMsg{body: body, receiptHandle: receiptHandle, arrivedAt: time.Now()})
		d.mu.Unlock()
		return
	}

	for _, env := range envelopes {
		ch <- env
	}

	// Acknowledge the SQS message after routing.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = d.client.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(d.client.outputQueueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
}

// flushOrphans delivers any buffered orphans for jobId to ch.
func (d *MessageDispatcher) flushOrphans(jobID string, ch chan Envelope) {
	d.mu.Lock()
	remaining := d.orphanBuf[:0]
	for _, o := range d.orphanBuf {
		envs, err := expandMessage(o.body)
		if err == nil && len(envs) > 0 && envs[0].Processing.JobID == jobID {
			for _, env := range envs {
				ch <- env
			}
			// Acknowledge from SQS.
			go func(handle string) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_, _ = d.client.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      aws.String(d.client.outputQueueURL),
					ReceiptHandle: aws.String(handle),
				})
			}(o.receiptHandle)
		} else {
			remaining = append(remaining, o)
		}
	}
	d.orphanBuf = remaining
	d.mu.Unlock()
}

// expireOrphans returns long-lived orphans to SQS by resetting their visibility
// timeout to 0 so they can be re-delivered on the next polling cycle.
func (d *MessageDispatcher) expireOrphans() {
	now := time.Now()
	d.mu.Lock()
	remaining := d.orphanBuf[:0]
	for _, o := range d.orphanBuf {
		if now.Sub(o.arrivedAt) >= orphanTTL {
			go func(handle string) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_, _ = d.client.sqsClient.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
					QueueUrl:          aws.String(d.client.outputQueueURL),
					ReceiptHandle:     aws.String(handle),
					VisibilityTimeout: 0,
				})
			}(o.receiptHandle)
		} else {
			remaining = append(remaining, o)
		}
	}
	d.orphanBuf = remaining
	d.mu.Unlock()
}

// CollectFromDispatcher receives exactly expected Envelopes from the dispatcher
// channel for a given jobId within timeout. It returns an error if the expected
// count is not reached.
func CollectFromDispatcher(ch <-chan Envelope, expected int, timeout time.Duration) ([]Envelope, error) {
	result := make([]Envelope, 0, expected)
	deadline := time.Now().Add(timeout)
	for len(result) < expected {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return result, fmt.Errorf("dispatcher timeout: got %d/%d envelopes", len(result), expected)
		}
		timer := time.NewTimer(remaining)
		select {
		case env, ok := <-ch:
			timer.Stop()
			if !ok {
				return result, fmt.Errorf("dispatcher channel closed with %d/%d envelopes", len(result), expected)
			}
			result = append(result, env)
		case <-timer.C:
			return result, fmt.Errorf("dispatcher timeout: got %d/%d envelopes", len(result), expected)
		}
	}
	return result, nil
}
