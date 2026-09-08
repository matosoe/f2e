package worker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/f2e/f2e/internal/application/port"
)

// concurrentSender dispatches SQS SendMessageBatch calls with bounded
// parallelism. Batches are submitted in production order; responses are
// collected asynchronously. The first send error cancels all pending sends
// and is returned by wait().
//
// Usage:
//
//	cs := newConcurrentSender(ctx, queue, queueURL, concurrency)
//	cs.submit(batch)
//	…
//	if err := cs.wait(); err != nil { … }
type concurrentSender struct {
	ctx               context.Context
	cancel            context.CancelFunc
	queue             port.Queue
	url               string
	sem               chan struct{} // limits in-flight goroutines
	wg                sync.WaitGroup
	mu                sync.Mutex
	firstErr          error
	cancellationErr   error
	confirmedMessages int64
	confirmedEvents   int64
	confirmedCalls    int64
}

func newConcurrentSender(ctx context.Context, queue port.Queue, url string, concurrency int) *concurrentSender {
	if concurrency < 1 {
		concurrency = 1
	}
	ctx, cancel := context.WithCancel(ctx)
	return &concurrentSender{
		ctx:    ctx,
		cancel: cancel,
		queue:  queue,
		url:    url,
		sem:    make(chan struct{}, concurrency),
	}
}

// submit enqueues a batch for sending. It blocks until a concurrency slot is
// available or the context is cancelled.
func (cs *concurrentSender) submit(batch []port.OutboundMessage) error {
	for _, part := range splitSendBatches(batch) {
		if err := cs.submitOne(part); err != nil {
			return err
		}
	}
	return nil
}

// splitSendBatches enforces both SQS limits before any request is made. A
// message above the multi-entry allowance is legal only as a singleton.
func splitSendBatches(messages []port.OutboundMessage) [][]port.OutboundMessage {
	var batches [][]port.OutboundMessage
	for _, message := range messages {
		if port.OutboundMessageSize(message) > port.MaxPhysicalMessageBytes {
			// submitOne turns this into a useful deterministic error.
			batches = append(batches, []port.OutboundMessage{message})
			continue
		}
		if len(batches) == 0 || len(batches[len(batches)-1]) == port.MaxMessagesPerBatch || port.OutboundMessageSize(message) > port.MaxMultiMessageBatchBytes || (len(batches[len(batches)-1]) > 0 && batchSize(batches[len(batches)-1])+port.OutboundMessageSize(message) > port.MaxMultiMessageBatchBytes) {
			batches = append(batches, nil)
		}
		last := len(batches) - 1
		batches[last] = append(batches[last], message)
	}
	return batches
}

func batchSize(batch []port.OutboundMessage) int {
	total := 0
	for _, message := range batch {
		total += port.OutboundMessageSize(message)
	}
	return total
}

func (cs *concurrentSender) submitOne(batch []port.OutboundMessage) error {
	if len(batch) == 0 {
		return nil
	}
	for _, message := range batch {
		if port.OutboundMessageSize(message) > port.MaxPhysicalMessageBytes {
			return fmt.Errorf("outbound message size %d exceeds %d byte budget", port.OutboundMessageSize(message), port.MaxPhysicalMessageBytes)
		}
	}
	// Check for a prior error before occupying a slot.
	cs.mu.Lock()
	err := cs.firstErr
	cs.mu.Unlock()
	if err != nil {
		return err
	}

	select {
	case cs.sem <- struct{}{}: // acquire slot
	case <-cs.ctx.Done():
		return cs.ctx.Err()
	}

	cs.wg.Add(1)
	go func(msgs []port.OutboundMessage) {
		defer cs.wg.Done()
		defer func() { <-cs.sem }() // release slot
		if sendErr := cs.send(msgs); sendErr != nil {
			cs.mu.Lock()
			if isCancellation(sendErr) {
				// A sibling's cancellation is a consequence, not the cause. Keep
				// it only for the case where the caller cancelled the root context.
				if cs.cancellationErr == nil {
					cs.cancellationErr = sendErr
				}
			} else if cs.firstErr == nil {
				cs.firstErr = sendErr
				cs.cancel()
			}
			cs.mu.Unlock()
		}
	}(batch)
	return nil
}

func isCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

type permanentError interface{ Permanent() bool }

func isPermanent(err error) bool {
	var permanent permanentError
	return errors.As(err, &permanent) && permanent.Permanent()
}

// send is the retry logic for a single batch (mirrors the original sendBatch closure).
func (cs *concurrentSender) send(batch []port.OutboundMessage) error {
	pending := append([]port.OutboundMessage(nil), batch...)
	for attempt := 0; attempt < 3; attempt++ {
		failed, e := cs.queue.Send(cs.ctx, cs.url, pending)
		if e == nil {
			failedSet := make(map[int]struct{}, len(failed))
			for _, index := range failed {
				failedSet[index] = struct{}{}
			}
			cs.mu.Lock()
			cs.confirmedCalls++
			for index, message := range pending {
				if _, failed := failedSet[index]; failed {
					continue
				}
				cs.confirmedMessages++
				if message.LogicalEvents > 0 {
					cs.confirmedEvents += int64(message.LogicalEvents)
				} else {
					cs.confirmedEvents++
				}
			}
			cs.mu.Unlock()
		}
		if e == nil && len(failed) == 0 {
			return nil
		}
		if e != nil && isPermanent(e) {
			return fmt.Errorf("SendMessageBatch permanent failure (attempt=%d messages=%d bytes=%d): %w", attempt+1, len(pending), batchSize(pending), e)
		}
		if e != nil && attempt == 2 {
			return fmt.Errorf("SendMessageBatch failed after attempt %d (messages=%d bytes=%d): %w", attempt+1, len(pending), batchSize(pending), e)
		}
		if e == nil {
			next := make([]port.OutboundMessage, 0, len(failed))
			for _, index := range failed {
				if index >= 0 && index < len(pending) {
					next = append(next, pending[index])
				}
			}
			pending = next
		}
		delay := time.Duration((1<<attempt)*50+rand.IntN(50)) * time.Millisecond
		select {
		case <-cs.ctx.Done():
			return cs.ctx.Err()
		case <-time.After(delay):
		}
	}
	return fmt.Errorf("SendMessageBatch partial failure after retries (messages=%d bytes=%d)", len(pending), batchSize(pending))
}

// wait blocks until all in-flight sends complete and returns the first error.
func (cs *concurrentSender) wait() error {
	cs.wg.Wait()
	cs.cancel()
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.firstErr == nil && cs.cancellationErr != nil {
		return cs.cancellationErr
	}
	return cs.firstErr
}

func (cs *concurrentSender) counts() (messages, events, calls int64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.confirmedMessages, cs.confirmedEvents, cs.confirmedCalls
}
