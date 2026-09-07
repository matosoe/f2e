package worker

import (
	"context"
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
	ctx      context.Context
	cancel   context.CancelFunc
	queue    port.Queue
	url      string
	sem      chan struct{} // limits in-flight goroutines
	wg       sync.WaitGroup
	mu       sync.Mutex
	firstErr error
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
	if len(batch) == 0 {
		return nil
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
			if cs.firstErr == nil {
				cs.firstErr = sendErr
				cs.cancel()
			}
			cs.mu.Unlock()
		}
	}(batch)
	return nil
}

// send is the retry logic for a single batch (mirrors the original sendBatch closure).
func (cs *concurrentSender) send(batch []port.OutboundMessage) error {
	pending := append([]port.OutboundMessage(nil), batch...)
	for attempt := 0; attempt < 3; attempt++ {
		failed, e := cs.queue.Send(cs.ctx, cs.url, pending)
		if e == nil && len(failed) == 0 {
			return nil
		}
		if e != nil && attempt == 2 {
			return e
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
	return fmt.Errorf("output batch partial failure after retries")
}

// wait blocks until all in-flight sends complete and returns the first error.
func (cs *concurrentSender) wait() error {
	cs.wg.Wait()
	cs.cancel()
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.firstErr
}
