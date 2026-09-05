package internal

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cucumber/godog"
)

// scenarioCtx holds per-scenario state shared across all step executions.
type scenarioCtx struct {
	aws           *AWSClient
	dispatcher    *MessageDispatcher // non-nil when E2E_CONCURRENCY > 1
	dispatcherCh  <-chan Envelope    // per-scenario channel set after dispatcher.Subscribe
	fileContent   []byte
	s3Key         string
	dataType      string
	expectedCount int

	// multiLineLayout and jsonArrayLayout are non-zero only for their respective formats.
	multiLineLayout MultiLineLayout
	jsonArrayLayout JSONArrayLayout
	// maxRecordLen is set for variable-length formats when chunking is needed (>1000 records).
	maxRecordLen int64

	// receivedMessages is populated for detailed-validation scenarios (≤1000 records).
	receivedMessages []Envelope

	// metrics accumulates instrumentation for this scenario; exported to RunReport at suite end.
	metrics       ScenarioMetrics
	scenarioName  string
	scenarioStart time.Time
	lastCounters  *APICounters // reference held so After hook can copy before nil-ing
}

func (s *scenarioCtx) reset() {
	s.fileContent = nil
	s.s3Key = ""
	s.dataType = ""
	s.expectedCount = 0
	s.multiLineLayout = MultiLineLayout{}
	s.jsonArrayLayout = JSONArrayLayout{}
	s.maxRecordLen = 0
	s.receivedMessages = nil
	s.dispatcherCh = nil
	s.metrics = ScenarioMetrics{}
	s.scenarioName = ""
	s.scenarioStart = time.Time{}
	s.lastCounters = nil
}

// uniqueKey returns a time-based S3 key unique within the test run.
func uniqueKey(format string) string {
	return fmt.Sprintf("e2e/%s/%d.dat", format, time.Now().UnixNano())
}

// NewScenarioInitializer returns the Godog ScenarioInitializer bound to the given AWSClient.
// The same client is reused across all scenarios; a fresh scenarioCtx is created per scenario.
func NewScenarioInitializer(client *AWSClient) func(*godog.ScenarioContext) {
	return NewScenarioInitializerWithMetrics(client, nil)
}

// NewScenarioInitializerWithMetrics is like NewScenarioInitializer but appends one
// ScenarioMetrics entry per scenario to collector after the scenario completes.
// collector is written under mu so it is safe for concurrent scenarios.
func NewScenarioInitializerWithMetrics(client *AWSClient, collector *[]ScenarioMetrics) func(*godog.ScenarioContext) {
	return newInitializer(client, nil, collector)
}

// NewScenarioInitializerWithDispatcher enables parallel-safe message collection
// by routing envelopes through dispatcher instead of polling the shared queue
// directly. Use this when godog.Options.Concurrency > 1.
func NewScenarioInitializerWithDispatcher(client *AWSClient, d *MessageDispatcher, collector *[]ScenarioMetrics) func(*godog.ScenarioContext) {
	return newInitializer(client, d, collector)
}

// newInitializer is the shared factory used by the public initializer variants.
var collectorMu sync.Mutex

func newInitializer(client *AWSClient, d *MessageDispatcher, collector *[]ScenarioMetrics) func(*godog.ScenarioContext) {
	return func(sc *godog.ScenarioContext) {
		// Each scenario gets its own AWSClient fork so Counters is never shared.
		s := &scenarioCtx{aws: client.Fork(), dispatcher: d}

		sc.Before(func(ctx context.Context, scenario *godog.Scenario) (context.Context, error) {
			s.reset()
			s.scenarioName = scenario.Name
			s.scenarioStart = time.Now()
			counters := &APICounters{}
			s.lastCounters = counters
			s.aws.Counters = counters
			return ctx, nil
		})

		sc.After(func(ctx context.Context, _ *godog.Scenario, _ error) (context.Context, error) {
			s.aws.Counters = nil
			// Unsubscribe from dispatcher if we registered (clean-up is idempotent).
			if s.dispatcher != nil && s.s3Key != "" {
				s.dispatcher.Unsubscribe(s.s3Key)
			}
			if collector != nil {
				m := s.metrics
				m.ScenarioName = s.scenarioName
				m.DataType = s.dataType
				m.RecordCount = s.expectedCount
				m.TotalMs = time.Since(s.scenarioStart).Milliseconds()
				if s.lastCounters != nil {
					m.API = *s.lastCounters
				}
				collectorMu.Lock()
				*collector = append(*collector, m)
				collectorMu.Unlock()
			}
			return ctx, nil
		})

		// ── File generation steps ───────────────────────────────────────────────
		sc.Step(`^I have a text file with (\d+) records$`, s.haveTextFile)
		sc.Step(`^I have a multi-line file with header, (\d+) data records, and trailer$`, s.haveMultiLineFile)
		sc.Step(`^I have a JSON array file with (\d+) elements$`, s.haveJSONArrayFile)
		sc.Step(`^I have an empty "([^"]*)" file$`, s.haveEmptyFile)

		// ── Upload + trigger step ───────────────────────────────────────────────
		sc.Step(`^I upload and process the file$`, s.uploadAndProcess)
		sc.Step(`^I upload the file through its configured S3 prefix$`, s.uploadThroughConfiguredS3Prefix)

		// ── Assertion steps ─────────────────────────────────────────────────────
		sc.Step(`^I receive exactly (\d+) events within (\d+) seconds$`, s.receiveExactly)
		sc.Step(`^no events are produced within (\d+) seconds$`, s.noEventsWithin)
		sc.Step(`^all events are valid F2E envelopes$`, s.allEventsAreValidEnvelopes)
		sc.Step(`^all event record numbers from (\d+) to (\d+) are present$`, s.recordNumbersArePresent)
	}
}

// ── Step implementations ────────────────────────────────────────────────────

func (s *scenarioCtx) haveTextFile(count int) error {
	t := StartPhase()
	s.fileContent = GenerateText(count)
	s.dataType = "text"
	s.expectedCount = count
	s.maxRecordLen = MaxRecordLenFor("text", count)
	s.metrics.SetupMs += t.ElapsedMs()
	return nil
}

func (s *scenarioCtx) haveMultiLineFile(count int) error {
	t := StartPhase()
	s.fileContent = GenerateMultiLine(count)
	s.dataType = "multi-line"
	s.expectedCount = count
	// MaxBytesPerRecord is always required by the organizer for multi-line.
	s.multiLineLayout = MultiLineLayout{
		BreakMarker:       "D",
		BreakPosition:     0,
		AcceptedPrefixes:  []string{"D"},
		MaxBytesPerRecord: maxRecordBytesMultiLine,
	}
	s.metrics.SetupMs += t.ElapsedMs()
	return nil
}

func (s *scenarioCtx) haveJSONArrayFile(count int) error {
	t := StartPhase()
	s.fileContent = GenerateJSONArray(count)
	s.dataType = "json"
	s.expectedCount = count
	// MaxBytesPerElement is always required by the organizer for json type.
	s.jsonArrayLayout = JSONArrayLayout{
		MaxBytesPerElement: maxBytesPerJSONElement,
	}
	s.metrics.SetupMs += t.ElapsedMs()
	return nil
}

func (s *scenarioCtx) haveEmptyFile(format string) error {
	s.fileContent = []byte{}
	s.dataType = format
	s.expectedCount = 0
	return nil
}

func (s *scenarioCtx) uploadAndProcess(ctx context.Context) error {
	if len(s.fileContent) == 0 && s.expectedCount != 0 {
		return fmt.Errorf("file content not set — call a 'I have a ... file' step first")
	}
	s.s3Key = uniqueKey(s.dataType)

	// Subscribe to the dispatcher before uploading so no message is missed.
	if s.dispatcher != nil {
		s.dispatcherCh = s.dispatcher.Subscribe(s.s3Key)
	}

	uploadTimer := StartPhase()
	if err := s.aws.UploadFile(ctx, s.s3Key, s.fileContent); err != nil {
		return fmt.Errorf("upload %s: %w", s.s3Key, err)
	}
	s.metrics.UploadMs = uploadTimer.ElapsedMs()

	req := OrganizerRequest{
		SchemaVersion: schemaVersion,
		Files: []FileRequest{
			{
				Bucket:               s.aws.bucket,
				Key:                  s.s3Key,
				DataType:             s.dataType,
				MaxRecordLengthBytes: s.maxRecordLen,
				MultiLineLayout:      s.multiLineLayout,
				JSONArrayLayout:      s.jsonArrayLayout,
			},
		},
	}
	intakeTimer := StartPhase()
	err := s.aws.SendOrganizerRequest(ctx, req)
	s.metrics.IntakeMs = intakeTimer.ElapsedMs()
	return err
}

// uploadThroughConfiguredS3Prefix exercises the S3 → Organizer path. The
// Organizer must resolve dataType and limits from SSM; no explicit request is
// published to file-intake.
func (s *scenarioCtx) uploadThroughConfiguredS3Prefix(ctx context.Context) error {
	if len(s.fileContent) == 0 {
		return fmt.Errorf("file content not set — call a 'I have a ... file' step first")
	}
	s.s3Key = fmt.Sprintf("example-%s/%d.dat", s.dataType, time.Now().UnixNano())

	// Subscribe to the dispatcher before uploading so no message is missed.
	if s.dispatcher != nil {
		s.dispatcherCh = s.dispatcher.Subscribe(s.s3Key)
	}

	uploadTimer := StartPhase()
	if err := s.aws.UploadFile(ctx, s.s3Key, s.fileContent); err != nil {
		return fmt.Errorf("upload %s: %w", s.s3Key, err)
	}
	s.metrics.UploadMs = uploadTimer.ElapsedMs()
	return nil
}

// receiveExactly is the core assertion step.
// In dispatcher mode (parallel), it collects envelopes from the per-scenario
// channel. In sequential mode it uses the concurrent queue collector.
// For >1000 records it polls the approximate queue count only (sequential only).
func (s *scenarioCtx) receiveExactly(ctx context.Context, expected, timeoutSec int) error {
	timeout := time.Duration(timeoutSec) * time.Second

	if s.dispatcherCh != nil {
		collectStart := time.Now()
		msgs, err := CollectFromDispatcher(s.dispatcherCh, expected, timeout)
		s.metrics.ConsumeMs = time.Since(collectStart).Milliseconds()
		if err != nil {
			return err
		}
		if len(msgs) != expected {
			return fmt.Errorf("expected exactly %d messages, received %d", expected, len(msgs))
		}
		s.receivedMessages = msgs
		return nil
	}

	if expected <= smallFileThreshold {
		collectStart := time.Now()
		msgs, err := s.aws.ConsumeAllConcurrent(ctx, expected, 4, timeout)
		elapsed := time.Since(collectStart).Milliseconds()
		s.metrics.ConsumeMs = elapsed
		if err != nil {
			return err
		}
		if len(msgs) != expected {
			return fmt.Errorf("expected exactly %d messages, received %d", expected, len(msgs))
		}
		s.receivedMessages = msgs
		return nil
	}

	// Count-only path for large files: measure wait separately from drain.
	waitTimer := StartPhase()
	actual, err := s.aws.WaitForCount(ctx, expected, timeout)
	s.metrics.WaitMs = waitTimer.ElapsedMs()
	if err != nil {
		return fmt.Errorf("expected %d messages, last observed ~%d: %w", expected, actual, err)
	}
	drainTimer := StartPhase()
	drainErr := s.aws.DrainOutputQueue(ctx)
	s.metrics.ConsumeMs = drainTimer.ElapsedMs()
	return drainErr
}

// noEventsWithin asserts that no messages appear for this scenario's job within
// the given window. In parallel mode it checks the per-scenario dispatcher
// channel; in sequential mode it checks the shared queue approximate count.
func (s *scenarioCtx) noEventsWithin(ctx context.Context, waitSec int) error {
	wait := time.Duration(waitSec) * time.Second
	select {
	case <-time.After(wait):
	case <-ctx.Done():
		return ctx.Err()
	}
	if s.dispatcherCh != nil {
		// In parallel mode: if a message arrived on our channel the scenario failed.
		select {
		case env, ok := <-s.dispatcherCh:
			if ok {
				return fmt.Errorf("expected 0 events but received envelope jobId=%s", env.Processing.JobID)
			}
		default:
		}
		return nil
	}
	count, err := s.aws.ApproximateCount(ctx)
	if err != nil {
		return err
	}
	if count > 0 {
		return fmt.Errorf("expected 0 events but output queue has ~%d", count)
	}
	return nil
}

// allEventsAreValidEnvelopes validates every received message for structural completeness.
func (s *scenarioCtx) allEventsAreValidEnvelopes(_ context.Context) error {
	if len(s.receivedMessages) == 0 {
		return fmt.Errorf("no messages to validate — 'I receive exactly N events' must run first")
	}
	validateTimer := StartPhase()
	defer func() { s.metrics.ValidateMs = validateTimer.ElapsedMs() }()

	eventIDs := make(map[string]struct{}, len(s.receivedMessages))
	sourceRecordIDs := make(map[string]struct{}, len(s.receivedMessages))
	var earliest, latest string
	for i, env := range s.receivedMessages {
		if env.Metadata.EventID == "" {
			return fmt.Errorf("message[%d]: metadata.eventId is empty", i)
		}
		if env.Metadata.SourceRecordID == "" {
			return fmt.Errorf("message[%d]: metadata.sourceRecordId is empty", i)
		}
		if _, exists := eventIDs[env.Metadata.EventID]; exists {
			return fmt.Errorf("message[%d]: duplicate metadata.eventId %s", i, env.Metadata.EventID)
		}
		if _, exists := sourceRecordIDs[env.Metadata.SourceRecordID]; exists {
			return fmt.Errorf("message[%d]: duplicate metadata.sourceRecordId %s", i, env.Metadata.SourceRecordID)
		}
		eventIDs[env.Metadata.EventID] = struct{}{}
		sourceRecordIDs[env.Metadata.SourceRecordID] = struct{}{}
		// Capture AWS-side timestamps from envelope.metadata.createdAt.
		// These use the Worker's clock and must not be subtracted from local times.
		if ts := env.Metadata.CreatedAt; ts != "" {
			if earliest == "" || ts < earliest {
				earliest = ts
			}
			if ts > latest {
				latest = ts
			}
		}
		if env.Metadata.Schema.ID == "" {
			return fmt.Errorf("message[%d]: metadata.schema.id is empty", i)
		}
		if env.Source.Type != "s3" {
			return fmt.Errorf("message[%d]: source.type = %q, want \"s3\"", i, env.Source.Type)
		}
		if env.Source.Bucket != s.aws.bucket {
			return fmt.Errorf("message[%d]: source.bucket = %q, want %q", i, env.Source.Bucket, s.aws.bucket)
		}
		if env.Source.Key != s.s3Key {
			return fmt.Errorf("message[%d]: source.key = %q, want %q", i, env.Source.Key, s.s3Key)
		}
		if env.Processing.JobID == "" {
			return fmt.Errorf("message[%d]: processing.jobId is empty", i)
		}
		if env.Processing.ChunkID == "" {
			return fmt.Errorf("message[%d]: processing.chunkId is empty", i)
		}
		if env.Processing.RecordNumber == nil {
			return fmt.Errorf("message[%d]: processing.recordNumber is nil", i)
		}
		if *env.Processing.RecordNumber < 1 {
			return fmt.Errorf("message[%d]: processing.recordNumber = %d, want >= 1", i, *env.Processing.RecordNumber)
		}
		switch s.dataType {
		case "text", "multi-line":
			if env.Data.Raw == "" {
				return fmt.Errorf("message[%d]: data.raw is empty for %s record", i, s.dataType)
			}
		case "json":
			if env.Data.Raw == "" {
				return fmt.Errorf("message[%d]: data.raw is empty for json element", i)
			}
		}
	}
	s.metrics.EarliestEnvelopeCreatedAt = earliest
	s.metrics.LatestEnvelopeCreatedAt = latest
	return nil
}

// recordNumbersArePresent verifies that receivedMessages contains all record numbers in [from, to].
func (s *scenarioCtx) recordNumbersArePresent(_ context.Context, from, to int) error {
	if len(s.receivedMessages) == 0 {
		return fmt.Errorf("no messages received")
	}
	seen := make(map[int64]bool, len(s.receivedMessages))
	for _, env := range s.receivedMessages {
		if env.Processing.RecordNumber == nil {
			return fmt.Errorf("nil recordNumber in received message")
		}
		n := *env.Processing.RecordNumber
		if seen[n] {
			return fmt.Errorf("duplicate recordNumber %d", n)
		}
		seen[n] = true
	}

	missing := make([]int64, 0)
	for n := int64(from); n <= int64(to); n++ {
		if !seen[n] {
			missing = append(missing, n)
		}
	}
	if len(missing) > 0 {
		sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
		if len(missing) > 10 {
			return fmt.Errorf("%d record numbers missing; first 10: %v", len(missing), missing[:10])
		}
		return fmt.Errorf("missing record numbers: %v", missing)
	}
	return nil
}
