package internal

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cucumber/godog"
)

// scenarioCtx holds per-scenario state shared across all step executions.
type scenarioCtx struct {
	aws           *AWSClient
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
}

// uniqueKey returns a time-based S3 key unique within the test run.
func uniqueKey(format string) string {
	return fmt.Sprintf("e2e/%s/%d.dat", format, time.Now().UnixNano())
}

// NewScenarioInitializer returns the Godog ScenarioInitializer bound to the given AWSClient.
// The same client is reused across all scenarios; a fresh scenarioCtx is created per scenario.
func NewScenarioInitializer(client *AWSClient) func(*godog.ScenarioContext) {
	return func(sc *godog.ScenarioContext) {
		s := &scenarioCtx{aws: client}

		sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
			s.reset()
			return ctx, nil
		})

		// ── File generation steps ───────────────────────────────────────────────
		sc.Step(`^I have a fixed-width file with (\d+) records$`, s.haveFixedWidthFile)
		sc.Step(`^I have a CSV file with (\d+) records$`, s.haveCSVFile)
		sc.Step(`^I have a JSONL file with (\d+) records$`, s.haveJSONLFile)
		sc.Step(`^I have an NDJSON file with (\d+) records$`, s.haveNDJSONFile)
		sc.Step(`^I have a text file with (\d+) records$`, s.haveTextFile)
		sc.Step(`^I have a multi-line file with header, (\d+) data records, and trailer$`, s.haveMultiLineFile)
		sc.Step(`^I have a JSON array file with (\d+) elements$`, s.haveJSONArrayFile)
		sc.Step(`^I have a binary file$`, s.haveBinaryFile)
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

func (s *scenarioCtx) haveFixedWidthFile(count int) error {
	s.fileContent = GenerateFixedWidth(count)
	s.dataType = "fixed-width"
	s.expectedCount = count
	return nil
}

func (s *scenarioCtx) haveCSVFile(count int) error {
	s.fileContent = GenerateCSV(count)
	s.dataType = "csv"
	s.expectedCount = count
	s.maxRecordLen = MaxRecordLenFor("csv", count)
	return nil
}

func (s *scenarioCtx) haveJSONLFile(count int) error {
	s.fileContent = GenerateJSONL(count)
	s.dataType = "jsonl"
	s.expectedCount = count
	s.maxRecordLen = MaxRecordLenFor("jsonl", count)
	return nil
}

func (s *scenarioCtx) haveNDJSONFile(count int) error {
	s.fileContent = GenerateNDJSON(count)
	s.dataType = "ndjson"
	s.expectedCount = count
	s.maxRecordLen = MaxRecordLenFor("ndjson", count)
	return nil
}

func (s *scenarioCtx) haveTextFile(count int) error {
	s.fileContent = GenerateText(count)
	s.dataType = "text"
	s.expectedCount = count
	s.maxRecordLen = MaxRecordLenFor("text", count)
	return nil
}

func (s *scenarioCtx) haveMultiLineFile(count int) error {
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
	return nil
}

func (s *scenarioCtx) haveJSONArrayFile(count int) error {
	s.fileContent = GenerateJSONArray(count)
	s.dataType = "json"
	s.expectedCount = count
	// MaxBytesPerElement is always required by the organizer for json type.
	s.jsonArrayLayout = JSONArrayLayout{
		MaxBytesPerElement: maxBytesPerJSONElement,
	}
	return nil
}

func (s *scenarioCtx) haveBinaryFile() error {
	s.fileContent = GenerateBinary()
	s.dataType = "binary"
	s.expectedCount = 1
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

	if err := s.aws.UploadFile(ctx, s.s3Key, s.fileContent); err != nil {
		return fmt.Errorf("upload %s: %w", s.s3Key, err)
	}

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
	return s.aws.SendOrganizerRequest(ctx, req)
}

// uploadThroughConfiguredS3Prefix exercises the S3 → Organizer path. The
// Organizer must resolve dataType and limits from SSM; no explicit request is
// published to file-intake.
func (s *scenarioCtx) uploadThroughConfiguredS3Prefix(ctx context.Context) error {
	if len(s.fileContent) == 0 {
		return fmt.Errorf("file content not set — call a 'I have a ... file' step first")
	}
	s.s3Key = fmt.Sprintf("example-%s/%d.dat", s.dataType, time.Now().UnixNano())
	if err := s.aws.UploadFile(ctx, s.s3Key, s.fileContent); err != nil {
		return fmt.Errorf("upload %s: %w", s.s3Key, err)
	}
	return nil
}

// receiveExactly is the core assertion step.
// For ≤1000 records it drains and stores all messages for further validation.
// For >1000 records it polls the approximate queue count only (draining would be too slow).
func (s *scenarioCtx) receiveExactly(ctx context.Context, expected, timeoutSec int) error {
	timeout := time.Duration(timeoutSec) * time.Second

	if expected <= smallFileThreshold {
		msgs, err := s.aws.ConsumeAll(ctx, expected, timeout)
		if err != nil {
			return err
		}
		if len(msgs) != expected {
			return fmt.Errorf("expected exactly %d messages, received %d", expected, len(msgs))
		}
		s.receivedMessages = msgs
		return nil
	}

	// Count-only path for large files.
	actual, err := s.aws.WaitForCount(ctx, expected, timeout)
	if err != nil {
		return fmt.Errorf("expected %d messages, last observed ~%d: %w", expected, actual, err)
	}
	return s.aws.DrainOutputQueue(ctx)
}

// noEventsWithin asserts that no messages appear in the output queue within the given window.
func (s *scenarioCtx) noEventsWithin(ctx context.Context, waitSec int) error {
	select {
	case <-time.After(time.Duration(waitSec) * time.Second):
	case <-ctx.Done():
		return ctx.Err()
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
	eventIDs := make(map[string]struct{}, len(s.receivedMessages))
	sourceRecordIDs := make(map[string]struct{}, len(s.receivedMessages))
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
		case "fixed-width", "jsonl", "ndjson", "text", "multi-line":
			if env.Data.Raw == "" {
				return fmt.Errorf("message[%d]: data.raw is empty for %s record", i, s.dataType)
			}
		case "csv":
			if len(env.Data.Fields) == 0 {
				return fmt.Errorf("message[%d]: data.fields is empty for csv record", i)
			}
		case "json":
			if env.Data.Raw == "" {
				return fmt.Errorf("message[%d]: data.raw is empty for json element", i)
			}
		case "binary":
			if env.Data.Base64 == "" || env.Data.Raw != "" {
				return fmt.Errorf("message[%d]: invalid binary payload", i)
			}
		}
	}
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
