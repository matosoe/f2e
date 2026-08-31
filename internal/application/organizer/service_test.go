package organizer

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/f2e/f2e/internal/adapter/inbound/s3event"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/platform/config"
)

func TestJobs(t *testing.T) {
	s := Service{Store: fakeHead{}, Config: config.Config{RecordLength: 10, RecordsPerChunk: 10}}
	b := []byte(`{"Records":[{"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"f2e-input"},"object":{"key":"input%2Fa+b.txt"}}}]}`)
	references, e := s3event.Parse(b)
	if e != nil {
		t.Fatal(e)
	}
	j, e := s.Jobs(context.Background(), references)
	if e != nil || len(j) != 3 || j[2].RecordCount != 5 || j[1].StartByte != 100 || j[2].EndByteInclusive != 249 {
		t.Fatalf("jobs=%+v err=%v", j, e)
	}
}

func TestPlanCarriesFormatAndOptionsToWorkerJob(t *testing.T) {
	s := Service{Store: fakeHead{}, Config: config.Config{RecordLength: 10, RecordsPerChunk: 10}}
	jobs, err := s.Plan(context.Background(), f2e.OrganizerRequest{SchemaVersion: f2e.SchemaVersion, Files: []f2e.FileRequest{{Bucket: "b", Key: "events.jsonl", DataType: f2e.DataTypeJSONL, Options: f2e.ProcessingOptions{BypassJSONValidation: true}}}})
	if err != nil || len(jobs) != 1 || jobs[0].DataType != f2e.DataTypeJSONL || !jobs[0].Options.BypassJSONValidation || jobs[0].EndByteInclusive != 249 {
		t.Fatalf("jobs=%+v err=%v", jobs, err)
	}
}

func TestVariableJobsCarryTrailingPadding(t *testing.T) {
	s := Service{Store: rangeStore{"aa\nbbb\ncccc\nd\n"}, Config: config.Config{RecordsPerChunk: 2}}
	jobs, err := s.Plan(context.Background(), f2e.OrganizerRequest{SchemaVersion: f2e.SchemaVersion, Files: []f2e.FileRequest{{Bucket: "b", Key: "records", DataType: f2e.DataTypeText, MaxRecordLengthBytes: 5}}})
	if err != nil || len(jobs) != 2 {
		t.Fatalf("jobs=%+v err=%v", jobs, err)
	}
	if jobs[0].StartByte != 0 || jobs[0].EndByteInclusive != 11 || jobs[0].TrailingPaddingBytes != 2 || jobs[1].StartByte != 10 || jobs[1].MaxRecordLengthBytes != 5 {
		t.Fatalf("jobs=%+v", jobs)
	}
}

func TestMultiLineJobsCleanBoundary(t *testing.T) {
	// Two records of exactly 10 bytes each; boundary falls between records.
	// Record 1: "1abc\n2def\n" (10 bytes), Record 2: "1xyz\n2uvw\n" (10 bytes).
	data := "1abc\n2def\n1xyz\n2uvw\n"
	s := Service{Store: rangeStore{data}, Config: config.Config{RecordsPerChunk: 1}}
	layout := f2e.MultiLineLayout{BreakMarker: "1", AcceptedPrefixes: []string{"2"}, MaxBytesPerRecord: 10}
	jobs, err := s.Plan(context.Background(), f2e.OrganizerRequest{
		SchemaVersion: f2e.SchemaVersion,
		Files:         []f2e.FileRequest{{Bucket: "b", Key: "k", DataType: f2e.DataTypeMultiLine, MultiLineLayout: layout}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d: %+v", len(jobs), jobs)
	}
	// nominalSize = 1*10 = 10; ownedEnd = 9; lookahead starts at byte 10 = "1xyz\n2uvw\n"
	// nextBreakOffset finds "1" at offset 0 → padding = 0
	if jobs[0].StartByte != 0 || jobs[0].EndByteInclusive != 9 || jobs[0].TrailingPaddingBytes != 0 {
		t.Fatalf("job[0]: %+v", jobs[0])
	}
	if jobs[1].StartByte != 10 || jobs[1].EndByteInclusive != 19 || jobs[1].TrailingPaddingBytes != 0 {
		t.Fatalf("job[1]: %+v", jobs[1])
	}
}

func TestMultiLineJobsBoundaryMidRecord(t *testing.T) {
	// Record 1: "1abc\n2def\n" (10 bytes), Record 2: "1xyz\n2uvw\n" (10 bytes).
	// nominalSize = 7: boundary falls inside Record 1 at byte 6 ("d" in "2def\n").
	// Lookahead from byte 7 = "ef\n1xyz\n2uvw\n"; next break at "1" → offset 3.
	// padding = 3; chunk 1 ends at byte 9.
	data := "1abc\n2def\n1xyz\n2uvw\n"
	s := Service{Store: rangeStore{data}, Config: config.Config{RecordsPerChunk: 1}}
	layout := f2e.MultiLineLayout{BreakMarker: "1", AcceptedPrefixes: []string{"2"}, MaxBytesPerRecord: 10}
	// Override nominalSize to 7 by using a fake MaxBytesPerRecord in the layout but
	// RecordsPerChunk=1 × MaxBytesPerRecord=10 would give 10; use MaxBytesPerRecord=7 directly.
	layout.MaxBytesPerRecord = 7
	jobs, err := s.Plan(context.Background(), f2e.OrganizerRequest{
		SchemaVersion: f2e.SchemaVersion,
		Files:         []f2e.FileRequest{{Bucket: "b", Key: "k", DataType: f2e.DataTypeMultiLine, MultiLineLayout: layout}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) == 0 {
		t.Fatal("no jobs returned")
	}
	// Chunk 1: start=0, ownedEnd=6, lookahead bytes 7-13 = "ef\n1xyz\n"
	// nextBreakOffset finds "1" at offset 3 → padding=3, EndByteInclusive=9
	if jobs[0].StartByte != 0 || jobs[0].EndByteInclusive != 9 || jobs[0].TrailingPaddingBytes != 3 {
		t.Fatalf("job[0]: %+v", jobs[0])
	}
	if jobs[0].MaxRecordLengthBytes != 7 {
		t.Fatalf("MaxRecordLengthBytes not propagated: %+v", jobs[0])
	}
}

func TestMultiLineJobsLayoutCarriedToJob(t *testing.T) {
	data := "1abc\n2def\n"
	s := Service{Store: rangeStore{data}, Config: config.Config{RecordsPerChunk: 1}}
	layout := f2e.MultiLineLayout{
		BreakPosition:     0,
		BreakMarker:       "1",
		AcceptedPrefixes:  []string{"2"},
		LineSeparator:     "|",
		MaxBytesPerRecord: 10,
	}
	jobs, err := s.Plan(context.Background(), f2e.OrganizerRequest{
		SchemaVersion: f2e.SchemaVersion,
		Files:         []f2e.FileRequest{{Bucket: "b", Key: "k", DataType: f2e.DataTypeMultiLine, MultiLineLayout: layout}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].MultiLineLayout.BreakMarker != "1" || jobs[0].MultiLineLayout.LineSeparator != "|" {
		t.Fatalf("layout not propagated: %+v", jobs[0].MultiLineLayout)
	}
}

func TestMultiLineJobsRejectsEmptyBreakMarker(t *testing.T) {
	s := Service{Store: rangeStore{"x\n"}, Config: config.Config{RecordsPerChunk: 1}}
	_, err := s.Plan(context.Background(), f2e.OrganizerRequest{
		SchemaVersion: f2e.SchemaVersion,
		Files: []f2e.FileRequest{{Bucket: "b", Key: "k", DataType: f2e.DataTypeMultiLine,
			MultiLineLayout: f2e.MultiLineLayout{MaxBytesPerRecord: 10}}},
	})
	if err == nil {
		t.Fatal("expected error for missing breakMarker")
	}
}

type fakeHead struct{}

func (fakeHead) Head(context.Context, string, string) (int64, string, string, error) {
	return 250, "etag", "", nil
}
func (fakeHead) GetRange(context.Context, string, string, int64, int64) (io.ReadCloser, error) {
	return nil, nil
}

type rangeStore struct{ data string }

func (s rangeStore) Head(context.Context, string, string) (int64, string, string, error) {
	return int64(len(s.data)), "etag", "", nil
}
func (s rangeStore) GetRange(_ context.Context, _ string, _ string, start, end int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.data[start : end+1])), nil
}
