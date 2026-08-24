package organizer

import (
	"context"
	"github.com/f2e/f2e/internal/adapter/inbound/s3event"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/platform/config"
	"io"
	"strings"
	"testing"
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

type fakeHead struct{}

func (fakeHead) Head(context.Context, string, string) (int64, string, error) { return 250, "etag", nil }
func (fakeHead) GetRange(context.Context, string, string, int64, int64) (io.ReadCloser, error) {
	return nil, nil
}

type rangeStore struct{ data string }

func (s rangeStore) Head(context.Context, string, string) (int64, string, error) {
	return int64(len(s.data)), "etag", nil
}
func (s rangeStore) GetRange(_ context.Context, _ string, _ string, start, end int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.data[start : end+1])), nil
}
