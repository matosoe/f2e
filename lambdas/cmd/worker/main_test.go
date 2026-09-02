package main

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/f2e/f2e/internal/application/port"
	workerapp "github.com/f2e/f2e/internal/application/worker"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/platform/config"
)

type testResolver struct{}

func (testResolver) OpenChunkRange(context.Context, f2e.ChunkJob, int64, int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("aaa\n")), nil
}

type testQueue struct{ sent int }

func (q *testQueue) Send(_ context.Context, _ string, messages []port.OutboundMessage) ([]int, error) {
	q.sent += len(messages)
	return nil, nil
}

func TestHandlerReturnsOnlyInvalidMessageAsPartialBatchFailure(t *testing.T) {
	q := &testQueue{}
	service = workerapp.Service{
		Resolver: testResolver{}, Queue: q,
		Config: config.Config{RecordLength: 4, BatchSize: 10, MaxEventBytes: 256 * 1024, OutputQueueURL: "out", EventSchemaID: "test", EventSchemaVersion: "1", EventFormat: "json"},
	}
	job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "00000001", Bucket: "b", Key: "k", RecordCount: 1, RecordLengthBytes: 4, EndByteInclusive: 3, DataType: f2e.DataTypeFixedWidth}
	body, _ := json.Marshal(job)
	event := events.SQSEvent{Records: []events.SQSMessage{
		{MessageId: "valid", Body: string(body), Attributes: map[string]string{"ApproximateReceiveCount": "1"}},
		{MessageId: "invalid", Body: "{", Attributes: map[string]string{"ApproximateReceiveCount": "2"}},
	}}
	response, err := handler(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	if q.sent != 1 || len(response.BatchItemFailures) != 1 || response.BatchItemFailures[0].ItemIdentifier != "invalid" {
		t.Fatalf("sent=%d failures=%+v", q.sent, response.BatchItemFailures)
	}
}
