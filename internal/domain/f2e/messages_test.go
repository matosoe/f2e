package f2e

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestEnvelopeV2AddsIdentityWithoutBreakingUnknownFieldConsumers(t *testing.T) {
	record := int64(1)
	envelope := Envelope[RecordPayload]{
		Metadata:   Metadata{EventID: strings.Repeat("a", 64), SourceRecordID: strings.Repeat("b", 64), Schema: Schema{ID: "record", Version: "2"}, Format: "json", CreatedAt: "2026-09-01T00:00:00Z"},
		Source:     Source{Type: "s3", Bucket: "bucket", Key: "key", ETag: `"etag"`, FileFormat: "text", FileSize: 4},
		Processing: Processing{JobID: "job", ChunkID: "chunk", RecordNumber: &record},
		Data:       RecordPayload{Raw: "abc"},
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	// A v1-style consumer decoding only its known fields continues to work.
	var legacy struct {
		Metadata struct {
			EventID string `json:"eventId"`
		} `json:"metadata"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &legacy); err != nil || legacy.Metadata.EventID != envelope.Metadata.EventID || len(legacy.Data) == 0 {
		t.Fatalf("legacy decode failed: err=%v value=%+v", err, legacy)
	}
	var current Envelope[RecordPayload]
	if err := json.Unmarshal(body, &current); err != nil || current.Metadata.SourceRecordID != envelope.Metadata.SourceRecordID {
		t.Fatalf("v2 round trip failed: err=%v value=%+v", err, current)
	}
}
