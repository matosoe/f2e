package s3event

import "testing"

func TestParseMultipleRecordsFiltersEventsAndDecodesKeys(t *testing.T) {
	body := []byte(`{"Records":[
    {"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"b"},"object":{"key":"folder%2Fa+b.csv"}}},
    {"eventName":"ObjectRemoved:Delete","s3":{"bucket":{"name":"b"},"object":{"key":"ignored"}}},
    {"eventName":"ObjectCreated:CompleteMultipartUpload","s3":{"bucket":{"name":"b"},"object":{"key":"second.jsonl"}}}
  ]}`)
	got, err := Parse(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Key != "folder/a b.csv" || got[1].Key != "second.jsonl" {
		t.Fatalf("references=%+v", got)
	}
}

func TestParseRejectsMalformedNotification(t *testing.T) {
	if _, err := Parse([]byte(`{"Records":`)); err == nil {
		t.Fatal("expected malformed JSON error")
	}
}
