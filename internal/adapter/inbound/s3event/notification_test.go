package s3event

import "testing"

func TestParseMultipleRecordsFiltersEventsAndDecodesKeys(t *testing.T) {
	body := []byte(`{"Records":[
    {"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"b"},"object":{"key":"folder%2Fa+b.csv","versionId":"version-1"}}},
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
	if got[0].VersionID != "version-1" {
		t.Fatalf("versionId=%q, want version-1", got[0].VersionID)
	}
}

func TestParseRejectsMalformedNotification(t *testing.T) {
	if _, err := Parse([]byte(`{"Records":`)); err == nil {
		t.Fatal("expected malformed JSON error")
	}
}
