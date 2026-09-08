package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateExactFixedWidthRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.txt")
	if err := generate(path, 3, 100, false); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) != 300 {
		t.Fatalf("size=%d, want 300", len(body))
	}
	lines := strings.Split(strings.TrimSuffix(string(body), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("lines=%d, want 3", len(lines))
	}
	for i, line := range lines {
		if len(line)+1 != 100 {
			t.Fatalf("line %d occupies %d bytes including LF, want 100", i+1, len(line)+1)
		}
	}
	if !strings.HasPrefix(lines[0], "00000001") || !strings.HasPrefix(lines[2], "00000003") {
		t.Fatalf("unexpected identifiers: %q, %q", lines[0][:8], lines[2][:8])
	}

	manifestBody, err := os.ReadFile(path + ".manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var got manifest
	if err := json.Unmarshal(manifestBody, &got); err != nil {
		t.Fatal(err)
	}
	if got.Records != 3 || got.RecordLengthBytes != 100 || got.SizeBytes != 300 || len(got.SHA256) != 64 {
		t.Fatalf("manifest=%+v", got)
	}
}

func TestGenerateRefusesOverwriteWithoutForce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.txt")
	if err := os.WriteFile(path, []byte("mine"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := generate(path, 1, 100, false); err == nil {
		t.Fatal("expected overwrite error")
	}
	body, _ := os.ReadFile(path)
	if string(body) != "mine" {
		t.Fatalf("existing file changed: %q", body)
	}
}
