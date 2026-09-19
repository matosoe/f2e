package config

import (
	"strconv"
	"testing"

	"github.com/matosoe/f2e/internal/application/port"
)

func TestLoadRejectsInvalidNumericConfiguration(t *testing.T) {
	t.Setenv("F2E_BATCH_SIZE", "11")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid batch size error")
	}
}

func TestLoadUsesSafeDefaults(t *testing.T) {
	for _, key := range []string{"F2E_RECORDS_PER_CHUNK", "F2E_BATCH_SIZE", "F2E_MAX_EVENT_BYTES", "F2E_MAX_RECEIVE_COUNT", "F2E_LEDGER_RETENTION_DAYS", "F2E_MAX_FILE_BYTES", "F2E_MAX_CHUNK_BYTES"} {
		t.Setenv(key, "")
	}
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.BatchSize != 10 || c.MaxEventBytes != port.DefaultMaxEventBytes || c.MaxReceiveCount != 3 || c.LedgerRetentionDays != 90 || c.MaxFileBytes != 10*1024*1024*1024 || c.MaxChunkBytes != 64*1024*1024 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestLoadCapsMaxEventBytesAtSQSLimit(t *testing.T) {
	t.Setenv("F2E_MAX_EVENT_BYTES", strconv.Itoa(port.MaxSQSMessageBytes))
	if _, err := Load(); err != nil {
		t.Fatalf("SQS maximum should be accepted: %v", err)
	}
	t.Setenv("F2E_MAX_EVENT_BYTES", strconv.Itoa(port.MaxSQSMessageBytes+1))
	if _, err := Load(); err == nil {
		t.Fatal("value above the SQS maximum should be rejected")
	}
}
