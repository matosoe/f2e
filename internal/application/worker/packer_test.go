package worker

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/f2e/f2e/internal/domain/f2e"
)

// makeEnvelope creates a minimal test envelope with the given eventId.
func makeEnvelope(eventID string) f2e.Envelope[f2e.RecordPayload] {
	rn := int64(1)
	return f2e.Envelope[f2e.RecordPayload]{
		Metadata:   f2e.Metadata{EventID: eventID, SourceRecordID: strings.Repeat("b", 64), Schema: f2e.Schema{ID: "s", Version: "1"}, Format: "json", CreatedAt: "2026-09-05T00:00:00Z"},
		Source:     f2e.Source{Type: "s3", Bucket: "bucket", Key: "key", ETag: `"etag"`, FileFormat: "text", FileSize: 100},
		Processing: f2e.Processing{JobID: "job1", ChunkID: "chunk1", RecordNumber: &rn},
		Data:       f2e.RecordPayload{Raw: "hello"},
	}
}

func newTestJob() f2e.ChunkJob {
	return f2e.ChunkJob{JobID: "job1", ChunkID: "chunk1"}
}

func newBundleCfg(maxEnv, maxBytes int) f2e.JobConfiguration {
	return f2e.JobConfiguration{
		MaxEventBytes:          256 * 1024,
		OutputMode:             f2e.OutputModeBundle,
		MaxEnvelopesPerMessage: maxEnv,
		MaxMessageBytes:        maxBytes,
	}
}

// TestPackerFlushOnCountLimit checks that a bundle is flushed when maxEnvelopes is reached.
func TestPackerFlushOnCountLimit(t *testing.T) {
	p := newBundlePacker(newTestJob(), newBundleCfg(2, 256*1024))

	env1 := makeEnvelope(strings.Repeat("a", 64))
	env2 := makeEnvelope(strings.Repeat("b", 64))
	env3 := makeEnvelope(strings.Repeat("c", 64))

	msg1, err := p.add(env1)
	if err != nil || msg1 != nil {
		t.Fatalf("first add: want nil msg, got msg=%v err=%v", msg1, err)
	}
	// Second add fills the bundle (maxEnvelopes=2); no flush yet (flush happens when third comes).
	msg2, err := p.add(env2)
	if err != nil || msg2 != nil {
		t.Fatalf("second add: want nil msg, got msg=%v err=%v", msg2, err)
	}
	// Third add triggers flush of the previous two.
	msg3, err := p.add(env3)
	if err != nil {
		t.Fatalf("third add: unexpected error %v", err)
	}
	if msg3 == nil {
		t.Fatal("third add: expected flushed bundle message, got nil")
	}

	// Decode flushed bundle.
	var bundle f2e.BundleEnvelope
	if err := json.Unmarshal([]byte(msg3.Body), &bundle); err != nil {
		t.Fatalf("decode flushed bundle: %v", err)
	}
	if bundle.SchemaVersion != f2e.BundleSchemaVersion {
		t.Fatalf("schemaVersion: want %q, got %q", f2e.BundleSchemaVersion, bundle.SchemaVersion)
	}
	if len(bundle.Items) != 2 {
		t.Fatalf("items: want 2, got %d", len(bundle.Items))
	}

	// Packer should still hold env3.
	final, err := p.flush()
	if err != nil || final == nil {
		t.Fatalf("final flush: want bundle, got msg=%v err=%v", final, err)
	}
	var finalBundle f2e.BundleEnvelope
	if err := json.Unmarshal([]byte(final.Body), &finalBundle); err != nil {
		t.Fatalf("decode final bundle: %v", err)
	}
	if len(finalBundle.Items) != 1 {
		t.Fatalf("final bundle items: want 1, got %d", len(finalBundle.Items))
	}
}

func TestPackerRespectsExplicitSingleEnvelopeLimit(t *testing.T) {
	p := newBundlePacker(newTestJob(), newBundleCfg(1, 256*1024))
	env1 := makeEnvelope(strings.Repeat("a", 64))
	env2 := makeEnvelope(strings.Repeat("b", 64))
	if msg, err := p.add(env1); err != nil || msg != nil {
		t.Fatalf("first add: msg=%v err=%v", msg, err)
	}
	msg, err := p.add(env2)
	if err != nil || msg == nil {
		t.Fatalf("second add: msg=%v err=%v", msg, err)
	}
	var bundle f2e.BundleEnvelope
	if err := json.Unmarshal([]byte(msg.Body), &bundle); err != nil {
		t.Fatal(err)
	}
	if len(bundle.Items) != 1 {
		t.Fatalf("items: want 1, got %d", len(bundle.Items))
	}
}

func TestPackerWithoutCountLimitFillsByBytes(t *testing.T) {
	p := newBundlePacker(newTestJob(), newBundleCfg(0, 256*1024))
	for i := 0; i < 101; i++ {
		env := makeEnvelope(fmt.Sprintf("%064d", i))
		if msg, err := p.add(env); err != nil || msg != nil {
			t.Fatalf("add %d: msg=%v err=%v", i, msg, err)
		}
	}
	msg, err := p.flush()
	if err != nil || msg == nil {
		t.Fatalf("flush: msg=%v err=%v", msg, err)
	}
	var bundle f2e.BundleEnvelope
	if err := json.Unmarshal([]byte(msg.Body), &bundle); err != nil {
		t.Fatal(err)
	}
	if len(bundle.Items) != 101 {
		t.Fatalf("items: want 101, got %d", len(bundle.Items))
	}
}

// TestPackerFlushOnBytesLimit checks that a bundle is flushed when adding an envelope
// would exceed maxMessageBytes.
func TestPackerFlushOnBytesLimit(t *testing.T) {
	env1 := makeEnvelope(strings.Repeat("a", 64))
	env2 := makeEnvelope(strings.Repeat("b", 64))
	env3 := makeEnvelope(strings.Repeat("c", 64))
	probe := newBundlePacker(newTestJob(), newBundleCfg(100, 256*1024))
	// Use the real serialized bundle, including attributes, as the boundary.
	maxBytes := probe.messageSize([]f2e.Envelope[f2e.RecordPayload]{env1, env2})
	p := newBundlePacker(newTestJob(), newBundleCfg(100, maxBytes))

	if _, err := p.add(env1); err != nil {
		t.Fatal(err)
	}
	if _, err := p.add(env2); err != nil {
		t.Fatal(err)
	}
	// Third add should trigger a flush.
	msg, err := p.add(env3)
	if err != nil {
		t.Fatalf("third add: %v", err)
	}
	if msg == nil {
		t.Fatal("expected flush on byte limit exceeded, got nil")
	}
	var bundle f2e.BundleEnvelope
	if err := json.Unmarshal([]byte(msg.Body), &bundle); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(bundle.Items) != 2 {
		t.Fatalf("want 2 items in flushed bundle, got %d", len(bundle.Items))
	}
}

// TestPackerEnvelopeTooLarge checks that a single envelope exceeding maxMessageBytes
// returns ErrEnvelopeTooLarge without data loss.
func TestPackerEnvelopeTooLarge(t *testing.T) {
	env := makeEnvelope(strings.Repeat("a", 64))
	// Force maxMessageBytes below what the complete physical message occupies.
	probe := newBundlePacker(newTestJob(), newBundleCfg(100, 256*1024))
	maxBytes := probe.messageSize([]f2e.Envelope[f2e.RecordPayload]{env}) - 1
	p := newBundlePacker(newTestJob(), newBundleCfg(100, maxBytes))

	_, err := p.add(env)
	var tooLarge f2e.ErrEnvelopeTooLarge
	if err == nil {
		t.Fatal("expected ErrEnvelopeTooLarge, got nil")
	}
	// Check via error type assertion through error interface.
	tooLarge, ok := err.(f2e.ErrEnvelopeTooLarge)
	if !ok {
		t.Fatalf("expected ErrEnvelopeTooLarge, got %T: %v", err, err)
	}
	if tooLarge.EventID != env.Metadata.EventID {
		t.Fatalf("EventID: want %q, got %q", env.Metadata.EventID, tooLarge.EventID)
	}
}

// TestPackerBundleIDIsStable checks that bundleID is deterministic regardless of insertion order.
func TestPackerBundleIDIsStable(t *testing.T) {
	env1 := makeEnvelope(strings.Repeat("a", 64))
	env2 := makeEnvelope(strings.Repeat("b", 64))

	id1 := bundleID([]f2e.Envelope[f2e.RecordPayload]{env1, env2})
	id2 := bundleID([]f2e.Envelope[f2e.RecordPayload]{env2, env1})
	if id1 != id2 {
		t.Fatalf("bundleID not stable: %s != %s", id1, id2)
	}
	if len(id1) != 64 {
		t.Fatalf("bundleID should be 64-char hex, got len %d", len(id1))
	}
}

// TestPackerFlushEmptyReturnsNil checks that flushing an empty packer is a no-op.
func TestPackerFlushEmptyReturnsNil(t *testing.T) {
	p := newBundlePacker(newTestJob(), newBundleCfg(10, 256*1024))
	msg, err := p.flush()
	if err != nil || msg != nil {
		t.Fatalf("flush empty: want nil, nil; got %v, %v", msg, err)
	}
}

// TestPackerBundleAttributesPresent checks that flushed bundles carry expected SQS attributes.
func TestPackerBundleAttributesPresent(t *testing.T) {
	p := newBundlePacker(newTestJob(), newBundleCfg(10, 256*1024))
	env := makeEnvelope(strings.Repeat("a", 64))
	if _, err := p.add(env); err != nil {
		t.Fatal(err)
	}
	msg, err := p.flush()
	if err != nil || msg == nil {
		t.Fatalf("flush: %v %v", msg, err)
	}
	if _, ok := msg.Attributes["schema"]; !ok {
		t.Error("missing schema attribute")
	}
	if _, ok := msg.Attributes["f2e-bundle-id"]; !ok {
		t.Error("missing f2e-bundle-id attribute")
	}
}
