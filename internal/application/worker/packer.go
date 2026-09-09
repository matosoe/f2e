package worker

// packer.go — streaming deterministic envelope packer for bundle output mode.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/domain/f2e"
)

// bundlePacker retains the already serialised representation of each envelope.
// That makes the admission check O(1): it only adds the bytes of the candidate
// item (and one comma when required), instead of re-marshalling the complete
// growing bundle for every record.
//
// It is not safe for concurrent use; each streamWithMetrics goroutine owns one.
type bundlePacker struct {
	job             f2e.ChunkJob
	maxEnvelopes    int
	maxMessageBytes int

	items    [][]byte
	eventIDs []string
	// emptyMessageSize is the physical SQS size of this bundle with Items: [].
	// Both a real and placeholder bundle ID have 64 hexadecimal bytes, so the
	// value remains exact until flush.
	emptyMessageSize   int
	currentMessageSize int
}

func newBundlePacker(job f2e.ChunkJob, cfg f2e.JobConfiguration) *bundlePacker {
	maxEnv := cfg.MaxEnvelopesPerMessage
	maxBytes := cfg.MaxMessageBytes
	if maxBytes <= 0 || maxBytes > port.MaxPhysicalMessageBytes {
		maxBytes = port.MaxPhysicalMessageBytes
	}
	p := &bundlePacker{job: job, maxEnvelopes: maxEnv, maxMessageBytes: maxBytes}
	p.emptyMessageSize = p.physicalMessageSize(bundleIDPlaceholder, nil)
	p.currentMessageSize = p.emptyMessageSize
	return p
}

const bundleIDPlaceholder = "0000000000000000000000000000000000000000000000000000000000000000"

// add retains serialised, the one canonical JSON representation made by the
// Worker. It never serialises a candidate bundle to decide whether it fits.
func (p *bundlePacker) add(env f2e.Envelope[f2e.RecordPayload], serialised []byte) (*port.OutboundMessage, error) {
	if len(serialised) == 0 {
		return nil, fmt.Errorf("empty serialised envelope")
	}

	candidateSize := p.sizeWith(serialised)
	if len(p.items) == 0 && candidateSize > p.maxMessageBytes {
		return nil, f2e.ErrEnvelopeTooLarge{
			EventID:     env.Metadata.EventID,
			ActualBytes: candidateSize,
			LimitBytes:  p.maxMessageBytes,
		}
	}

	var flushed *port.OutboundMessage
	if len(p.items) > 0 && ((p.maxEnvelopes > 0 && len(p.items) >= p.maxEnvelopes) || candidateSize > p.maxMessageBytes) {
		var err error
		flushed, err = p.flush()
		if err != nil {
			return nil, err
		}
		candidateSize = p.sizeWith(serialised)
	}

	p.items = append(p.items, serialised)
	p.eventIDs = append(p.eventIDs, env.Metadata.EventID)
	p.currentMessageSize = candidateSize
	return flushed, nil
}

// sizeWith returns the exact physical SQS message size after appending one
// serialised JSON item. The JSON body of an empty bundle already contains [];
// every item adds its bytes and every item after the first adds one comma.
func (p *bundlePacker) sizeWith(serialised []byte) int {
	size := p.currentMessageSize + len(serialised)
	if len(p.items) > 0 {
		size++
	}
	return size
}

func (p *bundlePacker) physicalMessageSize(id string, items [][]byte) int {
	body := p.serialiseBundle(id, items)
	return port.OutboundMessageSize(port.OutboundMessage{Body: string(body), Attributes: bundleAttributes(id)})
}

// serialiseBundle combines the immutable, individually serialised envelopes
// without asking encoding/json to serialise those envelopes again.
func (p *bundlePacker) serialiseBundle(id string, items [][]byte) []byte {
	var body bytes.Buffer
	body.Grow(p.emptyBodyCapacity(items))
	body.WriteString(`{"schemaVersion":`)
	writeJSONString(&body, f2e.BundleSchemaVersion)
	body.WriteString(`,"bundleId":`)
	writeJSONString(&body, id)
	body.WriteString(`,"jobId":`)
	writeJSONString(&body, p.job.JobID)
	body.WriteString(`,"chunkId":`)
	writeJSONString(&body, p.job.ChunkID)
	body.WriteString(`,"items":[`)
	for i, item := range items {
		if i > 0 {
			body.WriteByte(',')
		}
		body.Write(item)
	}
	body.WriteString(`]}`)
	return body.Bytes()
}

func (p *bundlePacker) emptyBodyCapacity(items [][]byte) int {
	size := 128
	for _, item := range items {
		size += len(item) + 1
	}
	return size
}

func writeJSONString(dst *bytes.Buffer, value string) {
	b, _ := json.Marshal(value) // marshaling a string cannot fail.
	dst.Write(b)
}

func bundleAttributes(id string) map[string]port.MessageAttribute {
	return map[string]port.MessageAttribute{
		"schema":        {DataType: "String", Value: f2e.BundleSchemaVersion},
		"f2e-bundle-id": {DataType: "String", Value: id},
	}
}

// flush constructs the body once, after the bundle boundary is known. Sorting
// is deliberately deferred to this point so bundle identity remains independent
// of insertion order without making each add increasingly expensive.
func (p *bundlePacker) flush() (*port.OutboundMessage, error) {
	if len(p.items) == 0 {
		return nil, nil
	}
	id := bundleIDFromEventIDs(p.eventIDs)
	body := p.serialiseBundle(id, p.items)
	msg := &port.OutboundMessage{Body: string(body), Attributes: bundleAttributes(id), LogicalEvents: len(p.items)}
	if actual := port.OutboundMessageSize(*msg); actual > p.maxMessageBytes {
		return nil, fmt.Errorf("bundle serialised to %d bytes, exceeds maxMessageBytes %d", actual, p.maxMessageBytes)
	}
	p.items = nil
	p.eventIDs = nil
	p.currentMessageSize = p.emptyMessageSize
	return msg, nil
}

// bundleID derives a stable identity from sorted event IDs. Kept as a helper
// for tests and callers that already have envelopes.
func bundleID(items []f2e.Envelope[f2e.RecordPayload]) string {
	ids := make([]string, len(items))
	for i, item := range items {
		ids[i] = item.Metadata.EventID
	}
	return bundleIDFromEventIDs(ids)
}

func bundleIDFromEventIDs(eventIDs []string) string {
	ids := append([]string(nil), eventIDs...)
	sort.Strings(ids)
	length := 0
	for _, id := range ids {
		length += len(id)
	}
	var concat strings.Builder
	concat.Grow(length)
	for _, id := range ids {
		concat.WriteString(id)
	}
	h := sha256.Sum256([]byte(concat.String()))
	return hex.EncodeToString(h[:])
}
