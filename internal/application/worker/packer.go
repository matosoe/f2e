package worker

// packer.go — streaming deterministic envelope packer for bundle output mode (T16).
//
// In single mode the worker sends one SQS message per envelope (existing behaviour).
// In bundle mode, outbound envelopes are grouped into BundleEnvelope messages bounded
// by maxEnvelopesPerMessage and maxMessageBytes. Each bundle is flushed as a single
// SendMessageBatch entry so the existing retry/backoff logic in streamWithMetrics
// continues to work unchanged.
//
// A single envelope that exceeds maxMessageBytes on its own produces
// ErrEnvelopeTooLarge: the record is counted as rejected and processing continues.
// The pipeline never truncates data.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/domain/f2e"
)

const (
	defaultMaxEnvelopesPerMessage = 100
	// bundleOverheadBytes is a conservative estimate of the JSON wrapper added
	// around the items array in a BundleEnvelope: schema version, bundleId, jobId,
	// chunkId keys and punctuation. Used when pre-checking whether an envelope fits.
	bundleOverheadBytes = 256
)

// bundlePacker accumulates envelopes and flushes complete bundles.
// It is not safe for concurrent use; each streamWithMetrics goroutine owns one.
type bundlePacker struct {
	job             f2e.ChunkJob
	maxEnvelopes    int
	maxMessageBytes int

	items    []f2e.Envelope[f2e.RecordPayload]
	itemSize int // running byte count of serialised items (without outer wrapper)
}

func newBundlePacker(job f2e.ChunkJob, cfg f2e.JobConfiguration) *bundlePacker {
	maxEnv := cfg.MaxEnvelopesPerMessage
	if maxEnv <= 0 {
		maxEnv = defaultMaxEnvelopesPerMessage
	}
	maxBytes := cfg.MaxMessageBytes
	if maxBytes <= 0 {
		maxBytes = cfg.MaxEventBytes
		if maxBytes <= 0 {
			maxBytes = 256 * 1024
		}
	}
	return &bundlePacker{
		job:             job,
		maxEnvelopes:    maxEnv,
		maxMessageBytes: maxBytes,
	}
}

// add attempts to add a serialised envelope to the current bundle.
// If the envelope alone exceeds maxMessageBytes it returns ErrEnvelopeTooLarge.
// If adding it would exceed the limits, flush is called first (with the
// current contents) and then the envelope is placed in the fresh bundle.
// Returns the OutboundMessage if a flush occurred, otherwise nil.
func (p *bundlePacker) add(env f2e.Envelope[f2e.RecordPayload], serialised []byte) (*port.OutboundMessage, error) {
	envelopeBytes := len(serialised)
	// A comma separator between items adds 1 byte for all items after the first.
	sizeWithComma := envelopeBytes
	if len(p.items) > 0 {
		sizeWithComma++
	}

	// A single envelope that cannot fit in an empty bundle is an explicit error.
	if bundleOverheadBytes+envelopeBytes > p.maxMessageBytes {
		return nil, f2e.ErrEnvelopeTooLarge{
			EventID:     env.Metadata.EventID,
			ActualBytes: bundleOverheadBytes + envelopeBytes,
			LimitBytes:  p.maxMessageBytes,
		}
	}

	// Flush if adding this envelope would exceed either limit.
	var flushed *port.OutboundMessage
	if len(p.items) > 0 && (len(p.items) >= p.maxEnvelopes || bundleOverheadBytes+p.itemSize+sizeWithComma > p.maxMessageBytes) {
		msg, err := p.flush()
		if err != nil {
			return nil, err
		}
		flushed = msg
		sizeWithComma = envelopeBytes // first item in new bundle — no comma
	}

	p.items = append(p.items, env)
	p.itemSize += sizeWithComma
	return flushed, nil
}

// flush serialises the current bundle into an OutboundMessage and resets the packer.
// Returns nil if there are no items buffered.
func (p *bundlePacker) flush() (*port.OutboundMessage, error) {
	if len(p.items) == 0 {
		return nil, nil
	}
	bundle := f2e.BundleEnvelope{
		SchemaVersion: f2e.BundleSchemaVersion,
		BundleID:      bundleID(p.items),
		JobID:         p.job.JobID,
		ChunkID:       p.job.ChunkID,
		Items:         p.items,
	}
	b, err := json.Marshal(bundle)
	if err != nil {
		return nil, fmt.Errorf("marshal bundle: %w", err)
	}
	// Final size check after serialisation (the estimate may differ slightly).
	if len(b) > p.maxMessageBytes {
		// This should be unreachable due to the pre-check above, but guard anyway.
		return nil, fmt.Errorf("bundle serialised to %d bytes, exceeds maxMessageBytes %d", len(b), p.maxMessageBytes)
	}
	msg := &port.OutboundMessage{
		Body: string(b),
		Attributes: map[string]port.MessageAttribute{
			"schema":        {DataType: "String", Value: f2e.BundleSchemaVersion},
			"f2e-bundle-id": {DataType: "String", Value: bundle.BundleID},
		},
	}
	p.items = nil
	p.itemSize = 0
	return msg, nil
}

// bundleID derives a stable bundle identity from the sorted eventIds of all items.
// Using sorted order makes the ID independent of insertion order, so a retry that
// re-produces the same logical set yields the same bundleId.
func bundleID(items []f2e.Envelope[f2e.RecordPayload]) string {
	ids := make([]string, len(items))
	for i, item := range items {
		ids[i] = item.Metadata.EventID
	}
	sort.Strings(ids)
	var concat string
	for _, id := range ids {
		concat += id
	}
	h := sha256.Sum256([]byte(concat))
	return hex.EncodeToString(h[:])
}
