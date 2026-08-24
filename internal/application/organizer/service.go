// Package organizer plans and schedules fixed-width file chunks.
package organizer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/platform/config"
)

type Service struct {
	Store  port.ObjectStore
	Queue  port.Queue
	Config config.Config
}

func hash(s string) string { x := sha256.Sum256([]byte(s)); return hex.EncodeToString(x[:]) }
func (s Service) Jobs(ctx context.Context, references []f2e.FileReference) ([]f2e.ChunkJob, error) {
	var jobs []f2e.ChunkJob
	for _, reference := range references {
		size, etag, e := s.Store.Head(ctx, reference.Bucket, reference.Key)
		if e != nil {
			return nil, fmt.Errorf("head %s/%s: %w", reference.Bucket, reference.Key, e)
		}
		if size == 0 || size%s.safeLen() != 0 {
			return nil, fmt.Errorf("invalid fixed-width object size %d", size)
		}
		fileID := hash(reference.Bucket + "/" + reference.Key + "/" + etag)
		total := size / s.safeLen()
		for start, chunk := int64(0), int64(1); start < total; start, chunk = start+int64(s.Config.RecordsPerChunk), chunk+1 {
			count := int64(s.Config.RecordsPerChunk)
			if total-start < count {
				count = total - start
			}
			startByte := start * s.safeLen()
			jobs = append(jobs, f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, JobID: hash(fileID), FileID: fileID, ChunkID: fmt.Sprintf("%08d", chunk), Bucket: reference.Bucket, Key: reference.Key, ETag: etag, StartRecord: start, RecordCount: count, RecordLengthBytes: s.safeLen(), StartByte: startByte, EndByteInclusive: startByte + count*s.safeLen() - 1})
		}
	}
	return jobs, nil
}
func (s Service) safeLen() int64 { return int64(s.Config.RecordLength) }
func (s Service) Publish(ctx context.Context, jobs []f2e.ChunkJob) error {
	bodies := make([]string, 0, 10)
	flush := func() error {
		f, e := s.Queue.Send(ctx, s.Config.ChunkQueueURL, bodies)
		if e != nil {
			return e
		}
		if len(f) > 0 {
			return fmt.Errorf("chunk batch partial failure: %v", f)
		}
		bodies = nil
		return nil
	}
	for _, j := range jobs {
		b, e := json.Marshal(j)
		if e != nil {
			return e
		}
		bodies = append(bodies, string(b))
		if len(bodies) == 10 {
			if e := flush(); e != nil {
				return e
			}
		}
	}
	if len(bodies) > 0 {
		return flush()
	}
	return nil
}
