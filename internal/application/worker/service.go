// Package worker processes chunk jobs and publishes output events.
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/domain/fixedwidth"
	"github.com/f2e/f2e/internal/platform/config"
	"io"
)

type Service struct {
	Store  port.ObjectStore
	Queue  port.Queue
	Config config.Config
}

func hash(s string) string { x := sha256.Sum256([]byte(s)); return hex.EncodeToString(x[:]) }
func (s Service) Process(ctx context.Context, body []byte) error {
	var j f2e.ChunkJob
	if e := json.Unmarshal(body, &j); e != nil {
		return fmt.Errorf("decode job: %w", e)
	}
	if e := s.valid(j); e != nil {
		return e
	}
	r, e := s.Store.GetRange(ctx, j.Bucket, j.Key, j.StartByte, j.EndByteInclusive)
	if e != nil {
		return e
	}
	defer r.Close()
	return s.stream(ctx, j, r)
}
func (s Service) valid(j f2e.ChunkJob) error {
	if j.SchemaVersion != f2e.SchemaVersion || j.RecordCount < 1 || j.RecordLengthBytes != int64(s.Config.RecordLength) || j.EndByteInclusive-j.StartByte+1 != j.RecordCount*j.RecordLengthBytes {
		return fmt.Errorf("invalid chunk job")
	}
	return nil
}
func (s Service) stream(ctx context.Context, j f2e.ChunkJob, r io.Reader) error {
	batch := make([]string, 0, 10)
	flush := func() error {
		failed, e := s.Queue.Send(ctx, s.Config.OutputQueueURL, batch)
		if e != nil {
			return e
		}
		if len(failed) > 0 {
			return fmt.Errorf("output batch partial failure: %v", failed)
		}
		batch = nil
		return nil
	}
	err := fixedwidth.Read(ctx, r, j.RecordLengthBytes, j.RecordCount, func(n, off int64, raw string) error {
		e := f2e.OutputEvent{SchemaVersion: f2e.SchemaVersion, EventID: hash(j.FileID + "/" + j.ChunkID + "/" + fmt.Sprint(j.StartRecord+n+1)), FileID: j.FileID, JobID: j.JobID, ChunkID: j.ChunkID, RecordNumber: j.StartRecord + n + 1, ByteOffset: j.StartByte + off}
		e.Payload.Raw = raw
		b, er := json.Marshal(e)
		if er != nil {
			return er
		}
		batch = append(batch, string(b))
		if len(batch) == 10 {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(batch) > 0 {
		return flush()
	}
	return nil
}
