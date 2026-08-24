// Package worker processes chunk jobs and publishes output events.
package worker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/domain/fixedwidth"
	"github.com/f2e/f2e/internal/platform/config"
	"io"
	"strings"
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
	// Jobs produced before the explicit dataType contract were fixed-width.
	if j.DataType == "" {
		j.DataType = f2e.DataTypeFixedWidth
	}
	if e := s.valid(j); e != nil {
		return e
	}
	readStart := j.StartByte
	if j.MaxRecordLengthBytes > 0 && readStart > 0 {
		readStart -= j.MaxRecordLengthBytes
		if readStart < 0 {
			readStart = 0
		}
	}
	r, e := s.Store.GetRange(ctx, j.Bucket, j.Key, readStart, j.EndByteInclusive)
	if e != nil {
		return e
	}
	defer r.Close()
	return s.stream(ctx, j, r, readStart)
}
func (s Service) valid(j f2e.ChunkJob) error {
	if j.SchemaVersion != f2e.SchemaVersion || j.Bucket == "" || j.Key == "" || j.EndByteInclusive < j.StartByte {
		return fmt.Errorf("invalid chunk job")
	}
	if j.DataType == f2e.DataTypeFixedWidth && (j.RecordCount < 1 || j.RecordLengthBytes != int64(s.Config.RecordLength) || j.EndByteInclusive-j.StartByte+1 != j.RecordCount*j.RecordLengthBytes) {
		return fmt.Errorf("invalid fixed-width chunk job")
	}
	if j.DataType != f2e.DataTypeFixedWidth && j.DataType != f2e.DataTypeJSONL && j.DataType != f2e.DataTypeNDJSON && j.DataType != f2e.DataTypeCSV && j.DataType != f2e.DataTypeBinary && j.DataType != f2e.DataTypeText {
		return fmt.Errorf("unsupported data type %q", j.DataType)
	}
	if j.Options.BypassJSONValidation && j.DataType != f2e.DataTypeJSONL && j.DataType != f2e.DataTypeNDJSON {
		return fmt.Errorf("invalid JSON validation option")
	}
	if j.MaxRecordLengthBytes > 0 && (j.DataType == f2e.DataTypeFixedWidth || j.DataType == f2e.DataTypeBinary || j.TrailingPaddingBytes < 0 || j.TrailingPaddingBytes > j.MaxRecordLengthBytes || j.EndByteInclusive-j.StartByte+1 < j.TrailingPaddingBytes) {
		return fmt.Errorf("invalid variable-record chunk job")
	}
	return nil
}
func (s Service) stream(ctx context.Context, j f2e.ChunkJob, r io.Reader, readStart int64) error {
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
	publish := func(n, off int64, raw string, fields []string, binary []byte) error {
		byteOffset := j.StartByte + off
		if j.MaxRecordLengthBytes > 0 {
			byteOffset = readStart + off
		}
		eventID := hash(j.FileID + "/" + j.ChunkID + "/" + fmt.Sprint(j.StartRecord+n+1))
		if j.MaxRecordLengthBytes > 0 {
			eventID = hash(j.FileID + "/" + fmt.Sprint(byteOffset))
		}
		e := f2e.OutputEvent{SchemaVersion: f2e.SchemaVersion, EventID: eventID, FileID: j.FileID, JobID: j.JobID, ChunkID: j.ChunkID, RecordNumber: j.StartRecord + n + 1, ByteOffset: byteOffset, DataType: j.DataType}
		e.Payload.Raw = raw
		e.Payload.Fields = fields
		if binary != nil {
			e.Payload.Base64 = base64.StdEncoding.EncodeToString(binary)
		}
		b, er := json.Marshal(e)
		if er != nil {
			return er
		}
		batch = append(batch, string(b))
		if len(batch) == 10 {
			return flush()
		}
		return nil
	}
	var err error
	switch j.DataType {
	case f2e.DataTypeFixedWidth:
		err = fixedwidth.Read(ctx, r, j.RecordLengthBytes, j.RecordCount, func(n, off int64, raw string) error { return publish(n, off, raw, nil, nil) })
	case f2e.DataTypeJSONL, f2e.DataTypeNDJSON, f2e.DataTypeText:
		err = readLines(ctx, r, j.MaxRecordLengthBytes > 0 && readStart > 0, func(n, off int64, raw string) error {
			if j.MaxRecordLengthBytes > 0 && (readStart+off < j.StartByte || readStart+off > j.EndByteInclusive-j.TrailingPaddingBytes) {
				return nil
			}
			if (j.DataType == f2e.DataTypeJSONL || j.DataType == f2e.DataTypeNDJSON) && !j.Options.BypassJSONValidation {
				var value any
				if e := json.Unmarshal([]byte(raw), &value); e != nil {
					return fmt.Errorf("invalid JSON at record %d: %w", n+1, e)
				}
			}
			return publish(n, off, raw, nil, nil)
		})
	case f2e.DataTypeCSV:
		err = readCSV(ctx, r, func(n, off int64, fields []string) error { return publish(n, off, "", fields, nil) })
	case f2e.DataTypeBinary:
		var body []byte
		body, err = io.ReadAll(r)
		if err == nil {
			err = publish(0, 0, "", nil, body)
		}
	}
	if err != nil {
		return err
	}
	if len(batch) > 0 {
		return flush()
	}
	return nil
}

func readLines(ctx context.Context, r io.Reader, discardFirst bool, fn func(int64, int64, string) error) error {
	br := bufio.NewReader(r)
	var offset, number int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, err := br.ReadString('\n')
		if len(line) > 0 {
			raw := strings.TrimSuffix(line, "\n")
			raw = strings.TrimSuffix(raw, "\r")
			if !discardFirst || number > 0 {
				if e := fn(number, offset, raw); e != nil {
					return e
				}
			}
			number++
			offset += int64(len(line))
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func readCSV(ctx context.Context, r io.Reader, fn func(int64, int64, []string) error) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	cr := csv.NewReader(bytes.NewReader(data))
	cr.FieldsPerRecord = -1
	for n := int64(0); ; n++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		fields, err := cr.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("invalid CSV at record %d: %w", n+1, err)
		}
		line, _ := cr.FieldPos(0)
		off := int64(0)
		for i := 0; i < line-1; i++ {
			p := bytes.IndexByte(data[off:], '\n')
			if p < 0 {
				break
			}
			off += int64(p + 1)
		}
		if err := fn(n, off, fields); err != nil {
			return err
		}
	}
}
