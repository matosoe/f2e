package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type manifest struct {
	Records           int64  `json:"records"`
	RecordLengthBytes int    `json:"recordLengthBytes"`
	SizeBytes         int64  `json:"sizeBytes"`
	SHA256            string `json:"sha256"`
}

func main() {
	output := flag.String("output", "", "destination file")
	records := flag.Int64("records", 5_000_000, "number of records")
	recordLength := flag.Int("record-length", 100, "bytes per record, including LF")
	force := flag.Bool("force", false, "replace an existing destination")
	flag.Parse()

	if *output == "" {
		fatal(errors.New("--output is required"))
	}
	if err := generate(*output, *records, *recordLength, *force); err != nil {
		fatal(err)
	}
}

func generate(output string, records int64, recordLength int, force bool) error {
	if records < 1 || records > 99_999_999 {
		return fmt.Errorf("records must be between 1 and 99999999")
	}
	if recordLength < 10 {
		return fmt.Errorf("record-length must be at least 10 bytes (8-digit id plus payload and LF)")
	}
	if _, err := os.Stat(output); err == nil && !force {
		return fmt.Errorf("destination already exists: %s (use --force)", output)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(output), ".fixed-records-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	hash := sha256.New()
	counting := &countingWriter{writer: io.MultiWriter(tmp, hash)}
	buffer := bufio.NewWriterSize(counting, 4*1024*1024)
	payload := strings.Repeat("X", recordLength-9)
	for i := int64(1); i <= records; i++ {
		if _, err = fmt.Fprintf(buffer, "%08d%s\n", i, payload); err != nil {
			_ = tmp.Close()
			return err
		}
	}
	if err = buffer.Flush(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}

	wantSize := records * int64(recordLength)
	if counting.bytes != wantSize {
		return fmt.Errorf("generated size is %d bytes, want %d", counting.bytes, wantSize)
	}
	if force {
		if err = os.Remove(output); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err = os.Rename(tmpName, output); err != nil {
		return err
	}

	m := manifest{Records: records, RecordLengthBytes: recordLength, SizeBytes: counting.bytes, SHA256: hex.EncodeToString(hash.Sum(nil))}
	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	if err = os.WriteFile(output+".manifest.json", body, 0o644); err != nil {
		return err
	}
	fmt.Printf("Generated %s: %s records, %s bytes, sha256=%s\n", output, strconv.FormatInt(records, 10), strconv.FormatInt(counting.bytes, 10), m.SHA256)
	return nil
}

type countingWriter struct {
	writer io.Writer
	bytes  int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	w.bytes += int64(n)
	return n, err
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
