package lineio_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/f2e/f2e/internal/domain/lineio"
)

// collect gathers all records returned by ReadLines into a slice of [offset, raw] pairs.
type record struct {
	n      int64
	offset int64
	raw    string
}

func collect(t *testing.T, input []byte, maxBytes int64, skipFirst bool) ([]record, error) {
	t.Helper()
	var records []record
	err := lineio.ReadLines(context.Background(), bytes.NewReader(input), maxBytes, skipFirst, func(n, offset int64, raw string) error {
		records = append(records, record{n, offset, raw})
		return nil
	})
	return records, err
}

// mustCollect is like collect but fails the test on error.
func mustCollect(t *testing.T, input []byte, maxBytes int64, skipFirst bool) []record {
	t.Helper()
	recs, err := collect(t, input, maxBytes, skipFirst)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return recs
}

// ─────────────────────────────────────────────
// Basic terminators
// ─────────────────────────────────────────────

func TestLFTerminator(t *testing.T) {
	recs := mustCollect(t, []byte("hello\nworld\n"), 256, false)
	if len(recs) != 2 {
		t.Fatalf("want 2 records, got %d", len(recs))
	}
	if recs[0].raw != "hello" || recs[0].offset != 0 {
		t.Errorf("record 0: got raw=%q offset=%d", recs[0].raw, recs[0].offset)
	}
	if recs[1].raw != "world" || recs[1].offset != 6 {
		t.Errorf("record 1: got raw=%q offset=%d", recs[1].raw, recs[1].offset)
	}
}

func TestCRTerminator(t *testing.T) {
	recs := mustCollect(t, []byte("hello\rworld\r"), 256, false)
	if len(recs) != 2 {
		t.Fatalf("want 2 records, got %d", len(recs))
	}
	if recs[0].raw != "hello" || recs[0].offset != 0 {
		t.Errorf("record 0: got raw=%q offset=%d", recs[0].raw, recs[0].offset)
	}
	if recs[1].raw != "world" || recs[1].offset != 6 {
		t.Errorf("record 1: got raw=%q offset=%d", recs[1].raw, recs[1].offset)
	}
}

func TestCRLFTerminator(t *testing.T) {
	recs := mustCollect(t, []byte("hello\r\nworld\r\n"), 256, false)
	if len(recs) != 2 {
		t.Fatalf("want 2 records, got %d", len(recs))
	}
	if recs[0].raw != "hello" || recs[0].offset != 0 {
		t.Errorf("record 0: got raw=%q offset=%d", recs[0].raw, recs[0].offset)
	}
	// "world" starts after "hello\r\n" (7 bytes)
	if recs[1].raw != "world" || recs[1].offset != 7 {
		t.Errorf("record 1: got raw=%q offset=%d", recs[1].raw, recs[1].offset)
	}
}

// LFCR is two terminators: LF terminates the first record, CR terminates
// an empty second record, leaving a third record starting after CR.
func TestLFCRAreTwoTerminators(t *testing.T) {
	// "a\n\rb\n" → records: "a", "", "b"
	recs := mustCollect(t, []byte("a\n\rb\n"), 256, false)
	if len(recs) != 3 {
		t.Fatalf("want 3 records (a, empty, b), got %d: %+v", len(recs), recs)
	}
	if recs[0].raw != "a" {
		t.Errorf("record 0: got %q", recs[0].raw)
	}
	if recs[1].raw != "" {
		t.Errorf("record 1 (empty): got %q", recs[1].raw)
	}
	if recs[2].raw != "b" {
		t.Errorf("record 2: got %q", recs[2].raw)
	}
}

// ─────────────────────────────────────────────
// Mixed terminators
// ─────────────────────────────────────────────

func TestMixedTerminators(t *testing.T) {
	// CR, LF, CRLF each produce one record.
	recs := mustCollect(t, []byte("a\rb\nc\r\n"), 256, false)
	if len(recs) != 3 {
		t.Fatalf("want 3 records, got %d", len(recs))
	}
	want := []string{"a", "b", "c"}
	for i, w := range want {
		if recs[i].raw != w {
			t.Errorf("record %d: got %q, want %q", i, recs[i].raw, w)
		}
	}
}

// ─────────────────────────────────────────────
// Trailing terminator does NOT produce extra record
// ─────────────────────────────────────────────

func TestTrailingTerminatorNoExtraRecord(t *testing.T) {
	for _, input := range []string{"line\n", "line\r", "line\r\n"} {
		recs := mustCollect(t, []byte(input), 256, false)
		if len(recs) != 1 {
			t.Errorf("input %q: want 1 record, got %d", input, len(recs))
		}
	}
}

// ─────────────────────────────────────────────
// Empty line is a valid record
// ─────────────────────────────────────────────

func TestEmptyLineIsValidRecord(t *testing.T) {
	// "\n" alone is one empty record.
	recs := mustCollect(t, []byte("\n"), 256, false)
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	if recs[0].raw != "" {
		t.Errorf("expected empty raw, got %q", recs[0].raw)
	}
}

func TestMultipleEmptyLines(t *testing.T) {
	// "a\n\n\nb\n" → 4 records: "a", "", "", "b"
	recs := mustCollect(t, []byte("a\n\n\nb\n"), 256, false)
	if len(recs) != 4 {
		t.Fatalf("want 4 records, got %d: %+v", len(recs), recs)
	}
}

// ─────────────────────────────────────────────
// Empty reader produces no records
// ─────────────────────────────────────────────

func TestEmptyReaderNoRecords(t *testing.T) {
	recs := mustCollect(t, []byte{}, 256, false)
	if len(recs) != 0 {
		t.Fatalf("want 0 records for empty reader, got %d", len(recs))
	}
}

// ─────────────────────────────────────────────
// Trailing content without terminator is an error
// ─────────────────────────────────────────────

func TestLastLineWithoutTerminatorIsError(t *testing.T) {
	_, err := collect(t, []byte("hello"), 256, false)
	if err == nil {
		t.Fatal("expected error for last line without terminator, got nil")
	}
}

func TestLastLineWithoutTerminatorAfterValidLines(t *testing.T) {
	_, err := collect(t, []byte("ok\nno-terminator"), 256, false)
	if err == nil {
		t.Fatal("expected error for last line without terminator")
	}
}

// ─────────────────────────────────────────────
// Trailing spaces and content are preserved
// ─────────────────────────────────────────────

func TestTrailingSpacesPreserved(t *testing.T) {
	recs := mustCollect(t, []byte("hello   \n"), 256, false)
	if recs[0].raw != "hello   " {
		t.Errorf("trailing spaces not preserved: got %q", recs[0].raw)
	}
}

// ─────────────────────────────────────────────
// UTF-8 multibyte content
// ─────────────────────────────────────────────

func TestUTF8MultibyteLine(t *testing.T) {
	// "ação" is 6 bytes in UTF-8 (a=1, ç=2, ã=2, o=1)
	recs := mustCollect(t, []byte("ação\n"), 256, false)
	if len(recs) != 1 || recs[0].raw != "ação" {
		t.Errorf("got %+v", recs)
	}
}

func TestUTF8MultibyteBoundary(t *testing.T) {
	// maxLineBytes counts content bytes, not runes.
	// "ção" is 5 bytes; maxLineBytes=5 must accept it.
	recs := mustCollect(t, []byte("ção\n"), 5, false)
	if len(recs) != 1 || recs[0].raw != "ção" {
		t.Errorf("got %+v", recs)
	}
}

// ─────────────────────────────────────────────
// Invalid UTF-8 is an identifiable error
// ─────────────────────────────────────────────

func TestInvalidUTF8IsError(t *testing.T) {
	// 0xFF is not valid UTF-8.
	_, err := collect(t, []byte{0xFF, '\n'}, 256, false)
	if err == nil {
		t.Fatal("expected error for invalid UTF-8")
	}
	if !strings.Contains(err.Error(), "UTF-8") && !strings.Contains(err.Error(), "utf-8") {
		t.Errorf("error should mention UTF-8: %v", err)
	}
}

func TestInvalidUTF8MidLine(t *testing.T) {
	bad := append([]byte("hello"), 0xC0, '\n') // 0xC0 alone is not valid UTF-8
	_, err := collect(t, bad, 256, false)
	if err == nil {
		t.Fatal("expected error for invalid UTF-8 mid-line")
	}
}

// ─────────────────────────────────────────────
// maxLineBytes limit
// ─────────────────────────────────────────────

func TestExactMaxLineBytesAccepted(t *testing.T) {
	line := strings.Repeat("x", 10) + "\n"
	recs := mustCollect(t, []byte(line), 10, false)
	if len(recs) != 1 || recs[0].raw != strings.Repeat("x", 10) {
		t.Errorf("exact limit should be accepted: %+v", recs)
	}
}

func TestOneBeyondMaxLineBytesRejected(t *testing.T) {
	line := strings.Repeat("x", 11) + "\n"
	_, err := collect(t, []byte(line), 10, false)
	if err == nil {
		t.Fatal("expected error when line exceeds maxLineBytes")
	}
}

// ─────────────────────────────────────────────
// skipFirst
// ─────────────────────────────────────────────

func TestSkipFirstDiscardsPart(t *testing.T) {
	// "fragment\nfull\n": with skipFirst the first record ("fragment") is discarded.
	recs := mustCollect(t, []byte("fragment\nfull\n"), 256, true)
	if len(recs) != 1 || recs[0].raw != "full" {
		t.Errorf("skipFirst: got %+v", recs)
	}
	if recs[0].n != 0 {
		t.Errorf("first published record should have n=0, got %d", recs[0].n)
	}
}

func TestSkipFirstWithSingleLine(t *testing.T) {
	// Only one line: skipFirst discards it; no records emitted.
	recs := mustCollect(t, []byte("only\n"), 256, true)
	if len(recs) != 0 {
		t.Errorf("skipFirst with single line: want 0 records, got %d", len(recs))
	}
}

func TestSkipFirstEmptyReader(t *testing.T) {
	// Empty reader with skipFirst: still zero records, no error.
	recs := mustCollect(t, []byte{}, 256, true)
	if len(recs) != 0 {
		t.Fatalf("want 0 records, got %d", len(recs))
	}
}

// ─────────────────────────────────────────────
// Byte offsets
// ─────────────────────────────────────────────

func TestByteOffsetsLF(t *testing.T) {
	// "aa\nbbb\nc\n": offsets 0, 3, 7
	recs := mustCollect(t, []byte("aa\nbbb\nc\n"), 256, false)
	wantOffsets := []int64{0, 3, 7}
	for i, want := range wantOffsets {
		if recs[i].offset != want {
			t.Errorf("record %d: want offset %d, got %d", i, want, recs[i].offset)
		}
	}
}

func TestByteOffsetsCRLF(t *testing.T) {
	// "aa\r\nbbb\r\n": offsets 0 (start), 4 (after "aa\r\n")
	recs := mustCollect(t, []byte("aa\r\nbbb\r\n"), 256, false)
	if recs[0].offset != 0 {
		t.Errorf("record 0 offset: want 0, got %d", recs[0].offset)
	}
	if recs[1].offset != 4 { // "aa\r\n" = 4 bytes
		t.Errorf("record 1 offset: want 4, got %d", recs[1].offset)
	}
}

func TestByteOffsetsMixed(t *testing.T) {
	// "a\rb\nc\r\n" → offsets: a@0, b@2, c@4
	recs := mustCollect(t, []byte("a\rb\nc\r\n"), 256, false)
	want := []int64{0, 2, 4}
	for i, w := range want {
		if recs[i].offset != w {
			t.Errorf("record %d: want offset %d, got %d", i, w, recs[i].offset)
		}
	}
}

// ─────────────────────────────────────────────
// Record numbering
// ─────────────────────────────────────────────

func TestRecordNumbers(t *testing.T) {
	recs := mustCollect(t, []byte("a\nb\nc\n"), 256, false)
	for i, r := range recs {
		if r.n != int64(i) {
			t.Errorf("record %d: got n=%d", i, r.n)
		}
	}
}

func TestRecordNumbersWithSkipFirst(t *testing.T) {
	// skipFirst discards first; published records should be numbered 0, 1, 2.
	recs := mustCollect(t, []byte("skip\na\nb\nc\n"), 256, true)
	for i, r := range recs {
		if r.n != int64(i) {
			t.Errorf("published record %d: got n=%d", i, r.n)
		}
	}
}

// ─────────────────────────────────────────────
// CRLF split across slow-read boundaries
// ─────────────────────────────────────────────

// slowReader returns data one byte at a time to exercise buffer refill paths.
type slowReader struct{ r io.Reader }

func (s slowReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return s.r.Read(p[:1])
}

func TestCRLFSlowReader(t *testing.T) {
	input := []byte("hello\r\nworld\r\n")
	var records []record
	err := lineio.ReadLines(context.Background(), slowReader{bytes.NewReader(input)}, 256, false, func(n, offset int64, raw string) error {
		records = append(records, record{n, offset, raw})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("want 2 records, got %d", len(records))
	}
	if records[0].raw != "hello" || records[1].raw != "world" {
		t.Errorf("got %+v", records)
	}
	if records[1].offset != 7 { // "hello\r\n" = 7 bytes
		t.Errorf("record 1 offset: want 7, got %d", records[1].offset)
	}
}

// ─────────────────────────────────────────────
// Context cancellation
// ─────────────────────────────────────────────

func TestContextCancelledBeforeFirstLine(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := lineio.ReadLines(ctx, strings.NewReader("line\n"), 256, false, func(_, _ int64, _ string) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Errorf("want context.Canceled, got %v", err)
	}
}

// ─────────────────────────────────────────────
// Callback errors are propagated
// ─────────────────────────────────────────────

func TestCallbackErrorIsPropagated(t *testing.T) {
	sentinel := errors.New("sentinel")
	err := lineio.ReadLines(context.Background(), strings.NewReader("a\nb\n"), 256, false, func(_, _ int64, _ string) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("want sentinel error, got %v", err)
	}
}

// ─────────────────────────────────────────────
// Invalid maxLineBytes
// ─────────────────────────────────────────────

func TestZeroMaxLineBytesIsError(t *testing.T) {
	err := lineio.ReadLines(context.Background(), strings.NewReader("a\n"), 0, false, func(_, _ int64, _ string) error { return nil })
	if err == nil {
		t.Fatal("expected error for maxLineBytes=0")
	}
}
