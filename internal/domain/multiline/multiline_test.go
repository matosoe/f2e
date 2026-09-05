package multiline_test

import (
	"context"
	"strings"
	"testing"

	"github.com/f2e/f2e/internal/domain/multiline"
)

// TestReadMultiLineBytePosBreak exercises Example 1: break on first byte value,
// with two accepted sub-line prefixes, also at position 0.
func TestReadMultiLineBytePosBreak(t *testing.T) {
	input := "1abc123\n2zzzaaa\n3999888\n3777666\n1def456\n2xxxbbb\n3555444\n3333222\n"
	var got []string
	err := multiline.ReadMultiLine(context.Background(), strings.NewReader(input), 0, "1", []string{"2", "3"}, "\x1C", 1024, func(_, _ int64, raw string) error {
		got = append(got, raw)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 records, got %d: %v", len(got), got)
	}
	if got[0] != "1abc123\x1C2zzzaaa\x1C3999888\x1C3777666" {
		t.Fatalf("record 0: %q", got[0])
	}
	if got[1] != "1def456\x1C2xxxbbb\x1C3555444\x1C3333222" {
		t.Fatalf("record 1: %q", got[1])
	}
}

// TestReadMultiLinePrefixBreak exercises Example 2: break on a full prefix string,
// with distinct accepted prefixes and interleaved header/trailer lines to skip.
func TestReadMultiLinePrefixBreak(t *testing.T) {
	input := "[header_geral]balblabla\n" +
		"[header_intermediario]blablablabla\n" +
		"[titulo]id=1\n[juros]valor=5\n[desconto]id=a;valor=1\n[desconto]id=b;valor2\n[mensagem]a\n[mensagem]b\n[mensagem]c\n" +
		"[titulo]id=2\n[juros]valor=5\n[mensagem]a\n[mensagem]b\n[mensagem]c\n" +
		"[header_intermediario]blablablabla2\n" +
		"[titulo]id=3\n[mensagem]b\n[mensagem]c\n" +
		"[trailer]123\n"
	var got []string
	err := multiline.ReadMultiLine(context.Background(), strings.NewReader(input), 0, "[titulo]", []string{"[juros]", "[desconto]", "[mensagem]"}, "\x1C", 1024, func(_, _ int64, raw string) error {
		got = append(got, raw)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 records, got %d: %v", len(got), got)
	}
	r1 := strings.Split(got[0], "\x1C")
	if r1[0] != "[titulo]id=1" || len(r1) != 7 {
		t.Fatalf("record 1 parts=%d: %q", len(r1), got[0])
	}
	r2 := strings.Split(got[1], "\x1C")
	if r2[0] != "[titulo]id=2" || len(r2) != 5 {
		t.Fatalf("record 2 parts=%d: %q", len(r2), got[1])
	}
	r3 := strings.Split(got[2], "\x1C")
	if r3[0] != "[titulo]id=3" || len(r3) != 3 {
		t.Fatalf("record 3 parts=%d: %q", len(r3), got[2])
	}
}

// TestReadMultiLineReportsBreakLineOffset verifies that fn receives the byte
// offset of each record's break line within the stream.
func TestReadMultiLineReportsBreakLineOffset(t *testing.T) {
	input := "1abc\n2def\n1xyz\n2uvw\n"
	var offsets []int64
	if err := multiline.ReadMultiLine(context.Background(), strings.NewReader(input), 0, "1", []string{"2"}, "", 1024, func(_, off int64, _ string) error {
		offsets = append(offsets, off)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 2 || offsets[0] != 0 || offsets[1] != 10 {
		t.Fatalf("offsets=%v", offsets)
	}
}

// TestReadMultiLineNoAcceptedPrefixesIncludesAll verifies that when AcceptedPrefixes
// is empty every line after the break line is included in the record.
func TestReadMultiLineNoAcceptedPrefixesIncludesAll(t *testing.T) {
	input := "BREAK\nany1\nany2\nBREAK\nany3\n"
	var got []string
	if err := multiline.ReadMultiLine(context.Background(), strings.NewReader(input), 0, "BREAK", nil, "\x1C", 1024, func(_, _ int64, raw string) error {
		got = append(got, raw)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "BREAK\x1Cany1\x1Cany2" || got[1] != "BREAK\x1Cany3" {
		t.Fatalf("got=%v", got)
	}
}

// TestReadMultiLineHeaderAndTrailerSkipped verifies that lines before the first
// break and after the last accepted line are silently ignored.
func TestReadMultiLineHeaderAndTrailerSkipped(t *testing.T) {
	input := "HEADER\n[R]line1\n[A]line2\nTRAILER\n"
	var got []string
	if err := multiline.ReadMultiLine(context.Background(), strings.NewReader(input), 0, "[R]", []string{"[A]"}, "\x1C", 1024, func(_, _ int64, raw string) error {
		got = append(got, raw)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "[R]line1\x1C[A]line2" {
		t.Fatalf("got=%v", got)
	}
}

func TestReadMultiLineSupportsCRLFAndCR(t *testing.T) {
	input := "1abc\r\n2def\r1xyz\n2uvw\r\n"
	var got []string
	if err := multiline.ReadMultiLine(context.Background(), strings.NewReader(input), 0, "1", []string{"2"}, "", 1024, func(_, _ int64, raw string) error {
		got = append(got, raw)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "1abc\x1C2def" || got[1] != "1xyz\x1C2uvw" {
		t.Fatalf("got=%v", got)
	}
}
