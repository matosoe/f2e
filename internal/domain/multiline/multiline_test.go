package multiline_test

import (
	"context"
	"strings"
	"testing"

	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/domain/multiline"
)

func layout(breakFields, includeFields, ignoreFields []f2e.LineMatchField) f2e.MultiLineLayout {
	return f2e.MultiLineLayout{BreakFields: breakFields, IncludeFields: includeFields, IgnoreFields: ignoreFields, MaxBytesPerRecord: 1024}
}

func field(start, length int, value string) f2e.LineMatchField {
	return f2e.LineMatchField{StartByte: start, LengthBytes: length, Value: value}
}

func TestReadMultiLineLayoutMatchesMultipleBytePositionsAndPrecedence(t *testing.T) {
	input := "HDR\nD1-000\nC1-more\nD2-000\nC2-more\nT2-end\n"
	l := layout([]f2e.LineMatchField{field(0, 1, "D"), field(1, 1, "1")}, []f2e.LineMatchField{field(0, 1, "C"), field(1, 1, "1")}, []f2e.LineMatchField{field(0, 1, "T")})
	var got []string
	stats, err := multiline.ReadMultiLineLayoutWithStats(context.Background(), strings.NewReader(input), l, func(_, _ int64, raw string) error { got = append(got, raw); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "D1-000\x1CC1-more" {
		t.Fatalf("records=%q", got)
	}
	if stats.HeaderLinesIgnored != 1 || stats.TrailerLinesIgnored != 3 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestReadMultiLineLayoutShortLineUTF8AndTerminators(t *testing.T) {
	input := "H\r\nDáX\rCáY\nDáZ\rCáW\r\n"
	// The accented rune occupies bytes 1 and 2; matching byte 3 proves byte, not rune, indexing.
	l := layout([]f2e.LineMatchField{field(0, 1, "D"), field(3, 1, "X")}, []f2e.LineMatchField{field(0, 1, "C"), field(3, 1, "Y")}, nil)
	var got []string
	if err := multiline.ReadMultiLineLayout(context.Background(), strings.NewReader(input), l, func(_, _ int64, raw string) error { got = append(got, raw); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "DáX\x1CCáY" {
		t.Fatalf("records=%q", got)
	}
}

func TestMatchesRequiresEveryFieldAndRejectsShortLines(t *testing.T) {
	fields := []f2e.LineMatchField{field(0, 1, "D"), field(3, 2, "42")}
	if !multiline.Matches([]byte("Dxx42"), fields) {
		t.Fatal("all fields should match")
	}
	if multiline.Matches([]byte("Dxx4"), fields) || multiline.Matches([]byte("Dxx43"), fields) {
		t.Fatal("short or partial field must not match")
	}
}
