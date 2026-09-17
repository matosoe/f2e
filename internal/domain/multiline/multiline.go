package multiline

import (
	"context"
	"io"
	"strings"

	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/domain/lineio"
)

const DefaultLineSeparator = "\x1C"

type Stats struct{ HeaderLinesIgnored, TrailerLinesIgnored int64 }

// Matches reports whether every fixed-position byte field matches line.
// A short line is simply not a match.
func Matches(line []byte, fields []f2e.LineMatchField) bool {
	if len(fields) == 0 {
		return false
	}
	for _, f := range fields {
		if f.StartByte < 0 || f.LengthBytes < 1 || len(f.Value) != f.LengthBytes || f.StartByte+f.LengthBytes > len(line) || string(line[f.StartByte:f.StartByte+f.LengthBytes]) != f.Value {
			return false
		}
	}
	return true
}

// ReadMultiLine is retained as a source-compatible adapter for callers during
// migration; new contracts use ReadMultiLineLayout.
func ReadMultiLine(ctx context.Context, r io.Reader, breakPosition int, breakMarker string, acceptedPrefixes []string, lineSeparator string, maxLineBytes int64, fn func(number, offset int64, raw string) error) error {
	_, err := ReadMultiLineWithStats(ctx, r, breakPosition, breakMarker, acceptedPrefixes, lineSeparator, maxLineBytes, fn)
	return err
}

func ReadMultiLineLayout(ctx context.Context, r io.Reader, layout f2e.MultiLineLayout, fn func(number, offset int64, raw string) error) error {
	_, err := ReadMultiLineLayoutWithStats(ctx, r, layout, fn)
	return err
}

func ReadMultiLineWithStats(ctx context.Context, r io.Reader, breakPosition int, breakMarker string, acceptedPrefixes []string, lineSeparator string, maxLineBytes int64, fn func(number, offset int64, raw string) error) (Stats, error) {
	if lineSeparator == "" {
		lineSeparator = DefaultLineSeparator
	}
	var stats Stats
	var n, start int64
	var lines []string
	emit := func() error {
		if len(lines) == 0 {
			return nil
		}
		raw := strings.Join(lines, lineSeparator)
		lines = nil
		if err := fn(n, start, raw); err != nil {
			return err
		}
		n++
		return nil
	}
	err := lineio.ReadLines(ctx, r, maxLineBytes, false, func(_ int64, off int64, line string) error {
		sub := ""
		if breakPosition >= 0 && breakPosition < len(line) {
			sub = line[breakPosition:]
		}
		if strings.HasPrefix(sub, breakMarker) {
			if err := emit(); err != nil {
				return err
			}
			start = off
			lines = []string{line}
			return nil
		}
		if len(lines) == 0 {
			stats.HeaderLinesIgnored++
			return nil
		}
		if len(acceptedPrefixes) == 0 {
			lines = append(lines, line)
			return nil
		}
		for _, p := range acceptedPrefixes {
			if strings.HasPrefix(sub, p) {
				lines = append(lines, line)
				return nil
			}
		}
		stats.TrailerLinesIgnored++
		return nil
	})
	if err != nil {
		return stats, err
	}
	return stats, emit()
}

func ReadMultiLineLayoutWithStats(ctx context.Context, r io.Reader, layout f2e.MultiLineLayout, fn func(number, offset int64, raw string) error) (Stats, error) {
	var stats Stats
	separator := layout.LineSeparator
	if separator == "" {
		separator = DefaultLineSeparator
	}
	var recordNum, recStart int64
	var recLines []string
	emit := func() error {
		if len(recLines) == 0 {
			return nil
		}
		raw, off := strings.Join(recLines, separator), recStart
		recLines = nil
		if err := fn(recordNum, off, raw); err != nil {
			return err
		}
		recordNum++
		return nil
	}
	err := lineio.ReadLines(ctx, r, layout.MaxBytesPerRecord, false, func(_ int64, off int64, line string) error {
		bytes := []byte(line)
		switch {
		case Matches(bytes, layout.BreakFields):
			if err := emit(); err != nil {
				return err
			}
			recStart = off
			recLines = append(recLines, line)
		case len(recLines) > 0 && Matches(bytes, layout.IncludeFields):
			recLines = append(recLines, line)
		case len(recLines) > 0:
			stats.TrailerLinesIgnored++
		default:
			stats.HeaderLinesIgnored++
		}
		return nil
	})
	if err != nil {
		return stats, err
	}
	return stats, emit()
}
