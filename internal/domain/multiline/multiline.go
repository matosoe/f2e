package multiline

import (
	"context"
	"io"
	"strings"

	"github.com/matosoe/f2e/internal/domain/f2e"
	"github.com/matosoe/f2e/internal/domain/lineio"
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

func ReadMultiLineLayout(ctx context.Context, r io.Reader, layout f2e.MultiLineLayout, fn func(number, offset int64, raw string) error) error {
	_, err := ReadMultiLineLayoutWithStats(ctx, r, layout, fn)
	return err
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
		case Matches(bytes, layout.IgnoreFields):
			if len(recLines) > 0 {
				stats.TrailerLinesIgnored++
			} else {
				stats.HeaderLinesIgnored++
			}
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
