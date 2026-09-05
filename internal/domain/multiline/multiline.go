package multiline

import (
	"context"
	"io"
	"strings"

	"github.com/f2e/f2e/internal/domain/lineio"
)

// DefaultLineSeparator is the ASCII File Separator (0x1C) used to join record lines by default.
const DefaultLineSeparator = "\x1C"

// ReadMultiLine reads physical lines from r using CR/LF/CRLF terminators and groups
// them into logical multi-line records. A new record begins when a line at breakPosition
// has breakMarker as a prefix. Lines matching acceptedPrefixes are appended to the current
// record; lines matching neither are silently skipped. maxLineBytes is the per-physical-line
// limit (content without terminator); use MaxBytesPerRecord or a large default if unknown.
func ReadMultiLine(ctx context.Context, r io.Reader, breakPosition int, breakMarker string, acceptedPrefixes []string, lineSeparator string, maxLineBytes int64, fn func(number, offset int64, raw string) error) error {
	if lineSeparator == "" {
		lineSeparator = DefaultLineSeparator
	}

	var (
		recordNum int64
		recLines  []string
		recStart  int64
	)

	emit := func() error {
		if len(recLines) == 0 {
			return nil
		}
		raw := strings.Join(recLines, lineSeparator)
		off := recStart
		recLines = nil
		if err := fn(recordNum, off, raw); err != nil {
			return err
		}
		recordNum++
		return nil
	}

	if err := lineio.ReadLines(ctx, r, maxLineBytes, false, func(_n, off int64, line string) error {
		sub := ""
		if breakPosition >= 0 && breakPosition < len(line) {
			sub = line[breakPosition:]
		}
		switch {
		case strings.HasPrefix(sub, breakMarker):
			if err := emit(); err != nil {
				return err
			}
			recStart = off
			recLines = append(recLines, line)
		case len(recLines) > 0:
			if len(acceptedPrefixes) == 0 {
				recLines = append(recLines, line)
				return nil
			}
			for _, ap := range acceptedPrefixes {
				if strings.HasPrefix(sub, ap) {
					recLines = append(recLines, line)
					break
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	return emit()
}
