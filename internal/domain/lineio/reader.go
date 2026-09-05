// Package lineio provides a line reader for the F2E three-mode pipeline.
//
// Terminator contracts (section 4.1 of the evolution plan):
//   - CR (\r), LF (\n), and CRLF (\r\n) each terminate one logical record.
//   - CRLF is ONE terminator; LFCR is TWO.
//   - The terminator is NOT included in the raw content passed to the callback.
//   - An empty terminated line is a valid empty record.
//   - A trailing terminator does NOT produce an extra empty record.
//   - The last non-empty line without a terminator is an error.
//   - An empty reader produces no records; callers that must reject empty files
//     must check file size before calling ReadLines.
//   - Bytes invalid as UTF-8 produce a distinct, identifiable error.
//   - A line exceeding maxLineBytes (content only, NOT counting the terminator)
//     is an error; maxLineBytes must be >= 1.
//
// Physical sizes: maxLineBytes counts only the content bytes, never the terminator.
// A CRLF terminator occupies 2 physical bytes; CR or LF alone occupies 1.
package lineio

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"unicode/utf8"
)

// ReadLines reads logical line records from r and calls fn for each one.
//
// Arguments:
//   - maxLineBytes: maximum byte count of the record content (terminator excluded).
//     Must be >= 1.
//   - skipFirst: when true, the first record found is silently discarded. Used by
//     workers that read a leading overlap region before their assigned startByte;
//     the first record in that region is a trailing fragment of the previous chunk.
//
// Callback arguments:
//   - n: 0-based record index within this call (after skipFirst).
//   - offset: byte offset in r where the record content starts.
//   - raw: record content without the terminator (may be empty for an empty line).
func ReadLines(
	ctx context.Context,
	r io.Reader,
	maxLineBytes int64,
	skipFirst bool,
	fn func(n, offset int64, raw string) error,
) error {
	if maxLineBytes < 1 {
		return fmt.Errorf("lineio: maxLineBytes must be >= 1")
	}

	bufCap := maxLineBytes
	if bufCap > 64*1024 {
		bufCap = 64 * 1024
	}

	br := bufio.NewReaderSize(r, 32*1024)
	buf := make([]byte, 0, bufCap)

	var (
		physOffset int64 // bytes consumed so far (advances at each line boundary)
		lineStart  int64 // physOffset at the start of the current line
		published  int64 // records passed to fn
		seen       int64 // total records found (including any skipped)
	)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		b, err := br.ReadByte()
		if err == io.EOF {
			if len(buf) > 0 {
				return fmt.Errorf("lineio: no line terminator at end of input (line starting at byte offset %d)", lineStart)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("lineio: read error at byte offset %d: %w", physOffset+int64(len(buf)), err)
		}

		switch b {
		case '\r':
			terminatorLen := int64(1)
			next, peekErr := br.ReadByte()
			if peekErr == nil {
				if next == '\n' {
					terminatorLen = 2 // CRLF: one logical terminator, two physical bytes
				} else {
					_ = br.UnreadByte() // CR alone; put the non-LF byte back
				}
			} else if peekErr != io.EOF {
				return fmt.Errorf("lineio: read error at byte offset %d: %w", physOffset+int64(len(buf))+1, peekErr)
			}
			// peekErr == io.EOF: CR is the last byte — valid CR-terminated last line.
			if err2 := emitLine(buf, lineStart, skipFirst, &seen, &published, fn); err2 != nil {
				return err2
			}
			physOffset += int64(len(buf)) + terminatorLen
			lineStart = physOffset
			buf = buf[:0]

		case '\n':
			if err2 := emitLine(buf, lineStart, skipFirst, &seen, &published, fn); err2 != nil {
				return err2
			}
			physOffset += int64(len(buf)) + 1
			lineStart = physOffset
			buf = buf[:0]

		default:
			if int64(len(buf)) >= maxLineBytes {
				return fmt.Errorf("lineio: line starting at byte offset %d exceeds maximum %d bytes", lineStart, maxLineBytes)
			}
			buf = append(buf, b)
		}
	}
}

// emitLine validates content and dispatches one complete record to fn.
func emitLine(
	content []byte,
	lineStart int64,
	skipFirst bool,
	seen, published *int64,
	fn func(n, offset int64, raw string) error,
) error {
	if !utf8.Valid(content) {
		return fmt.Errorf("lineio: invalid UTF-8 in line starting at byte offset %d", lineStart)
	}
	if skipFirst && *seen == 0 {
		*seen++
		return nil
	}
	n := *published
	if err := fn(n, lineStart, string(content)); err != nil {
		return err
	}
	*published++
	*seen++
	return nil
}