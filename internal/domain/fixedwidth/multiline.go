package fixedwidth

import (
	"bufio"
	"context"
	"io"
	"strings"
)

// DefaultLineSeparator is the ASCII File Separator (0x1C) used to join record lines by default.
const DefaultLineSeparator = "\x1C"

// ReadMultiLine reads lines from r and groups them into logical multi-line records.
//
// A new record begins whenever the substring of a line starting at breakPosition has breakMarker as a prefix.
// Lines whose substring at breakPosition matches one of acceptedPrefixes are appended to the current record.
// When acceptedPrefixes is empty, all lines after a break line are appended.
// Lines matching neither condition (e.g. file-level headers and trailers) are silently skipped.
//
// Lines within a record are joined with lineSeparator; when empty, DefaultLineSeparator is used.
// fn receives the 0-based record number, the byte offset of the record's break line within r, and the joined record.
func ReadMultiLine(ctx context.Context, r io.Reader, breakPosition int, breakMarker string, acceptedPrefixes []string, lineSeparator string, fn func(number, offset int64, raw string) error) error {
	if lineSeparator == "" {
		lineSeparator = DefaultLineSeparator
	}
	br := bufio.NewReader(r)
	var (
		recordNum  int64
		recLines   []string
		recStart   int64
		byteOffset int64
	)
	emit := func() error {
		if len(recLines) == 0 {
			return nil
		}
		raw := strings.Join(recLines, lineSeparator)
		off := recStart
		recLines = nil
		err := fn(recordNum, off, raw)
		recordNum++
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, err := br.ReadString('\n')
		if len(line) > 0 {
			lineStart := byteOffset
			byteOffset += int64(len(line))
			content := strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
			sub := ""
			if breakPosition < len(content) {
				sub = content[breakPosition:]
			}
			switch {
			case strings.HasPrefix(sub, breakMarker):
				if e := emit(); e != nil {
					return e
				}
				recStart = lineStart
				recLines = append(recLines, content)
			case len(recLines) > 0:
				if len(acceptedPrefixes) == 0 {
					recLines = append(recLines, content)
				} else {
					for _, ap := range acceptedPrefixes {
						if strings.HasPrefix(sub, ap) {
							recLines = append(recLines, content)
							break
						}
					}
				}
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return emit()
}
