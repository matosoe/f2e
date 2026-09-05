package internal

import (
	"bytes"
	"fmt"
	"strings"
)

const (
	// maxRecordBytes* are used for variable-length formats when chunking is needed (>1000 records).
	maxRecordBytesText      int64 = 64
	maxRecordBytesMultiLine int64 = 128
	maxBytesPerJSONElement  int64 = 256
	// smallFileThreshold is the maximum record count processed as a single chunk.
	smallFileThreshold = 1000
)

// GenerateText produces N variable-length text lines.
func GenerateText(recordCount int) []byte {
	var buf bytes.Buffer
	buf.Grow(recordCount * 45)
	for i := 1; i <= recordCount; i++ {
		fmt.Fprintf(&buf, "record-%08d-text-data-field-content\n", i)
	}
	return buf.Bytes()
}

// GenerateMultiLine produces a file with one header line (H), N data lines (D), and one trailer
// line (T). The BreakMarker "D" and AcceptedPrefixes ["D"] cause header and trailer to be skipped
// by the F2E multi-line reader, yielding exactly N events.
func GenerateMultiLine(recordCount int) []byte {
	var buf bytes.Buffer
	buf.Grow(recordCount*65 + 30)
	fmt.Fprintf(&buf, "H%s\n", strings.Repeat("0", 19)) // fixed-length header line
	for i := 1; i <= recordCount; i++ {
		fmt.Fprintf(&buf, "D%08d%s\n", i, strings.Repeat("X", 50))
	}
	fmt.Fprintf(&buf, "T%08d\n", recordCount) // trailer with record count
	return buf.Bytes()
}

// GenerateJSONArray produces a JSON array of N element objects.
func GenerateJSONArray(recordCount int) []byte {
	var buf bytes.Buffer
	buf.Grow(recordCount * 70)
	buf.WriteString("[\n")
	for i := 1; i <= recordCount; i++ {
		if i > 1 {
			buf.WriteString(",\n")
		}
		fmt.Fprintf(&buf, `  {"id":%d,"name":"record-%d","value":"data-%d"}`, i, i, i)
	}
	buf.WriteString("\n]\n")
	return buf.Bytes()
}

// MaxRecordLenFor returns the MaxRecordLengthBytes for variable-length formats.
// Returns 0 for small files, which signals the organizer to use single-chunk mode.
func MaxRecordLenFor(dataType string, recordCount int) int64 {
	if recordCount <= smallFileThreshold {
		return 0 // single-chunk: no need for record-length hint
	}
	switch dataType {
	case "text":
		return maxRecordBytesText
	default:
		return 0
	}
}
