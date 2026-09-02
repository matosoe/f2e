package f2e

import (
	"bytes"
	"encoding/json"
)

// NextCompleteJSONElement returns the [start, end) byte positions of the next
// complete JSON value in data at or after an element boundary. It accepts every
// JSON value type. Returns (-1, -1) at the containing array's end or when the
// available bounded buffer does not hold a complete value.
func NextCompleteJSONElement(data []byte, pos int) (int, int) {
	for pos < len(data) {
		b := data[pos]
		if b == ']' || b == '}' {
			return -1, -1
		}
		if b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == ',' {
			pos++
			continue
		}
		break
	}
	if pos >= len(data) {
		return -1, -1
	}
	start := pos
	decoder := json.NewDecoder(bytes.NewReader(data[start:]))
	var raw json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return -1, -1
	}
	return start, start + int(decoder.InputOffset())
}

// JSONElementEndAfter iterates elements in data and returns the end position (exclusive)
// of the first element whose end byte index exceeds afterPos.
// Returns -1 if all complete elements end at or before afterPos.
func JSONElementEndAfter(data []byte, afterPos int) int {
	pos := 0
	for {
		start, end := NextCompleteJSONElement(data, pos)
		if start < 0 {
			return -1
		}
		if start > afterPos {
			// afterPos already lies between complete values.
			return -1
		}
		if end > afterPos {
			return end
		}
		pos = end
	}
}
