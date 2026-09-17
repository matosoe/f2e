package f2e

import (
	"bytes"
	"encoding/json"
)

// FindJSONObjectStarts finds complete object elements whose first key is name.
//
// name is matched as a decoded JSON object key, never as arbitrary text. The
// lexical scan deliberately skips JSON strings (including escaped characters),
// so a serialized object or a field name embedded in a field value cannot be a
// record boundary. The producer contract still requires name not to be used as
// the first key of nested objects.
func FindJSONObjectStarts(data []byte, name string) [][2]int {
	var found [][2]int
	inString := false
	escaped := false
	for i := 0; i < len(data); i++ {
		if inString {
			switch {
			case escaped:
				escaped = false
			case data[i] == '\\':
				escaped = true
			case data[i] == '"':
				inString = false
			}
			continue
		}
		if data[i] == '"' {
			inString = true
			continue
		}
		if data[i] != '{' {
			continue
		}
		p := i + 1
		for p < len(data) && (data[p] == ' ' || data[p] == '\t' || data[p] == '\r' || data[p] == '\n') {
			p++
		}
		if p >= len(data) || data[p] != '"' {
			continue
		}
		dec := json.NewDecoder(bytes.NewReader(data[p:]))
		var key string
		if err := dec.Decode(&key); err != nil || key != name {
			continue
		}
		q := p + int(dec.InputOffset())
		for q < len(data) && (data[q] == ' ' || data[q] == '\t' || data[q] == '\r' || data[q] == '\n') {
			q++
		}
		if q >= len(data) || data[q] != ':' {
			continue
		}
		obj := json.NewDecoder(bytes.NewReader(data[i:]))
		var raw json.RawMessage
		if err := obj.Decode(&raw); err != nil {
			continue
		}
		found = append(found, [2]int{i, i + int(obj.InputOffset())})
	}
	return found
}

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
