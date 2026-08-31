package f2e

// JSONObjectEnd returns the number of bytes consumed to close the first JSON element
// that begins (or is already open) in data.  Handles nested objects/arrays and strings
// with escape sequences.  Returns -1 if no element boundary is found within data.
func JSONObjectEnd(data []byte) int {
	depth := 0
	inStr := false
	for i := 0; i < len(data); i++ {
		b := data[i]
		if inStr {
			if b == '\\' {
				i++
				continue
			}
			if b == '"' {
				inStr = false
			}
			continue
		}
		switch b {
		case '"':
			inStr = true
		case '{', '[':
			depth++
		case '}', ']':
			depth--
			if depth <= 0 {
				return i + 1
			}
		}
	}
	return -1
}

// NextCompleteJSONElement returns the [start, end) byte positions of the next complete
// JSON element (object or array) in data at or after pos.
// If the content at pos is the tail of a partial element (non-bracket content), that
// partial element is skipped first.
// Returns (-1, -1) when no complete element is found.
func NextCompleteJSONElement(data []byte, pos int) (int, int) {
	for pos < len(data) {
		b := data[pos]
		if b == ']' || b == '}' {
			return -1, -1
		}
		if b == '{' || b == '[' {
			break
		}
		if b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == ',' {
			pos++
			continue
		}
		// Mid-element content: skip to end of the partial element.
		end := JSONObjectEnd(data[pos:])
		if end < 0 {
			return -1, -1
		}
		pos += end
		for pos < len(data) && (data[pos] == ' ' || data[pos] == '\t' || data[pos] == '\n' || data[pos] == '\r' || data[pos] == ',') {
			pos++
		}
		if pos >= len(data) || data[pos] == ']' || data[pos] == '}' {
			return -1, -1
		}
		break
	}
	if pos >= len(data) {
		return -1, -1
	}
	start := pos
	length := JSONObjectEnd(data[start:])
	if length < 0 {
		return -1, -1
	}
	return start, start + length
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
		if end > afterPos {
			return end
		}
		pos = end
	}
}
