package f2e

import "testing"

func TestNextCompleteJSONElementSupportsEveryJSONValue(t *testing.T) {
	data := []byte(` {"nested":[1,{"escaped":"a\"b"}]}, [true], "text", 42, false, null ] trailing`)
	want := []string{`{"nested":[1,{"escaped":"a\"b"}]}`, `[true]`, `"text"`, `42`, `false`, `null`}
	pos := 0
	for i, expected := range want {
		start, end := NextCompleteJSONElement(data, pos)
		if start < 0 || string(data[start:end]) != expected {
			t.Fatalf("element %d: got %q, want %q", i, slice(data, start, end), expected)
		}
		pos = end
	}
	if start, end := NextCompleteJSONElement(data, pos); start != -1 || end != -1 {
		t.Fatalf("expected array end, got %d:%d", start, end)
	}
}

func TestJSONElementEndAfterDoesNotConsumeNextElementAcrossSeparator(t *testing.T) {
	data := []byte(`{"a":1}, {"b":2}`)
	if got := JSONElementEndAfter(data, 8); got != -1 {
		t.Fatalf("separator is already a safe boundary, got padding end %d", got)
	}
	if got := JSONElementEndAfter(data, 3); got != 7 {
		t.Fatalf("boundary inside first value: got end %d, want 7", got)
	}
}

func slice(data []byte, start, end int) string {
	if start < 0 || end < start || end > len(data) {
		return ""
	}
	return string(data[start:end])
}
