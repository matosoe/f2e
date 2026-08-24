package fixedwidth

import (
	"bytes"
	"context"
	"testing"
)

func TestRead(t *testing.T) {
	var got []string
	err := Read(context.Background(), bytes.NewBufferString("abc\ndef\n"), 4, 2, func(_, _ int64, s string) error { got = append(got, s); return nil })
	if err != nil || len(got) != 2 || got[1] != "def" {
		t.Fatalf("got %v %v", got, err)
	}
}
func TestReadRejectsInvalidLF(t *testing.T) {
	if err := Read(context.Background(), bytes.NewBufferString("abcX"), 4, 1, func(_, _ int64, _ string) error { return nil }); err == nil {
		t.Fatal("expected error")
	}
}
