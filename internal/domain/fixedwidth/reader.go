// Package fixedwidth reads fixed-width ASCII records.
package fixedwidth

import (
	"bufio"
	"context"
	"fmt"
	"io"
)

// Each fixed-width record includes exactly one LF byte.
func Read(ctx context.Context, r io.Reader, recordLength, count int64, fn func(number, offset int64, raw string) error) error {
	if recordLength < 2 || count < 0 {
		return fmt.Errorf("invalid record layout")
	}
	br := bufio.NewReaderSize(r, 32*1024)
	b := make([]byte, recordLength)
	for i := int64(0); i < count; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := io.ReadFull(br, b); err != nil {
			return fmt.Errorf("record %d: short read: %w", i, err)
		}
		if b[recordLength-1] != '\n' {
			return fmt.Errorf("record %d: missing LF", i)
		}
		for _, v := range b[:recordLength-1] {
			if v > 127 {
				return fmt.Errorf("record %d: non-ASCII data", i)
			}
		}
		if err := fn(i, i*recordLength, string(b[:recordLength-1])); err != nil {
			return err
		}
	}
	var extra [1]byte
	if n, _ := br.Read(extra[:]); n != 0 {
		return fmt.Errorf("range has extra bytes")
	}
	return nil
}
