// Package port defines the dependencies required by application services.
package port

import (
	"context"
	"io"
)

// MessageAttribute is an SQS/SNS message attribute.
type MessageAttribute struct {
	DataType string
	Value    string
}

type ObjectStore interface {
	Head(context.Context, string, string) (size int64, eTag string, versionID string, err error)
	GetRange(context.Context, string, string, int64, int64) (io.ReadCloser, error)
}

type Queue interface {
	Send(context.Context, string, []string, map[string]MessageAttribute) ([]int, error)
}
