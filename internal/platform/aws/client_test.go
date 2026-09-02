package aws

import (
	"context"
	"strings"
	"testing"

	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/domain/f2e"
)

func TestPresignedGetRangeRejectsUnsafeURLs(t *testing.T) {
	for _, raw := range []string{
		"http://bucket.s3.amazonaws.com/object",
		"https://localhost/object",
		"https://bucket.example.com/object",
	} {
		if _, err := presignedGetRange(context.Background(), raw, 0, 1); err == nil {
			t.Fatalf("expected URL %q to be rejected", raw)
		}
	}
}

func TestValidateOutboundMessageLimitsAttributesAndTotalSize(t *testing.T) {
	valid := port.OutboundMessage{Body: `{}`, Attributes: map[string]port.MessageAttribute{"schema": {DataType: "String", Value: "record:1"}}}
	if err := validateOutboundMessage(valid); err != nil {
		t.Fatalf("valid message rejected: %v", err)
	}
	for name, message := range map[string]port.OutboundMessage{
		"reserved name": {Body: `{}`, Attributes: map[string]port.MessageAttribute{"AWS.bad": {DataType: "String", Value: "x"}}},
		"invalid type":  {Body: `{}`, Attributes: map[string]port.MessageAttribute{"schema": {DataType: "Number", Value: "1"}}},
		"oversized":     {Body: strings.Repeat("x", 256*1024+1)},
	} {
		if err := validateOutboundMessage(message); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
}

func TestValidateS3RangeIdentityDetectsReplacementAndTruncation(t *testing.T) {
	identity := f2e.ObjectIdentity{ETag: `"original"`, VersionID: "v1", Size: 100}
	for name, err := range map[string]error{
		"etag":       validateS3RangeIdentity(identity, 0, 9, 10, "bytes 0-9/100", `"replacement"`, "v1"),
		"version":    validateS3RangeIdentity(identity, 0, 9, 10, "bytes 0-9/100", `"original"`, "v2"),
		"truncation": validateS3RangeIdentity(identity, 0, 9, 8, "bytes 0-7/100", `"original"`, "v1"),
	} {
		if err == nil || !strings.Contains(err.Error(), "immutable object") {
			t.Errorf("%s: expected immutable identity error, got %v", name, err)
		}
	}
	if err := validateS3RangeIdentity(identity, 0, 9, 10, "bytes 0-9/100", `"original"`, "v1"); err != nil {
		t.Fatalf("valid identity rejected: %v", err)
	}
}

func TestPresignedGetRangeRejectsInvalidRange(t *testing.T) {
	if _, err := presignedGetRange(context.Background(), "https://bucket.s3.amazonaws.com/object", 1, 0); err == nil {
		t.Fatal("expected invalid range to be rejected")
	}
}
