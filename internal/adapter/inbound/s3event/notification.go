// Package s3event decodes Amazon S3 event notifications received by F2E.
package s3event

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/f2e/f2e/internal/domain/f2e"
)

type Notification struct {
	Records []Record `json:"Records"`
}

type Record struct {
	EventName string `json:"eventName"`
	S3        struct {
		Bucket struct {
			Name string `json:"name"`
		} `json:"bucket"`
		Object struct {
			Key       string `json:"key"`
			VersionID string `json:"versionId"`
			Size      int64  `json:"size"`
			ETag      string `json:"eTag"`
			Sequencer string `json:"sequencer"`
		} `json:"object"`
	} `json:"s3"`
}

func Parse(body []byte) ([]f2e.FileReference, error) {
	var notification Notification
	if err := json.Unmarshal(body, &notification); err != nil {
		return nil, err
	}

	references := make([]f2e.FileReference, 0, len(notification.Records))
	for _, record := range notification.Records {
		if !strings.HasPrefix(record.EventName, "ObjectCreated:") {
			continue
		}
		key, err := url.QueryUnescape(strings.ReplaceAll(record.S3.Object.Key, "+", "%20"))
		if err != nil {
			return nil, fmt.Errorf("decode key: %w", err)
		}
		references = append(references, f2e.FileReference{Bucket: record.S3.Bucket.Name, Key: key, VersionID: record.S3.Object.VersionID})
	}

	return references, nil
}
