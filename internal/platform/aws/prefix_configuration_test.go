package aws

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/f2e/f2e/internal/domain/f2e"
)

func TestResolvePrefixConfigurationSelectsMostSpecificPrefix(t *testing.T) {
	base := f2e.PrefixConfiguration{Bucket: "input", Prefix: "example/", DataType: f2e.DataTypeText}
	specific := f2e.PrefixConfiguration{Bucket: "input", Prefix: "example-text/", DataType: f2e.DataTypeText, MaxRecordLengthBytes: 128}
	a := testSSMClient(t, map[string]any{"Parameters": parameters(t, base, specific)})

	got, err := a.ResolvePrefixConfiguration(context.Background(), "input", "example-text/records.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got.Prefix != specific.Prefix || got.MaxRecordLengthBytes != 128 {
		t.Fatalf("selected %+v, want %+v", got, specific)
	}
	if _, err := a.ResolvePrefixConfiguration(context.Background(), "input", "unknown/file.txt"); err == nil {
		t.Fatal("expected an unmatched key to fail")
	}
}

func TestLoadGlobalLimitsDecodesSSMDocument(t *testing.T) {
	want := f2e.GlobalLimits{MaxFileBytes: 1024, MaxChunkBytes: 1024, MaxEventBytes: 1024, MaxBatchSize: 1, MaxJSONArraySearchBytes: 1024, InputTypes: map[f2e.DataType]f2e.InputTypeLimits{f2e.DataTypeText: {MaxFileBytes: 1024, MaxRecordBytes: 512}}}
	value, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	a := testSSMClient(t, map[string]any{"Parameter": map[string]string{"Name": "/f2e/local/global-limits", "Value": string(value)}})

	got, err := a.LoadGlobalLimits(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxEventBytes != want.MaxEventBytes || got.InputTypes[f2e.DataTypeText].MaxRecordBytes != 512 {
		t.Fatalf("decoded %+v, want %+v", got, want)
	}
}

func parameters(t *testing.T, configurations ...f2e.PrefixConfiguration) []map[string]string {
	t.Helper()
	result := make([]map[string]string, 0, len(configurations))
	for i, configuration := range configurations {
		value, err := json.Marshal(configuration)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, map[string]string{"Name": "/f2e/local/file-config/input/" + string(rune('a'+i)), "Value": string(value)})
	}
	return result
}

func testSSMClient(t *testing.T, response map[string]any) *AWS {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	endpoint := server.URL
	client := ssm.NewFromConfig(awsSDK.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}, func(options *ssm.Options) {
		options.BaseEndpoint = &endpoint
	})
	return &AWS{SSM: client, FileConfigPath: "/f2e/local/file-config", GlobalLimitsParameter: "/f2e/local/global-limits"}
}
