package aws

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/f2e/f2e/internal/domain/f2e"
)

// ResolvePrefixConfiguration selects the most specific SSM configuration whose
// bucket and key prefix match the received S3 object. It returns the
// configuration together with an immutable provenance snapshot.
func (a *AWS) ResolvePrefixConfiguration(ctx context.Context, bucket, key string) (f2e.PrefixConfiguration, f2e.ConfigurationSnapshot, error) {
	path := strings.TrimRight(a.FileConfigPath, "/") + "/" + bucket + "/"
	input := &ssm.GetParametersByPathInput{Path: awsSDK.String(path), Recursive: awsSDK.Bool(true), WithDecryption: awsSDK.Bool(true)}
	var selected f2e.PrefixConfiguration
	var selectedParamName string
	var selectedParamVersion int64
	for {
		out, err := a.SSM.GetParametersByPath(ctx, input)
		if err != nil {
			return selected, f2e.ConfigurationSnapshot{}, fmt.Errorf("read SSM file configurations below %s: %w", path, err)
		}
		for _, parameter := range out.Parameters {
			var candidate f2e.PrefixConfiguration
			if err := json.Unmarshal([]byte(awsSDK.ToString(parameter.Value)), &candidate); err != nil {
				return selected, f2e.ConfigurationSnapshot{}, fmt.Errorf("decode SSM parameter %s: %w", awsSDK.ToString(parameter.Name), err)
			}
			if candidate.Bucket == bucket && strings.HasPrefix(key, candidate.Prefix) && len(candidate.Prefix) > len(selected.Prefix) {
				selected = candidate
				selectedParamName = awsSDK.ToString(parameter.Name)
				selectedParamVersion = parameter.Version
			}
		}
		if out.NextToken == nil {
			break
		}
		input.NextToken = out.NextToken
	}
	if selected.Prefix == "" {
		return selected, f2e.ConfigurationSnapshot{}, fmt.Errorf("no SSM file configuration matches s3://%s/%s", bucket, key)
	}
	raw, _ := json.Marshal(selected)
	configHash := sha256Hex(raw)
	snapshot := f2e.ConfigurationSnapshot{
		ConfigHash:       configHash,
		ParameterName:    selectedParamName,
		ParameterVersion: selectedParamVersion,
		Responsible:      selected.Responsible,
		LoadedAt:         time.Now().UTC().Format(time.RFC3339Nano),
	}
	return selected, snapshot, nil
}

func (a *AWS) LoadGlobalLimits(ctx context.Context) (f2e.GlobalLimits, int64, string, error) {
	var limits f2e.GlobalLimits
	out, err := a.SSM.GetParameter(ctx, &ssm.GetParameterInput{Name: awsSDK.String(a.GlobalLimitsParameter), WithDecryption: awsSDK.Bool(true)})
	if err != nil {
		return limits, 0, "", fmt.Errorf("read SSM global limits %s: %w", a.GlobalLimitsParameter, err)
	}
	if err := json.Unmarshal([]byte(awsSDK.ToString(out.Parameter.Value)), &limits); err != nil {
		return limits, 0, "", fmt.Errorf("decode SSM global limits %s: %w", a.GlobalLimitsParameter, err)
	}
	return limits, out.Parameter.Version, a.GlobalLimitsParameter, nil
}

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
