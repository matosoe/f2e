package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/f2e/f2e/internal/domain/f2e"
)

// ResolvePrefixConfiguration selects the most specific SSM configuration whose
// bucket and key prefix match the received S3 object.
func (a *AWS) ResolvePrefixConfiguration(ctx context.Context, bucket, key string) (f2e.PrefixConfiguration, error) {
	path := strings.TrimRight(a.FileConfigPath, "/") + "/" + bucket + "/"
	input := &ssm.GetParametersByPathInput{Path: awsSDK.String(path), Recursive: awsSDK.Bool(true), WithDecryption: awsSDK.Bool(true)}
	var selected f2e.PrefixConfiguration
	for {
		out, err := a.SSM.GetParametersByPath(ctx, input)
		if err != nil {
			return selected, fmt.Errorf("read SSM file configurations below %s: %w", path, err)
		}
		for _, parameter := range out.Parameters {
			var candidate f2e.PrefixConfiguration
			if err := json.Unmarshal([]byte(awsSDK.ToString(parameter.Value)), &candidate); err != nil {
				return selected, fmt.Errorf("decode SSM parameter %s: %w", awsSDK.ToString(parameter.Name), err)
			}
			if candidate.Bucket == bucket && strings.HasPrefix(key, candidate.Prefix) && len(candidate.Prefix) > len(selected.Prefix) {
				selected = candidate
			}
		}
		if out.NextToken == nil {
			break
		}
		input.NextToken = out.NextToken
	}
	if selected.Prefix == "" {
		return selected, fmt.Errorf("no SSM file configuration matches s3://%s/%s", bucket, key)
	}
	return selected, nil
}

func (a *AWS) LoadGlobalLimits(ctx context.Context) (f2e.GlobalLimits, error) {
	var limits f2e.GlobalLimits
	out, err := a.SSM.GetParameter(ctx, &ssm.GetParameterInput{Name: awsSDK.String(a.GlobalLimitsParameter), WithDecryption: awsSDK.Bool(true)})
	if err != nil {
		return limits, fmt.Errorf("read SSM global limits %s: %w", a.GlobalLimitsParameter, err)
	}
	if err := json.Unmarshal([]byte(awsSDK.ToString(out.Parameter.Value)), &limits); err != nil {
		return limits, fmt.Errorf("decode SSM global limits %s: %w", a.GlobalLimitsParameter, err)
	}
	return limits, nil
}
