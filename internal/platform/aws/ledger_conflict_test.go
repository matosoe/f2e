package aws

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestTransactionConflictClassification(t *testing.T) {
	conflict := &types.TransactionCanceledException{CancellationReasons: []types.CancellationReason{{Code: stringPointer("TransactionConflict")}}}
	if !transactionConflict(fmt.Errorf("wrapped: %w", conflict)) {
		t.Fatal("expected wrapped TransactionConflict to be retryable")
	}
	conditional := &types.TransactionCanceledException{CancellationReasons: []types.CancellationReason{{Code: stringPointer("ConditionalCheckFailed")}}}
	if transactionConflict(conditional) {
		t.Fatal("conditional failure must not be classified as transaction conflict")
	}
}

func TestCompleteChunkTokenIsStableAndChunkScoped(t *testing.T) {
	first := completeChunkToken("job", "chunk-1", 1, 0)
	if len(first) != 36 {
		t.Fatalf("token length = %d, want 36", len(first))
	}
	if first != completeChunkToken("job", "chunk-1", 1, 0) {
		t.Fatal("same chunk attempt must produce the same token")
	}
	if first == completeChunkToken("job", "chunk-2", 1, 0) {
		t.Fatal("different chunks must not share a token")
	}
	if first == completeChunkToken("job", "chunk-1", 1, 1) {
		t.Fatal("a canceled-conflict retry must use a fresh token")
	}
}

func stringPointer(value string) *string { return &value }
