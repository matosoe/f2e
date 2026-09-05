// query-job is an operational CLI for the F2E ledger.
//
// Usage:
//
//	query-job -jobId <id>
//	query-job -receiptId <id>
//	query-job -status PROCESSING [-after <rfc3339>] [-before <rfc3339>] [-limit N]
//	query-job -stuck [-stuckMinutes N] [-limit N]
//
// The tool reads F2E_LEDGER_TABLE, AWS_REGION and optionally AWS_ENDPOINT_URL
// from the environment. It prints JSON to stdout; non-zero exit on error.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/f2e/f2e/internal/domain/f2e"
	awsclient "github.com/f2e/f2e/internal/platform/aws"
	"github.com/f2e/f2e/internal/platform/config"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	jobID := flag.String("jobId", "", "query by jobId")
	receiptID := flag.String("receiptId", "", "query by SQS receiptId")
	status := flag.String("status", "", "query by status (e.g. PROCESSING)")
	after := flag.String("after", "", "updatedAt >= after (RFC3339)")
	before := flag.String("before", "", "updatedAt < before (RFC3339)")
	stuck := flag.Bool("stuck", false, "find stuck (non-terminal) jobs")
	stuckMinutes := flag.Int("stuckMinutes", 30, "minutes without progress to consider stuck")
	limit := flag.Int("limit", 100, "maximum results to return")
	flag.Parse()

	ctx := context.Background()
	c, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if c.LedgerTable == "" {
		return fmt.Errorf("F2E_LEDGER_TABLE is required")
	}
	a, err := awsclient.New(ctx, c)
	if err != nil {
		return fmt.Errorf("aws client: %w", err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	switch {
	case *jobID != "":
		summary, err := a.GetJob(ctx, *jobID)
		if err != nil {
			return err
		}
		return enc.Encode(summary)
	case *receiptID != "":
		summary, err := a.GetJobByReceiptID(ctx, *receiptID)
		if err != nil {
			return err
		}
		return enc.Encode(summary)
	case *status != "":
		summaries, err := a.QueryJobsByStatus(ctx, f2e.JobStatus(*status), *after, *before, *limit)
		if err != nil {
			return err
		}
		return enc.Encode(summaries)
	case *stuck:
		threshold := time.Duration(*stuckMinutes) * time.Minute
		summaries, err := a.StuckJobs(ctx, threshold, *limit)
		if err != nil {
			return err
		}
		return enc.Encode(summaries)
	default:
		flag.Usage()
		return fmt.Errorf("one of -jobId, -receiptId, -status or -stuck is required")
	}
}
