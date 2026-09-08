// Worker Lambda bootstrap for the F2E framework.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/f2e/f2e/internal/application/worker"
	awsclient "github.com/f2e/f2e/internal/platform/aws"
	"github.com/f2e/f2e/internal/platform/config"
	"github.com/f2e/f2e/internal/platform/processmetrics"
)

var service worker.Service

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx := context.Background()
	c, e := config.Load()
	if e != nil {
		slog.Error("invalid configuration", "service", "worker", "error", e)
		os.Exit(1)
	}
	a, e := awsclient.New(ctx, c)
	if e != nil {
		slog.Error("aws initialization failed", "service", "worker", "error", e)
		os.Exit(1)
	}
	service = worker.Service{Resolver: a, Queue: a, Config: c}
	if c.LedgerTable != "" {
		service.Ledger = a
	}
}
func handler(ctx context.Context, e events.SQSEvent) (events.SQSEventResponse, error) {
	started := time.Now()
	startUserCPU, startSystemCPU, cpuAvailable := processmetrics.CPUTime()
	defer func() {
		wallSeconds := time.Since(started).Seconds()
		endUserCPU, endSystemCPU, endAvailable := processmetrics.CPUTime()
		if !cpuAvailable || !endAvailable {
			slog.Info("worker invocation resources", "service", "worker", "durationMs", wallSeconds*1000, "cpuAvailable", false)
			return
		}
		userCPU := endUserCPU - startUserCPU
		systemCPU := endSystemCPU - startSystemCPU
		totalCPU := userCPU + systemCPU
		utilization := 0.0
		if wallSeconds > 0 {
			utilization = totalCPU / wallSeconds * 100
		}
		slog.Info("worker invocation resources",
			"service", "worker",
			"durationMs", wallSeconds*1000,
			"cpuAvailable", true,
			"cpuUserMs", userCPU*1000,
			"cpuSystemMs", systemCPU*1000,
			"cpuTotalMs", totalCPU*1000,
			"cpuUtilizationPercent", utilization,
		)
	}()
	out := events.SQSEventResponse{}
	for _, r := range e.Records {
		attempt, _ := strconv.Atoi(r.Attributes["ApproximateReceiveCount"])
		if attempt < 1 {
			attempt = 1
		}
		if err := service.ProcessAttempt(ctx, []byte(r.Body), attempt); err != nil {
			classification := "permanent"
			var transient interface{ Transient() bool }
			if errors.As(err, &transient) && transient.Transient() {
				classification = "transient"
			}
			slog.Error("worker message failed", "service", "worker", "sqsMessageId", r.MessageId, "receiveCount", r.Attributes["ApproximateReceiveCount"], "errorClass", classification, "error", err)
			out.BatchItemFailures = append(out.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: r.MessageId})
		}
	}
	return out, nil
}
func main() { lambda.Start(handler) }
