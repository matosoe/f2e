// Package e2e_test contains end-to-end tests for the F2E pipeline using Godog (Gherkin/BDD).
//
// Prerequisites: start the local environment with automacao/subir-ambiente.sh before running.
//
// Run all non-load tests:
//
//	cd e2e && go test -v ./...
//
// Run including 1-million-record load scenarios:
//
//	cd e2e && E2E_LOAD_TESTS=true go test -v -timeout 30m ./...
//
// A JSON metrics report is written to E2E_METRICS_FILE (default: e2e-metrics.json) after each run.
// All durations are local wall-clock milliseconds. AWS-side timestamps from envelope.metadata.createdAt
// are stored separately and must not be compared with local durations.
package e2e_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cucumber/godog"
	"github.com/f2e/f2e/e2e/internal"
)

func TestE2E(t *testing.T) {
	if !internal.RealAWS() && !internal.LocalStackAvailable() {
		t.Fatal("LocalStack not available at http://localhost:4566 — run automacao/subir-ambiente.sh before executing the E2E suite")
	}

	client := internal.NewAWSClient()

	tags := "~@load"
	if os.Getenv("E2E_LOAD_TESTS") == "true" {
		tags = ""
	}
	if requestedTags := os.Getenv("E2E_TAGS"); requestedTags != "" {
		tags = requestedTags
	}

	target := "localstack"
	if internal.RealAWS() {
		target = "aws"
	}

	runStart := time.Now()
	runID := runStart.UTC().Format("20060102T150405Z")

	var scenarioMetrics []internal.ScenarioMetrics

	suite := godog.TestSuite{
		Name:                "f2e-e2e",
		ScenarioInitializer: internal.NewScenarioInitializerWithMetrics(client, &scenarioMetrics),
		Options: &godog.Options{
			Format:      "pretty",
			Paths:       []string{"features"},
			TestingT:    t,
			Strict:      true,
			Concurrency: 1, // sequential: all scenarios share the same output queue
			Tags:        tags,
		},
	}

	if suite.Run() != 0 {
		t.Fatal("E2E feature suite failed")
	}
	// A focused tag run is useful during development; its scenario count and
	// expected DLQ state intentionally differ from the full regression suite.
	if os.Getenv("E2E_TAGS") != "" {
		writeMetricsReport(t, runID, runStart, target, scenarioMetrics, 0, 0)
		return
	}
	// Two empty-file scenarios (text and multi-line) are rejected by the organizer.
	// They must be auditable in intake DLQ; no worker job is expected to be poison.
	intakeDLQ, chunkDLQ := 0, 0
	if internal.RealAWS() {
		if err := client.WaitForDLQCounts(t.Context(), 2, 0, 8*time.Minute); err != nil {
			t.Fatal(err)
		}
		intakeDLQ, chunkDLQ = 2, 0
	} else {
		var err error
		intakeDLQ, chunkDLQ, err = client.DLQCounts(t.Context())
		if err != nil {
			t.Fatalf("read DLQs: %v", err)
		}
		if intakeDLQ != 2 || chunkDLQ != 0 {
			t.Fatalf("unexpected DLQ counts: intake=%d (want 2), chunks=%d (want 0)", intakeDLQ, chunkDLQ)
		}
	}
	// 14 valid scenarios (text:4, multi-line:4, json-array:5 including empty, ssm:1)
	// create ledger jobs.
	if err := client.AssertLedgerComplete(t.Context(), 14); err != nil {
		t.Fatal(err)
	}
	writeMetricsReport(t, runID, runStart, target, scenarioMetrics, intakeDLQ, chunkDLQ)
}

// writeMetricsReport persists the RunReport to the file named by E2E_METRICS_FILE
// (default: e2e-metrics.json in the working directory). Failures to write are
// logged as test warnings rather than fatal errors so they don't mask suite results.
func writeMetricsReport(t *testing.T, runID string, runStart time.Time, target string, scenarios []internal.ScenarioMetrics, intakeDLQ, chunkDLQ int) {
	t.Helper()
	report := internal.RunReport{
		RunID:          runID,
		StartedAt:      runStart.UTC().Format(time.RFC3339),
		EndedAt:        time.Now().UTC().Format(time.RFC3339),
		TotalMs:        time.Since(runStart).Milliseconds(),
		Target:         target,
		Scenarios:      scenarios,
		IntakeDLQCount: intakeDLQ,
		ChunkDLQCount:  chunkDLQ,
		Notes: []string{
			"All durations are local wall-clock milliseconds (local clock only).",
			"AWS-side timestamps in earliestEnvelopeCreatedAt / latestEnvelopeCreatedAt use the Worker clock and must not be subtracted from local durations.",
			fmt.Sprintf("Target: %s. AWS-side latency has not been independently measured; the reported 50-minute figure is not reproduced here as a baseline.", target),
		},
	}

	dest := os.Getenv("E2E_METRICS_FILE")
	if dest == "" {
		dest = "e2e-metrics.json"
	}
	dest, _ = filepath.Abs(dest)

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Logf("WARNING: failed to marshal metrics report: %v", err)
		return
	}
	if err := os.WriteFile(dest, data, 0o600); err != nil {
		t.Logf("WARNING: failed to write metrics report to %s: %v", dest, err)
		return
	}
	t.Logf("metrics report written to %s (%d scenarios)", dest, len(scenarios))
}

func TestUndefinedStepFailsStrictSuite(t *testing.T) {
	dir := t.TempDir()
	feature := "Feature: strict mode\n  Scenario: undefined\n    Given a step that does not exist\n"
	path := filepath.Join(dir, "undefined.feature")
	if err := os.WriteFile(path, []byte(feature), 0o600); err != nil {
		t.Fatal(err)
	}
	suite := godog.TestSuite{Name: "strict-sanity", Options: &godog.Options{Format: "progress", Paths: []string{path}, Strict: true}}
	if suite.Run() == 0 {
		t.Fatal("strict Godog suite accepted an undefined step")
	}
}
