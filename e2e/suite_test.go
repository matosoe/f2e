// Package e2e_test contains end-to-end tests for the F2E pipeline using Godog (Gherkin/BDD).
//
// Prerequisites: start the local environment with automacao/subir-ambiente.sh before running.
//
// Run all non-load tests (sequential, default):
//
//	cd e2e && go test -v ./...
//
// Run with parallel scenarios (4 concurrent):
//
//	cd e2e && E2E_CONCURRENCY=4 go test -v ./...
//
// Run benchmark comparison (sequential vs parallel):
//
//	cd e2e && E2E_BENCHMARK=true E2E_CONCURRENCY=4 go test -v ./...
//
// Run including 1-million-record load scenarios:
//
//	cd e2e && E2E_LOAD_TESTS=true go test -v -timeout 30m ./...
//
// Five reports are written to E2E_REPORT_DIR (default: relatorios) after each run.
// All durations are local wall-clock milliseconds. AWS-side timestamps from envelope.metadata.createdAt
// are stored separately and must not be compared with local durations.
package e2e_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/cucumber/godog"
	"github.com/matosoe/f2e/e2e/internal"
)

func TestE2E(t *testing.T) {
	if !internal.RealAWS() && !internal.LocalStackAvailable() {
		t.Fatalf("LocalStack indisponível em %s — execute automacao/subir-ambiente.sh antes da suíte E2E", internal.LocalStackEndpoint())
	}

	client := internal.NewAWSClient()
	baselineIntake, baselineChunk, err := client.DLQCounts(t.Context())
	if err != nil {
		t.Fatalf("read initial DLQ counts: %v", err)
	}

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

	concurrency := 1
	if v := os.Getenv("E2E_CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			concurrency = n
		}
	}

	runStart := time.Now()
	runID := runStart.Local().Format("20060102T150405.000000000-0700")
	reportDir := os.Getenv("E2E_REPORT_DIR")
	if reportDir == "" {
		reportDir = "relatorios"
	}
	if err := os.MkdirAll(reportDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := func(kind, ext string) string { return filepath.Join(reportDir, runID+"-"+kind+"."+ext) }
	recorder, err := internal.NewMessageRecorder(path("eventos", "jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	client.Recorder = recorder

	var scenarioMetrics []internal.ScenarioMetrics
	var benchmarkResult *internal.BenchmarkComparison
	intakeDLQ, chunkDLQ := 0, 0
	defer func() {
		if err := recorder.Close(); err != nil {
			t.Errorf("event report: %v", err)
		}
		writeExecutionReports(t, client, path, runID, runStart, target, scenarioMetrics, intakeDLQ, chunkDLQ, benchmarkResult)
	}()

	if os.Getenv("E2E_BENCHMARK") == "true" && concurrency > 1 {
		benchmarkResult = runBenchmark(t, client, tags, concurrency)
	}

	var initializer func(*godog.ScenarioContext)
	var dispatcher *internal.MessageDispatcher

	if concurrency > 1 {
		dispatcher = internal.NewMessageDispatcher(client)
		defer dispatcher.Stop()
		initializer = internal.NewScenarioInitializerWithDispatcher(client, dispatcher, &scenarioMetrics)
	} else {
		initializer = internal.NewScenarioInitializerWithMetrics(client, &scenarioMetrics)
	}

	suite := godog.TestSuite{
		Name:                "f2e-e2e",
		ScenarioInitializer: initializer,
		Options: &godog.Options{
			Format:      "pretty",
			Paths:       []string{"features"},
			TestingT:    t,
			Strict:      true,
			Concurrency: concurrency,
			Tags:        tags,
		},
	}

	if suite.Run() != 0 {
		t.Fatal("E2E feature suite failed")
	}
	// A focused tag run is useful during development; its scenario count and
	// expected DLQ state intentionally differ from the full regression suite.
	if os.Getenv("E2E_TAGS") != "" {
		return
	}
	// The local suite has three deterministic empty-file rejections. They must
	// be auditable in intake DLQ; no worker job is expected to be poison.
	if internal.RealAWS() {
		if err := client.WaitForDLQCounts(t.Context(), baselineIntake+3, baselineChunk, 8*time.Minute); err != nil {
			t.Fatal(err)
		}
		intakeDLQ, chunkDLQ = 3, 0
	} else {
		var err error
		intakeDLQ, chunkDLQ, err = client.DLQCounts(t.Context())
		if err != nil {
			t.Fatalf("read DLQs: %v", err)
		}
		intakeDLQ -= baselineIntake
		chunkDLQ -= baselineChunk
		if intakeDLQ != 3 || chunkDLQ != 0 {
			t.Fatalf("unexpected new DLQ counts: intake=%d (want 3), chunks=%d (want 0)", intakeDLQ, chunkDLQ)
		}
	}
	// Four empty text/multi-line scenarios are rejected; every other scenario,
	// including the empty JSON array, creates a ledger job. The three 10,000-
	// record cases were intentionally removed from the local suite.
	if err := client.AssertLedgerComplete(t.Context(), 14, runStart); err != nil {
		t.Fatal(err)
	}
}

// runBenchmark runs the scenario suite twice — first sequential, then parallel
// at the given concurrency — and returns a BenchmarkComparison. The benchmark
// intentionally uses tag filtering to run a representative but fast subset.
func runBenchmark(t *testing.T, client *internal.AWSClient, tags string, concurrency int) *internal.BenchmarkComparison {
	t.Helper()

	// Sequential run.
	seqStart := time.Now()
	seqSuite := godog.TestSuite{
		Name:                "f2e-e2e-seq",
		ScenarioInitializer: internal.NewScenarioInitializer(client),
		Options: &godog.Options{
			Format:      "progress",
			Paths:       []string{"features"},
			TestingT:    t,
			Strict:      true,
			Concurrency: 1,
			Tags:        tags + ",~@load",
		},
	}
	seqSuite.Run()
	seqMs := time.Since(seqStart).Milliseconds()

	// Parallel run.
	parStart := time.Now()
	d := internal.NewMessageDispatcher(client)
	defer d.Stop()
	parSuite := godog.TestSuite{
		Name:                "f2e-e2e-par",
		ScenarioInitializer: internal.NewScenarioInitializerWithDispatcher(client, d, nil),
		Options: &godog.Options{
			Format:      "progress",
			Paths:       []string{"features"},
			TestingT:    t,
			Strict:      true,
			Concurrency: concurrency,
			Tags:        tags + ",~@load",
		},
	}
	parSuite.Run()
	parMs := time.Since(parStart).Milliseconds()

	speedup := 0.0
	if parMs > 0 {
		speedup = float64(seqMs) / float64(parMs)
	}

	return &internal.BenchmarkComparison{
		Concurrency:       concurrency,
		SequentialTotalMs: seqMs,
		ParallelTotalMs:   parMs,
		SpeedupFactor:     speedup,
		Note:              "Speedup is valid only when both runs target the same AWS environment. LocalStack single-node may not reflect production parallelism.",
	}
}

// writeExecutionReports persists the summary and the queue/ledger snapshots.
func writeExecutionReports(t *testing.T, client *internal.AWSClient, path func(string, string) string, runID string, runStart time.Time, target string, scenarios []internal.ScenarioMetrics, intakeDLQ, chunkDLQ int, benchmark *internal.BenchmarkComparison) {
	t.Helper()
	sort.Slice(scenarios, func(i, j int) bool { return scenarios[i].ScenarioName < scenarios[j].ScenarioName })
	report := internal.RunReport{
		RunID:          runID,
		StartedAt:      runStart.Local().Format(time.RFC3339),
		EndedAt:        time.Now().Local().Format(time.RFC3339),
		TimeZone:       runStart.Local().Format("-07:00"),
		TotalMs:        time.Since(runStart).Milliseconds(),
		Target:         target,
		Scenarios:      scenarios,
		ScenarioCount:  len(scenarios),
		IntakeDLQCount: intakeDLQ,
		ChunkDLQCount:  chunkDLQ,
		Benchmark:      benchmark,
		Notes: []string{
			"All durations are local wall-clock milliseconds (local clock only).",
			"AWS-side timestamps in earliestEnvelopeCreatedAt / latestEnvelopeCreatedAt use the Worker clock and must not be subtracted from local durations.",
			fmt.Sprintf("Target: %s. AWS-side latency has not been independently measured; the reported 50-minute figure is not reproduced here as a baseline.", target),
		},
	}
	for _, scenario := range scenarios {
		report.TotalRecords += scenario.RecordCount
	}

	writeJSON := func(dest string, value any) {
		data, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			t.Errorf("marshal %s: %v", dest, err)
			return
		}
		if err := os.WriteFile(dest, data, 0o600); err != nil {
			t.Errorf("write %s: %v", dest, err)
		}
	}
	ctx := t.Context()
	completion, err := client.SnapshotQueue(ctx, envOr("F2E_E2E_COMPLETION_QUEUE_NAME", "completion-events"), runStart)
	if err != nil {
		t.Errorf("completion snapshot: %v", err)
	}
	writeJSONLines(t, path("completion-events", "jsonl"), completion)
	dlq := make([]internal.MessageReport, 0)
	completionDLQName := envOr("F2E_E2E_COMPLETION_DLQ_NAME", "completion-events-dlq")
	for _, name := range []string{envOr("F2E_E2E_INTAKE_DLQ_NAME", "file-intake-dlq"), envOr("F2E_E2E_CHUNK_DLQ_NAME", "chunk-jobs-dlq"), completionDLQName} {
		messages, snapshotErr := client.SnapshotQueue(ctx, name, runStart)
		if snapshotErr != nil {
			t.Errorf("DLQ %s snapshot: %v", name, snapshotErr)
		}
		if name == completionDLQName {
			report.CompletionDLQCount = len(messages)
		}
		dlq = append(dlq, messages...)
	}
	writeJSONLines(t, path("dlq", "jsonl"), dlq)
	ledger, err := client.SnapshotLedger(ctx)
	if err != nil {
		t.Errorf("ledger snapshot: %v", err)
	}
	writeJSON(path("dynamodb", "json"), ledger)
	writeJSON(path("resumo", "json"), report)
	if legacy := os.Getenv("E2E_METRICS_FILE"); legacy != "" {
		writeJSON(legacy, report)
	}
	t.Logf("reports written under %s", filepath.Dir(path("resumo", "json")))
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func writeJSONLines(t *testing.T, dest string, entries []internal.MessageReport) {
	t.Helper()
	f, err := os.Create(dest)
	if err != nil {
		t.Errorf("create %s: %v", dest, err)
		return
	}
	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			t.Errorf("marshal %s: %v", dest, err)
			break
		}
		if _, err := f.Write(append(data, '\n')); err != nil {
			t.Errorf("write %s: %v", dest, err)
			break
		}
	}
	if err := f.Close(); err != nil {
		t.Errorf("close %s: %v", dest, err)
	}
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
