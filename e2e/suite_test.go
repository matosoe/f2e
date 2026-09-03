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
package e2e_test

import (
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

	suite := godog.TestSuite{
		Name:                "f2e-e2e",
		ScenarioInitializer: internal.NewScenarioInitializer(client),
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
		return
	}
	// Six explicit empty-file scenarios are rejected by the organizer. They
	// must be auditable in intake DLQ; no worker job is expected to be poison.
	if internal.RealAWS() {
		if err := client.WaitForDLQCounts(t.Context(), 6, 0, 8*time.Minute); err != nil {
			t.Fatal(err)
		}
	} else {
		intakeDLQ, chunkDLQ, err := client.DLQCounts(t.Context())
		if err != nil {
			t.Fatalf("read DLQs: %v", err)
		}
		if intakeDLQ != 6 || chunkDLQ != 0 {
			t.Fatalf("unexpected DLQ counts: intake=%d (want 6), chunks=%d (want 0)", intakeDLQ, chunkDLQ)
		}
	}
	// 30 valid scenarios (including the empty JSON array) create ledger jobs.
	if err := client.AssertLedgerComplete(t.Context(), 30); err != nil {
		t.Fatal(err)
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
