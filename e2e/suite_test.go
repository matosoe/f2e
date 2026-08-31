package e2e_test
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
	"testing"

	"github.com/cucumber/godog"
	"github.com/f2e/f2e/e2e/internal"
)

func TestE2E(t *testing.T) {
	if !internal.LocalStackAvailable() {
		t.Skip("LocalStack not available at http://localhost:4566 — run automacao/subir-ambiente.sh first")
	}

	client := internal.NewAWSClient()

	tags := "~@load"
	if os.Getenv("E2E_LOAD_TESTS") == "true" {
		tags = ""
	}

	suite := godog.TestSuite{
		Name: "f2e-e2e",
		ScenarioInitializer: internal.NewScenarioInitializer(client),
		Options: &godog.Options{
			Format:      "pretty",
			Paths:       []string{"features"},
			TestingT:    t,
			Concurrency: 1, // sequential: all scenarios share the same output queue
			Tags:        tags,
		},
	}

	if suite.Run() != 0 {
		t.Fatal("E2E feature suite failed")
	}
}
