@failure @regression
Feature: Resilience and failure recovery

  These scenarios verify that the F2E pipeline handles failure cases correctly:
  deduplication, rejection, and edge conditions that must not produce unexpected
  output or ledger corruption. Each scenario uses a unique S3 key so it is safe
  to run in parallel with other features.

  # ---------------------------------------------------------------------------
  # Deduplication
  # ---------------------------------------------------------------------------

  @smoke
  Scenario: Re-uploading the same key does not duplicate output events
    Given I have a text file with 5 records
    When I upload and process the file
    Then I receive exactly 5 events within 60 seconds
    And all events are valid F2E envelopes
    And all event record numbers from 1 to 5 are present

  # ---------------------------------------------------------------------------
  # Rejection: empty files
  # ---------------------------------------------------------------------------

  @smoke
  Scenario: Empty text file is rejected and produces no output events
    Given I have an empty "text" file
    When I upload and process the file
    Then no events are produced within 15 seconds

  @smoke
  Scenario: Empty multi-line file is rejected and produces no output events
    Given I have an empty "multi-line" file
    When I upload and process the file
    Then no events are produced within 15 seconds

  # ---------------------------------------------------------------------------
  # Edge cases: small valid files
  # ---------------------------------------------------------------------------

  @smoke
  Scenario: Single-record text file produces exactly one event
    Given I have a text file with 1 records
    When I upload and process the file
    Then I receive exactly 1 events within 60 seconds
    And all events are valid F2E envelopes
    And all event record numbers from 1 to 1 are present

  @smoke
  Scenario: Single-element JSON array file produces exactly one event
    Given I have a JSON array file with 1 elements
    When I upload and process the file
    Then I receive exactly 1 events within 60 seconds
    And all events are valid F2E envelopes
    And all event record numbers from 1 to 1 are present
